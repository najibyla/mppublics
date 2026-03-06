"""
04_analyzer.py - Extraction ZIP récursive + parsing PDF à 4 niveaux
Chaîne :
  1. PyMuPDF    → texte natif (rapide)
  2. pdfplumber → tableaux structurés
  3. Tesseract  → OCR scans
  4. Ollama/Mistral → extraction intelligente (local, gratuit)
Statut : TO_ANALYZE -> TO_SYNC
"""

import json
import logging
import os
import re
import shutil
import zipfile
from pathlib import Path

import fitz  # PyMuPDF
import pdfplumber
from PIL import Image
import pytesseract

from dotenv import load_dotenv
from db import get_conn, init_db
from utils import sync_retry, parse_montant
from notifier import notify_error, notify_critical, notify_success, pipeline_guard

load_dotenv()

os.makedirs("logs", exist_ok=True)
os.makedirs("summaries", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ANALYZER] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/04_analyzer.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

SUMMARIES_DIR  = Path("summaries").resolve()
TEMP_DIR       = Path("temp_extract").resolve()
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "20"))
MAX_RETRY_DB     = int(os.getenv("MAX_RETRY", "3"))
ANALYZER_WORKERS = int(os.getenv("ANALYZER_WORKERS", "2"))  # Parallélisme PDF

# Modèle Ollama — mixtral:8x7b recommandé sur VM 24Go
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL", "mistral")
OLLAMA_ENABLED = os.getenv("OLLAMA_ENABLED", "true").lower() == "true"

MONTANT_PATTERNS = [
    re.compile(
        r'(?:Estimation|Montant\s+du\s+programme|Budget\s+pr[ée]visionnel|'
        r'Valeur\s+estim[ée]e|Prix\s+de\s+r[ée]f[ée]rence|Enveloppe\s+budg[ée]taire)'
        r'.{0,120}?(\d[\d\s,.]+)\s*(?:DH|MAD|Dhs)?',
        re.IGNORECASE | re.DOTALL
    ),
    re.compile(
        r'(\d{1,3}(?:[\s.]\d{3})+(?:[,.]\d{1,2})?)\s*(?:DH|MAD|Dhs)',
        re.IGNORECASE
    ),
]


# ─── Extraction ZIP ───────────────────────────────────────────────────────────

def extract_zip_recursive(zip_path, dest_dir, depth=0):
    if depth > 5:
        return
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(dest_dir)
        for nested in Path(dest_dir).rglob("*.zip"):
            nested_dest = nested.parent / nested.stem
            nested_dest.mkdir(exist_ok=True)
            extract_zip_recursive(str(nested), str(nested_dest), depth + 1)
            nested.unlink()
    except zipfile.BadZipFile:
        log.warning(f"ZIP corrompu : {zip_path}")
    except Exception as e:
        log.error(f"Extraction {zip_path} : {e}")


# ─── Niveau 1 : PyMuPDF (texte natif) ────────────────────────────────────────

def extract_text_pymupdf(pdf_path):
    """Extraction texte natif — le plus rapide."""
    text = ""
    try:
        doc = fitz.open(str(pdf_path))
        for page in doc:
            text += page.get_text()
        doc.close()
    except Exception as e:
        log.debug(f"PyMuPDF {pdf_path.name} : {e}")
    return text.strip()


# ─── Niveau 2 : pdfplumber (tableaux structurés) ─────────────────────────────

def extract_tables_pdfplumber(pdf_path):
    """
    Extrait les tableaux structurés d'un PDF.
    Particulièrement efficace pour les bordereaux de prix et tableaux financiers.
    Retourne le texte brut des tableaux + le texte de la page.
    """
    result = ""
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                # Texte normal
                page_text = page.extract_text() or ""
                result += page_text + "\n"

                # Tableaux → convertis en texte tabulaire
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if row:
                            cleaned = [str(cell).strip() if cell else "" for cell in row]
                            result += " | ".join(cleaned) + "\n"
    except Exception as e:
        log.debug(f"pdfplumber {pdf_path.name} : {e}")
    return result.strip()


# ─── Niveau 3 : Tesseract OCR (scans) ────────────────────────────────────────

def extract_text_ocr(pdf_path):
    """OCR page par page — utilisé si les niveaux 1 et 2 échouent."""
    text = ""
    try:
        doc = fitz.open(str(pdf_path))
        for i, page in enumerate(doc):
            mat = fitz.Matrix(2.0, 2.0)   # zoom x2 pour meilleure qualité
            pix = page.get_pixmap(matrix=mat)
            img_path = TEMP_DIR / f"_ocr_{pdf_path.stem}_{i}.png"
            pix.save(str(img_path))
            page_text = pytesseract.image_to_string(
                Image.open(str(img_path)),
                lang="fra+ara",
                config="--psm 6"   # Assume un bloc de texte uniforme
            )
            text += page_text + "\n"
            img_path.unlink(missing_ok=True)
        doc.close()
    except Exception as e:
        log.debug(f"OCR {pdf_path.name} : {e}")
    return text.strip()


# ─── Niveau 4 : Ollama/Mistral (extraction intelligente locale) ───────────────

def extract_with_ollama(text, reference):
    """
    Envoie le texte à Mistral (local via Ollama) pour extraction structurée.
    Retourne un dict avec les champs extraits ou None si Ollama indisponible.
    """
    if not OLLAMA_ENABLED:
        return None
    try:
        import ollama   # pip install ollama

        # Limiter le texte pour ne pas dépasser le contexte
        text_excerpt = text[:4000] if len(text) > 4000 else text

        prompt = f"""Tu es un expert en marchés publics marocains.
Analyse ce document et extrais les informations suivantes en JSON strict.
Si une information est absente, mets null.

Document :
\"\"\"
{text_excerpt}
\"\"\"

Réponds UNIQUEMENT avec ce JSON (sans markdown, sans explication) :
{{
  "montant_estime": <nombre en DH ou null>,
  "objet": "<description du marché ou null>",
  "acheteur": "<nom de l'acheteur public ou null>",
  "date_limite": "<JJ/MM/AAAA ou null>",
  "lieu_execution": "<ville/région ou null>",
  "domaines": "<domaines d'activité ou null>",
  "contact_email": "<email ou null>",
  "contact_tel": "<téléphone ou null>",
  "resume": "<résumé en 2-3 phrases>"
}}"""

        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.1}   # Faible température = réponses stables
        )

        raw = response["message"]["content"].strip()
        # Nettoyer si markdown présent
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'\s*```$', '', raw, flags=re.MULTILINE)

        result = json.loads(raw)
        log.info(f"[{reference}] Ollama OK — montant={result.get('montant_estime')}")
        return result

    except ImportError:
        log.debug("ollama non installé — niveau 4 ignoré")
        return None
    except json.JSONDecodeError as e:
        log.debug(f"[{reference}] Ollama JSON invalide : {e}")
        return None
    except Exception as e:
        log.debug(f"[{reference}] Ollama erreur : {e}")
        return None


# ─── Moteur de recherche de montant ──────────────────────────────────────────

def find_montant(text):
    for pattern in MONTANT_PATTERNS:
        match = pattern.search(text)
        if match:
            val = parse_montant(match.group(1))
            if val and val > 1000:
                return val
    return None


# ─── Pipeline PDF à 4 niveaux ─────────────────────────────────────────────────

@sync_retry(max_attempts=2, delay=2, exceptions=(Exception,))
def parse_pdf(pdf_path, reference):
    """
    Chaîne d'extraction à 4 niveaux.
    Retourne (texte_complet, montant, ollama_data)
    """
    ollama_data = None

    # Niveau 1 : texte natif PyMuPDF
    text = extract_text_pymupdf(pdf_path)
    montant = find_montant(text) if text else None
    log.debug(f"[{reference}] L1 PyMuPDF : {len(text)} chars | montant={montant}")

    # Niveau 2 : pdfplumber pour les tableaux
    # Toujours exécuté car complémentaire (capture des tableaux que PyMuPDF rate)
    plumber_text = extract_tables_pdfplumber(pdf_path)
    if plumber_text and plumber_text not in text:
        text = (text + "\n" + plumber_text).strip()
        if montant is None:
            montant = find_montant(plumber_text)
        log.debug(f"[{reference}] L2 pdfplumber : +{len(plumber_text)} chars | montant={montant}")

    # Niveau 3 : OCR si texte insuffisant (< 100 chars = probablement un scan)
    if len(text) < 100:
        log.debug(f"[{reference}] L3 OCR (texte insuffisant : {len(text)} chars)")
        ocr_text = extract_text_ocr(pdf_path)
        if ocr_text:
            text = (text + "\n" + ocr_text).strip()
            if montant is None:
                montant = find_montant(ocr_text)
            log.debug(f"[{reference}] L3 OCR : +{len(ocr_text)} chars | montant={montant}")

    # Niveau 4 : Ollama si montant encore non trouvé ou texte complexe
    if montant is None and text and len(text) > 100:
        log.debug(f"[{reference}] L4 Ollama")
        ollama_data = extract_with_ollama(text, reference)
        if ollama_data and ollama_data.get("montant_estime"):
            montant = ollama_data["montant_estime"]

    return text, montant, ollama_data


# ─── Résumé ───────────────────────────────────────────────────────────────────

def generate_summary(reference, texts, montant_pdf, ollama_data=None):
    """
    Génère un résumé .txt structuré.
    Privilégie les données Ollama si disponibles.
    """
    full_text = "\n\n".join(texts)
    lines = [f"=== RÉSUMÉ — {reference} ===\n"]

    # Si Ollama a fourni un résumé structuré → l'utiliser en priorité
    if ollama_data:
        lines.append("── Extraction IA (Mistral) ──")
        if ollama_data.get("resume"):
            lines.append(ollama_data["resume"])
            lines.append("")
        lines.append("── Données extraites ──")
        fields = [
            ("Objet",          "objet"),
            ("Acheteur",       "acheteur"),
            ("Montant estimé", "montant_estime"),
            ("Date limite",    "date_limite"),
            ("Lieu exécution", "lieu_execution"),
            ("Domaines",       "domaines"),
            ("Email",          "contact_email"),
            ("Téléphone",      "contact_tel"),
        ]
        for label, key in fields:
            val = ollama_data.get(key)
            if val:
                lines.append(f"{label:<20}: {val}")
    else:
        # Fallback : extraction regex
        lines.append("── Points clés (extraction regex) ──")
        keywords = [
            ("Objet",          r'Objet.{0,5}:?\s*([^\n]{5,200})'),
            ("Acheteur",       r'Acheteur.{0,5}:?\s*([^\n]{3,100})'),
            ("Estimation",     r'(?:Estimation|Montant).{0,30}?(\d[\d\s,.]+\s*(?:DH|MAD)?)'),
            ("Date limite",    r'Date.limite.{0,10}:?\s*(\d{2}[/\-]\d{2}[/\-]\d{4}.{0,6})'),
            ("Lieu",           r"Lieu.d.ex[ée]cution.{0,10}:?\s*([^\n]{3,80})"),
            ("Email",          r'([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})'),
            ("Téléphone",      r'(?:Tél|Tel)[^\d]*(\+?212[\s.\-]?[5-7]\d{8}|0[5-7]\d{8})'),
        ]
        for label, pattern in keywords:
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                lines.append(f"{label:<20}: {m.group(1).strip()[:150]}")

    if montant_pdf:
        lines.append(f"\n{'Montant PDF':<20}: {montant_pdf:,.0f} DH")

    lines.append(f"\n── Niveau d'extraction utilisé ──")
    level = "Ollama/Mistral" if ollama_data else ("OCR" if full_text else "PyMuPDF")
    lines.append(f"{'Source':<20}: {level}")

    summary_path = SUMMARIES_DIR / f"{reference}.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return str(summary_path)


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_pending(limit=BATCH_SIZE):
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            """SELECT id, reference, zip_path, estimation FROM tenders
               WHERE status='TO_ANALYZE'
               AND (retry_count IS NULL OR retry_count < ?)
               ORDER BY created_at DESC
               LIMIT ?""",
            (MAX_RETRY_DB, limit)
        )
        return c.fetchall()
    except Exception as e:
        log.error(f"get_pending erreur : {e}")
        return []
    finally:
        if conn:
            conn.close()


def update_tender(tender_pk, estimation, resume_path, status):
    """Met à jour par PK — évite les ambiguïtés de référence."""
    conn = None
    try:
        conn = get_conn()
        conn.execute(
            "UPDATE tenders SET estimation=?, resume_txt=?, status=? WHERE id=?",
            (estimation, resume_path, status, tender_pk)
        )
        conn.commit()
    except Exception as e:
        log.error(f"update_tender id={tender_pk} : {e}")
    finally:
        if conn:
            conn.close()


def increment_retry(tender_pk, error_msg=None):
    """Incrémente le retry par PK."""
    conn = None
    try:
        conn = get_conn()
        conn.execute(
            """UPDATE tenders SET
               retry_count = COALESCE(retry_count, 0) + 1,
               last_error  = COALESCE(?, last_error)
               WHERE id=?""",
            (error_msg, tender_pk)
        )
        conn.commit()
    except Exception as e:
        log.error(f"increment_retry id={tender_pk} : {e}")
    finally:
        if conn:
            conn.close()


# ─── Main ─────────────────────────────────────────────────────────────────────

def process_one(row):
    """Traite une seule offre — appelé en parallèle via ThreadPoolExecutor."""
    tender_pk   = row["id"]
    reference   = row["reference"]
    zip_path    = row["zip_path"]
    est_web     = row["estimation"]
    # Nom du dossier temporaire basé sur PK pour éviter collisions de noms
    extract_dir = TEMP_DIR / f"{tender_pk}_{reference.replace('/', '_')}"

    if not zip_path or not Path(zip_path).exists():
        log.warning(f"[{reference}] ZIP introuvable : {zip_path}")
        increment_retry(tender_pk, "ZIP introuvable")
        return "error"

    extract_dir.mkdir(parents=True, exist_ok=True)
    try:
        extract_zip_recursive(zip_path, str(extract_dir))
        pdfs = list(extract_dir.rglob("*.pdf")) + list(extract_dir.rglob("*.PDF"))
        log.info(f"[{reference}] {len(pdfs)} PDF(s) trouvés")

        if not pdfs:
            log.warning(f"[{reference}] Aucun PDF dans le ZIP")
            increment_retry(tender_pk, "Aucun PDF dans le ZIP")
            return "error"

        all_texts   = []
        montant_pdf = None
        ollama_data = None

        for pdf in pdfs:
            try:
                text, montant, od = parse_pdf(pdf, reference)
                if text:
                    all_texts.append(text)
                if montant and montant_pdf is None:
                    montant_pdf = montant
                if od and ollama_data is None:
                    ollama_data = od
            except Exception as e:
                log.warning(f"[{reference}] PDF {pdf.name} ignoré : {e}")

        estimation_final = est_web or montant_pdf
        resume_path = generate_summary(reference, all_texts, montant_pdf, ollama_data)
        update_tender(tender_pk, estimation_final, resume_path, "TO_SYNC")

        log.info(
            f"[{reference}] ✓ estimation={estimation_final} | "
            f"PDFs={len(pdfs)} | ollama={'oui' if ollama_data else 'non'}"
        )
        return "ok"

    except Exception as e:
        log.error(f"[{reference}] Erreur : {e}", exc_info=True)
        notify_error("04_analyzer", e, f"Référence {reference} (id={tender_pk})")
        increment_retry(tender_pk, str(e))
        update_tender(tender_pk, est_web, None, "ERROR_ANALYZE")
        return "error"
    finally:
        shutil.rmtree(str(extract_dir), ignore_errors=True)


@pipeline_guard("04_analyzer")
def run():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        init_db()
    except Exception as e:
        notify_critical("04_analyzer", e, "Impossible d'initialiser la base de données")
        raise

    TEMP_DIR.mkdir(exist_ok=True)

    try:
        pending = get_pending()
    except Exception as e:
        notify_critical("04_analyzer", e, "Impossible de récupérer les offres à analyser")
        raise

    log.info(f"{len(pending)} offres à analyser | workers={ANALYZER_WORKERS}")

    if not pending:
        log.info("Rien à analyser.")
        return

    ok = errors = 0

    try:
        with ThreadPoolExecutor(max_workers=ANALYZER_WORKERS) as executor:
            futures = {executor.submit(process_one, row): row["reference"] for row in pending}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result == "ok":
                        ok += 1
                    else:
                        errors += 1
                except Exception as e:
                    log.error(f"Future erreur : {e}", exc_info=True)
                    notify_error("04_analyzer", e, "Erreur dans un worker ThreadPoolExecutor")
                    errors += 1
    except Exception as e:
        log.critical(f"Erreur fatale ThreadPoolExecutor : {e}", exc_info=True)
        notify_critical("04_analyzer", e, "Erreur fatale dans le pool de workers")
        raise

    notify_success("04_analyzer", {
        "Analysés": ok + errors,
        "OK":       ok,
        "Erreurs":  errors,
    })
    log.info(f"=== Fin : {ok} OK | {errors} erreurs ===")


if __name__ == "__main__":
    run()
