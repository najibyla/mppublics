"""
02b_scrape_pv.py - Scraping des PVs d'ouverture des plis
Codes procédure validés depuis v5 :
  40 → Concours Architectural
   4 → Concours Phase 1
  47 → Concours Phase 2
URL : AvisExtraitPV + annonceType=5
"""

import asyncio
import hashlib
import logging
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from db import get_conn, init_db, extract_url_id
from utils import async_retry, human_delay, random_page_delay, human_move_and_click
from notifier import notify_error, notify_critical, notify_success, notify_warning, pipeline_guard

load_dotenv()

BASE_URL       = os.getenv("BASE_URL", "https://www.marchespublics.gov.ma/index.php")
URL_RECHERCHE_PV = os.getenv(
    "URL_RECHERCHE_PV",
    "https://www.marchespublics.gov.ma/index.php"
    "?page=entreprise.EntrepriseAdvancedSearch&AvisExtraitPV"
)

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCRAPEPV] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/02b_scrape_pv.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Codes de procédure validés depuis v5
PROCEDURES = {
    "40": "Concours Architectural",
    "4":  "Concours Phase 1",
    "47": "Concours Phase 2",
}


async def get_random_ua():
    try:
        from fake_useragent import UserAgent
        ua = UserAgent(use_cache_server=False, verify_ssl=False)
        return ua.random
    except Exception:
        return ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


@async_retry(max_attempts=3, delay=5, exceptions=(Exception,))
async def search_procedure(page, code, name):
    """Lance la recherche pour un type de procédure donné."""
    log.info(f"Recherche PV : {name} (code={code})")
    await page.goto(URL_RECHERCHE_PV, wait_until="networkidle", timeout=25000)
    await random_page_delay(page, min_ms=2000, max_ms=4000)

    # annonceType = 5 (PV) — ID validé v5
    await page.select_option(
        "#ctl0_CONTENU_PAGE_AdvancedSearch_annonceType", value="5"
    )
    await human_delay(500, 1000)

    # procedureType — ID validé v5
    await page.select_option(
        "#ctl0_CONTENU_PAGE_AdvancedSearch_procedureType", value=code
    )
    await human_delay(500, 1000)

    # Bouton lancer recherche — ID validé v5
    btn = page.locator("#ctl0_CONTENU_PAGE_AdvancedSearch_lancerRecherche")
    await human_move_and_click(page, btn)
    await page.wait_for_load_state("networkidle", timeout=20000)
    await human_delay(3000, 6000)

    # Passer à 500 par page
    try:
        sel = page.locator("#ctl0_CONTENU_PAGE_resultSearch_listePageSizeBottom")
        if await sel.count():
            await sel.select_option(value="500")
            await page.wait_for_load_state("networkidle", timeout=20000)
            await human_delay(2000, 4000)
    except Exception:
        pass


async def parse_pv_rows(page, procedure_name):
    """Extrait les PVs de la page courante.
    
    Utilise text_content() au lieu de inner_text() pour lire les panneaux collapsés.
    Fix context destroyed : même protection que parse_rows() du crawler.
    """
    results = []
    try:
        # Attendre que la page soit stable avant locator.all()
        await page.wait_for_load_state("networkidle", timeout=20000)

        table_sel = "table.table-results tbody tr"
        try:
            await page.wait_for_selector(table_sel, timeout=15000)
        except Exception:
            table_sel = "table tr:has(td)"
            try:
                await page.wait_for_selector(table_sel, timeout=10000)
            except Exception:
                log.warning(f"Tableau PV non trouvé — {procedure_name}")
                return results

        await asyncio.sleep(1.5)

        rows = await page.locator(table_sel).all()
        if not rows:
            log.warning(f"0 PVs pour {procedure_name}")
            return results

        log.info(f"  {len(rows)} PVs trouvés pour {procedure_name}")

        for row in rows:
            try:
                cells = await row.locator("td").all()
                if len(cells) < 6:
                    continue

                # Date publication — cells[1] comme dans le crawler principal
                date_pub = ""
                try:
                    cell1_text = await cells[1].inner_text()
                    m = re.search(r'\d{2}/\d{2}/\d{4}', cell1_text)
                    if m:
                        date_pub = m.group(0)
                except Exception:
                    pass

                # Référence
                ref_el = cells[2].locator("span.ref").first
                reference = ""
                if await ref_el.count():
                    reference = (await ref_el.inner_text()).strip()

                # Objet — utiliser text_content() pour lire les panneaux cachés
                objet = ""
                try:
                    obj_el = cells[2].locator("[id*='_panelBlocObjet']").first
                    if await obj_el.count():
                        txt = await obj_el.text_content() or ""
                        objet = txt.replace("Objet :", "").strip()
                except Exception:
                    pass

                # Lien détail
                lnk = cells[5].locator("a[href*='EntrepriseDetailConsultation']").first
                lien = ""
                if await lnk.count():
                    raw = await lnk.get_attribute("href") or ""
                    lien = (BASE_URL + raw) if raw.startswith("?") else raw

                if reference:
                    results.append({
                        "reference":        reference,
                        "objet":            objet,
                        "lien":             lien,
                        "procedure":        procedure_name,
                        "pv_pdf_url":       "",
                        "date_publication": date_pub,
                    })
            except Exception as e:
                log.debug(f"Ligne PV ignorée : {e}")

    except Exception as e:
        log.error(f"Erreur parse_pv_rows : {e}")
    return results


def pv_already_known(url_id: str, reference: str) -> bool:
    """Vérifie si un PV existe déjà en base pour ce tender (par url_id ou reference).
    
    Permet de skipper la visite de la page détail si le PV est déjà connu.
    Note : si un nouveau PV est publié pour un tender existant, il sera quand même
    détecté car save_pv() vérifie par hash_pdf (URL unique par PV).
    """
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        if url_id:
            row = c.execute(
                "SELECT p.id FROM pvs p JOIN tenders t ON p.tender_id=t.id WHERE t.url_id=? LIMIT 1",
                (url_id,)
            ).fetchone()
            if row:
                return True
        # Fallback sur reference
        row = c.execute(
            "SELECT id FROM pvs WHERE reference=? LIMIT 1", (reference,)
        ).fetchone()
        return row is not None
    except Exception:
        return False
    finally:
        if conn:
            conn.close()


async def enrich_pv_pdf(page, items):
    """
    Pour chaque PV, va sur la page détail récupérer le lien PDF.
    Sélecteur validé v5 : a[href*='EntrepriseDownloadAvis']
    """
    enriched = []
    skipped = 0
    for i, item in enumerate(items):
        uid = extract_url_id(item.get("lien", ""))
        ref = item["reference"]

        # Skip si PV déjà connu en base — évite de revisiter la page détail inutilement
        if pv_already_known(uid, ref):
            log.debug(f"  PDF [{i+1}/{len(items)}] {ref} — déjà en base, skip")
            skipped += 1
            continue

        log.info(f"  PDF [{i+1}/{len(items)}] {ref}")
        try:
            await page.goto(item["lien"], wait_until="networkidle", timeout=20000)
            await random_page_delay(page, min_ms=1500, max_ms=3500)

            # Toggle principal
            try:
                toggle = page.locator("a[onclick*='infosPrincipales']").first
                if await toggle.count():
                    await page.evaluate("el => el.click()", await toggle.element_handle())
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            # Lien PDF PV
            pdf_url = ""
            try:
                lnk = page.locator("a[href*='EntrepriseDownloadAvis']").first
                if await lnk.count():
                    raw = await lnk.get_attribute("href") or ""
                    if "?" in raw:
                        pdf_url = BASE_URL + "?" + raw.split("?")[1]
                    else:
                        pdf_url = raw
            except Exception:
                pass

            item["pv_pdf_url"] = pdf_url
            enriched.append(item)
            await human_delay(1500, 3000)

        except Exception as e:
            log.warning(f"  PDF erreur {item['reference']} : {e}")
            enriched.append(item)

    if skipped:
        log.info(f"  {skipped} PVs déjà en base skippés (pas de visite page détail)")
    return enriched


def save_pv(tender_id, reference, pdf_url, procedure_type, date_pub=""):
    """Insère un PV unique dans la table pvs avec déduplication par hash."""
    if not pdf_url:
        return False
    
    conn = get_conn()
    c = conn.cursor()
    
    try:
        # Compute hash du PDF
        pdf_hash = hashlib.md5(pdf_url.encode()).hexdigest()
        
        # Vérifier si ce PDF est déjà en base
        existing = c.execute(
            "SELECT id FROM pvs WHERE hash_pdf=?", (pdf_hash,)
        ).fetchone()
        
        if existing:
            # Mettre à jour la date si elle était vide
            if date_pub:
                c.execute(
                    """UPDATE pvs SET date_publication=?
                       WHERE id=? AND (date_publication IS NULL OR date_publication='')""",
                    (date_pub, existing["id"])
                )
                conn.commit()
            log.debug(f"PV déjà en base (hash match) : {reference}")
            conn.close()
            return False
        
        # Insérer le PV
        c.execute(
            """INSERT INTO pvs
               (tender_id, reference, pdf_url, procedure_type, date_publication, hash_pdf, synced_to_sheets)
               VALUES (?, ?, ?, ?, ?, ?, 0)""",
            (tender_id, reference, pdf_url, procedure_type, date_pub, pdf_hash)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log.error(f"save_pv {reference} : {e}")
        conn.close()
        return False


def save_pvs(items):
    """
    Insère les PVs dans la table dédiée `pvs` (1 tender → N PVs).

    Stratégie de matching pour retrouver le tender :
    1. url_id extrait du lien (le plus fiable)
    2. reference (fallback — peut matcher plusieurs lignes)

    Déduplication par hash_pdf : évite les doublons si le même PDF est publié
    plusieurs fois pour la même consultation.

    Si le tender n'existe pas, on l'insère en TO_ENRICH (orphan PV).
    """
    conn = get_conn()
    c = conn.cursor()
    inserted_pvs = created_tenders = 0

    for item in items:
        ref            = item["reference"]
        pdf_url        = item.get("pv_pdf_url", "")
        lien           = item.get("lien", "")
        procedure_type = item.get("procedure_type", "")
        objet          = item.get("objet", "")

        if not pdf_url:
            continue

        # Tenter de retrouver le tender par url_id d'abord
        uid = extract_url_id(lien)
        tender_id = None

        if uid:
            tender = c.execute(
                "SELECT id FROM tenders WHERE url_id=?", (uid,)
            ).fetchone()
            if tender:
                tender_id = tender["id"]

        if not tender_id:
            # Fallback sur référence
            tender = c.execute(
                "SELECT id FROM tenders WHERE reference=? ORDER BY created_at DESC LIMIT 1",
                (ref,)
            ).fetchone()
            if tender:
                tender_id = tender["id"]

        if not tender_id:
            # PV orphelin : créer le tender d'abord
            try:
                c.execute(
                    """INSERT INTO tenders
                       (url_id, reference, lien_detail, objet, status)
                       VALUES (?, ?, ?, ?, 'TO_ENRICH')""",
                    (uid or None, ref, lien, objet)
                )
                tender_id = c.lastrowid
                conn.commit()  # Commit immédiat pour éviter le lock dans save_pv()
                created_tenders += 1
                log.info(f"[{ref}] Tender orphelin créé (id={tender_id})")
            except Exception as e:
                log.warning(f"[{ref}] Impossible de créer le tender : {e}")
                continue

        # Insérer le PV dans la table pvs
        conn.commit()  # Libérer tout verrou avant d'appeler save_pv()
        if tender_id:
            if save_pv(tender_id, ref, pdf_url, procedure_type, ""):
                inserted_pvs += 1

    conn.commit()
    conn.close()
    log.info(f"PVs : {inserted_pvs} insérés | {created_tenders} tenders orphelins créés")


@pipeline_guard("02b_scrape_pv")
async def run():
    try:
        init_db()
    except Exception as e:
        notify_critical("02b_scrape_pv", e, "Impossible d'initialiser la base de données")
        raise

    user_agent = await get_random_ua()

    try:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"]
                )
            except Exception as e:
                notify_critical("02b_scrape_pv", e, "Impossible de lancer Chromium")
                raise

            try:
                context = await browser.new_context(
                    user_agent=user_agent, locale="fr-FR"
                )
                page = await context.new_page()
            except Exception as e:
                notify_critical("02b_scrape_pv", e, "Impossible de créer le contexte navigateur")
                await browser.close()
                raise

            all_pvs = []

            try:
                for code, name in PROCEDURES.items():
                    try:
                        await search_procedure(page, code, name)
                        rows = await parse_pv_rows(page, name)
                        if rows:
                            enriched = await enrich_pv_pdf(page, rows)
                            # Ajouter le procedure_type à chaque PV
                            for pv in enriched:
                                pv["procedure_type"] = name
                            all_pvs.extend(enriched)
                    except Exception as e:
                        log.error(f"Erreur procédure {name} (code={code}) : {e}")
                        notify_error("02b_scrape_pv", e, f"Procédure {name} (code={code})")
                    await human_delay(3000, 6000)

                if all_pvs:
                    try:
                        save_pvs(all_pvs)
                    except Exception as e:
                        notify_error("02b_scrape_pv", e, "Erreur sauvegarde PVs en base")
                        raise
                    log.info(f"=== Total PVs traités : {len(all_pvs)} ===")
                else:
                    log.info("Aucun PV trouvé")

            except Exception as e:
                log.critical(f"Erreur fatale dans la boucle PVs : {e}", exc_info=True)
                notify_critical("02b_scrape_pv", e, "Erreur fatale dans la boucle PVs")
            finally:
                await browser.close()

    except Exception as e:
        notify_critical("02b_scrape_pv", e, "Erreur fatale pipeline scrape PV")
        raise

    notify_success("02b_scrape_pv", {
        "Procédures": len(PROCEDURES),
        "PVs traités": len(all_pvs),
    })


if __name__ == "__main__":
    asyncio.run(run())
