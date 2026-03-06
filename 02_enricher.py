"""
02_enricher.py - Enrichissement des offres (TO_ENRICH -> TO_DOWNLOAD)
Sélecteurs validés depuis v5. Champs supplémentaires : Caution, Qualifications,
Agréments, Lieu Ouverture, Adresse Retrait/Dépôt, Réunion, Visites, Variante.

Mise à jour schéma :
  - get_pending() retourne id (PK), url_id, reference, lien_detail, date_publication
  - update_tender() utilise id comme clé de mise à jour (fiable, unique)
  - tender_hash calculé après enrichissement (reference + date_pub + acheteur)
"""

import asyncio
import logging
import os
import re
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from db import get_conn, init_db, compute_tender_hash
from utils import (async_retry, compute_hash, parse_montant,
                   human_delay, human_scroll, random_page_delay, human_move_and_click)
from notifier import notify_error, notify_critical, notify_success, notify_warning, pipeline_guard

load_dotenv()

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENRICHER] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/02_enricher.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "50"))
MAX_RETRY_DB = int(os.getenv("MAX_RETRY", "3"))
BASE_URL     = os.getenv("BASE_URL", "https://www.marchespublics.gov.ma/index.php")


async def get_random_ua():
    try:
        from fake_useragent import UserAgent
        ua = UserAgent(use_cache_server=False, verify_ssl=False)
        return ua.random
    except Exception:
        return ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


async def get_best_text(page, partial_id):
    """
    Cherche tous les éléments contenant partial_id.
    Retourne le premier texte non vide — stratégie bulldozer v5.
    """
    try:
        elements = await page.locator(f"[id*='{partial_id}']").all()
        for el in elements:
            txt = (await el.inner_text()).strip()
            if txt and txt not in ("N/A", "-", ""):
                # Nettoyer les labels redondants
                txt = re.sub(r'^[^:]+:\s*', '', txt, count=1).strip()
                return txt or "N/A"
        return "N/A"
    except Exception:
        return "N/A"


async def expand_toggles(page):
    """
    Clique sur tous les toggles title-toggle — ID validé depuis v5.
    """
    try:
        toggles = await page.locator("a.title-toggle").all()
        for t in toggles:
            try:
                await page.evaluate("arguments => arguments[0].click()", [await t.element_handle()])
                await page.wait_for_timeout(200)
            except Exception:
                pass
        log.debug(f"{len(toggles)} toggles cliqués")
        await page.wait_for_timeout(1500)
    except Exception as e:
        log.debug(f"expand_toggles : {e}")


@async_retry(max_attempts=3, delay=4, exceptions=(Exception,))
async def scrape_detail(page, url, reference):
    """
    Scrape la page détail d'une offre avec les vrais sélecteurs v5.
    """
    data = {
        "objet": "", "acheteur": "", "estimation": None,
        "domaines": "", "lieu": "",
        "contact_nom": "", "contact_email": "",
        "contact_tel": "", "contact_fax": "",
        "caution": "", "qualifications": "", "agrements": "",
        "lieu_ouverture": "", "adresse_retrait": "", "adresse_depot": "",
        "reunion": "", "visites": "", "variante": "",
        "lien_dce": "", "attributaire": "",
        "montant_reel": None, "date_attribution": "",
        "nb_soumissionnaires": None, "hash_content": "",
    }

    await page.goto(url, wait_until="networkidle", timeout=25000)
    await random_page_delay(page, min_ms=2000, max_ms=5000)
    await expand_toggles(page)
    await human_delay(800, 1500)

    data["hash_content"] = compute_hash(await page.content())

    # ── Estimation — sélecteur exact v5 ───────────────────────────────────────
    est_raw = await get_best_text(page, "labelReferentielZoneText")
    data["estimation"] = parse_montant(est_raw) if est_raw != "N/A" else None

    # ── Contact — bloc panelContcatAdministratif (v5) ─────────────────────────
    try:
        bloc = page.locator("[id*='panelContcatAdministratif']").first
        if await bloc.count():
            data["contact_nom"]   = await get_best_text(page, "contactAdministratif")

            # Email, Tel, Fax dans le bloc contact
            for field, partial in [
                ("contact_email", "_email"),
                ("contact_tel",   "_telephone"),
                ("contact_fax",   "_telecopieur"),
            ]:
                try:
                    els = await bloc.locator(f"[id*='{partial}']").all()
                    for el in els:
                        txt = (await el.inner_text()).strip()
                        if txt and txt not in ("N/A", "-", ""):
                            data[field] = txt
                            break
                except Exception:
                    pass
    except Exception as e:
        log.debug(f"[{reference}] Contact : {e}")

    # ── Champs complémentaires — sélecteurs v5 ────────────────────────────────
    fields_map = {
        "caution":         "cautionProvisoire",
        "qualifications":  "_qualification",
        "agrements":       "_agrements",
        "lieu_ouverture":  "lieuOuverturePlis",
        "adresse_retrait": "adresseRetraitDossiers",
        "adresse_depot":   "adresseDepotOffres",
        "reunion":         "dateReunion",
        "visites":         "panelRepeaterVisitesLieux",
        "variante":        "varianteValeur",
    }
    for field, partial_id in fields_map.items():
        val = await get_best_text(page, partial_id)
        data[field] = val if val != "N/A" else ""

    # ── Objet ─────────────────────────────────────────────────────────────────
    try:
        obj_el = page.locator("[id*='_panelBlocObjet'] .info-bulle").first
        if await obj_el.count():
            data["objet"] = (await obj_el.inner_text()).strip()
        else:
            obj_el2 = page.locator("[id*='_panelBlocObjet']").first
            if await obj_el2.count():
                txt = await obj_el2.inner_text()
                data["objet"] = txt.replace("Objet :", "").strip()
    except Exception:
        pass

    # ── Acheteur ──────────────────────────────────────────────────────────────
    try:
        ach_el = page.locator("[id*='_panelBlocDenomination']").first
        if await ach_el.count():
            txt = await ach_el.inner_text()
            data["acheteur"] = txt.replace("Acheteur public :", "").strip()
    except Exception:
        pass

    # ── Lieu ──────────────────────────────────────────────────────────────────
    try:
        lieu_el = page.locator("[id*='_panelBlocLieuxExec'] .info-bulle").first
        if await lieu_el.count():
            data["lieu"] = (await lieu_el.inner_text()).strip()
    except Exception:
        pass

    # ── Lien DCE ─────────────────────────────────────────────────────────────
    for sel in [
        "a[href*='EntrepriseDemandeTelechargement']",
        "a[href*='DCE']", "a[href*='telecharger']",
        "a:has-text('DCE')", "a:has-text('Télécharger le dossier')"
    ]:
        el = page.locator(sel).first
        if await el.count():
            raw = await el.get_attribute("href") or ""
            data["lien_dce"] = (BASE_URL + raw) if raw.startswith("?") else raw
            break

    # ── Note : PVs sont scrapés séparément par 02b_scrape_pv.py ───────────────

    # ── Résultats d'attribution ───────────────────────────────────────────────
    for sel in ["[id*='resultat']", "[id*='attribution']", "[id*='Attribution']"]:
        el = page.locator(sel).first
        if await el.count():
            result_text = (await el.inner_text()).strip()
            attr = re.search(
                r'(?:Attributaire|Titulaire|Adjudicataire)\s*:?\s*([^\n]{3,80})',
                result_text, re.IGNORECASE
            )
            if attr:
                data["attributaire"] = attr.group(1).strip()
            montant_m = re.search(
                r'(?:Montant\s+attribu[ée]|Prix\s+retenu).{0,40}?(\d[\d\s,.]+)',
                result_text, re.IGNORECASE
            )
            if montant_m:
                data["montant_reel"] = parse_montant(montant_m.group(1))
            nb = re.search(r'(\d+)\s+(?:soumissionnaire|candidat)', result_text, re.IGNORECASE)
            if nb:
                data["nb_soumissionnaires"] = int(nb.group(1))
            break

    log.info(
        f"[{reference}] OK | est={data['estimation']} | "
        f"DCE={'✓' if data['lien_dce'] else '✗'} | "
        f"acheteur={data['acheteur'][:20] if data['acheteur'] else '—'}"
    )
    return data


def get_pending(limit=BATCH_SIZE):
    """Retourne les offres à enrichir avec id (PK) + date_publication pour le tender_hash."""
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT id, url_id, reference, lien_detail, date_publication FROM tenders
           WHERE status='TO_ENRICH'
           AND (retry_count IS NULL OR retry_count < ?)
           ORDER BY created_at DESC
           LIMIT ?""",
        (MAX_RETRY_DB, limit)
    )
    rows = c.fetchall()
    conn.close()
    return rows


def update_tender(tender_pk: int, reference: str, date_publication: str, data: dict, status: str):
    """
    Met à jour une offre par sa PK (id).
    Calcule et stocke le tender_hash une fois l'acheteur connu.

    tender_pk        : tenders.id (clé primaire, toujours unique)
    reference        : pour le calcul du hash (copie du row crawler)
    date_publication : pour le calcul du hash
    data             : résultat de scrape_detail()
    status           : statut cible (TO_DOWNLOAD ou ERROR_ENRICH)
    """
    # Calculer le hash sémantique maintenant qu'on a l'acheteur
    t_hash = compute_tender_hash(reference, date_publication, data.get("acheteur", ""))

    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute(
            """UPDATE tenders SET
                objet=?, acheteur=?, estimation=?, domaines=?,
                contact_nom=?, contact_email=?, contact_tel=?, contact_fax=?,
                lien_dce=?, attributaire=?, montant_reel=?,
                date_attribution=?, nb_soumissionnaires=?,
                hash_content=?, tender_hash=?, status=?
               WHERE id=?""",
            (
                data["objet"], data["acheteur"], data["estimation"], data["domaines"],
                data["contact_nom"], data["contact_email"], data["contact_tel"],
                data["contact_fax"], data["lien_dce"],
                data["attributaire"], data["montant_reel"], data["date_attribution"],
                data["nb_soumissionnaires"], data["hash_content"],
                t_hash, status, tender_pk
            )
        )
        conn.commit()
        log.debug(f"[id={tender_pk}] tender_hash={t_hash[:12]}…")
    except Exception as e:
        log.error(f"update_tender id={tender_pk} : {e}")
        raise
    finally:
        conn.close()


def increment_retry(tender_pk: int, error_msg=None):
    """Incrémente le compteur d'erreurs — utilise la PK pour éviter les ambiguïtés."""
    conn = get_conn()
    conn.execute(
        """UPDATE tenders SET
           retry_count = COALESCE(retry_count, 0) + 1,
           last_error  = COALESCE(?, last_error)
           WHERE id=?""",
        (error_msg, tender_pk)
    )
    conn.commit()
    conn.close()


@pipeline_guard("02_enricher")
async def run():
    try:
        init_db()
    except Exception as e:
        notify_critical("02_enricher", e, "Impossible d'initialiser la base de données")
        raise

    pending = get_pending()
    log.info(f"{len(pending)} offres à enrichir")
    if not pending:
        log.info("Rien à enrichir.")
        return

    user_agent = await get_random_ua()

    try:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"]
                )
            except Exception as e:
                notify_critical("02_enricher", e, "Impossible de lancer Chromium")
                raise

            try:
                context = await browser.new_context(
                    user_agent=user_agent, locale="fr-FR"
                )
                page = await context.new_page()
            except Exception as e:
                notify_critical("02_enricher", e, "Impossible de créer le contexte navigateur")
                await browser.close()
                raise

            ok = errors = 0

            try:
                for row in pending:
                    tender_pk  = row["id"]
                    reference  = row["reference"]
                    date_pub   = row["date_publication"] or ""
                    url        = row["lien_detail"]

                    if not url:
                        log.warning(f"[{reference}] Pas de lien détail")
                        increment_retry(tender_pk, "Pas de lien détail")
                        errors += 1
                        continue

                    try:
                        data = await scrape_detail(page, url, reference)
                        update_tender(tender_pk, reference, date_pub, data, "TO_DOWNLOAD")
                        ok += 1
                    except Exception as e:
                        log.error(f"[{reference} id={tender_pk}] Échec définitif : {e}")
                        notify_error("02_enricher", e, f"Référence {reference} (id={tender_pk})")
                        increment_retry(tender_pk, str(e))
                        errors += 1

                    await human_delay(1500, 3500)

            except Exception as e:
                log.critical(f"Erreur fatale dans la boucle : {e}", exc_info=True)
                notify_critical("02_enricher", e, "Erreur fatale dans la boucle d'enrichissement")
            finally:
                await browser.close()

    except Exception as e:
        notify_critical("02_enricher", e, "Erreur fatale pipeline enrichisseur")
        raise

    notify_success("02_enricher", {
        "Traités":  ok + errors,
        "OK":       ok,
        "Erreurs":  errors,
    })
    log.info(f"=== Fin : {ok} OK | {errors} erreurs ===")


if __name__ == "__main__":
    asyncio.run(run())
