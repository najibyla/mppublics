"""
03b_deownload.py - Version Hybride (Playwright + Requests)
Inspiré par la stratégie de reconstruction d'URL et transfert de cookies.
"""

import asyncio
import logging
import os
import zipfile
import requests
import urllib.parse
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from dotenv import load_dotenv
from db import get_conn, init_db
from utils import async_retry, human_delay, random_page_delay
from notifier import notify_error, notify_critical, notify_success, pipeline_guard

load_dotenv()
os.makedirs("logs", exist_ok=True)
os.makedirs("downloads", exist_ok=True)

# Configuration
BASE_URL = os.getenv("BASE_URL", "https://www.marchespublics.gov.ma/")
DOWNLOAD_DIR = Path("downloads").resolve()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))
MAX_RETRY_DB = int(os.getenv("MAX_RETRY", "3"))

DCE_NOM = os.getenv("DCE_NOM", "VEILLE")
DCE_PRENOM = os.getenv("DCE_PRENOM", "AUTOMATISEE")
DCE_EMAIL = os.getenv("DCE_EMAIL", "portailmpma@gmail.com")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DOWNLOADER] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/03_downloader.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def is_valid_zip(path):
    try:
        if not os.path.exists(path): return False
        with zipfile.ZipFile(path, 'r') as z:
            return z.testzip() is None
    except Exception:
        return False

def increment_retry(tender_pk, error_msg=None):
    """Utilise la PK pour éviter les ambiguïtés avec les références dupliquées."""
    conn = None
    try:
        conn = get_conn()
        conn.execute(
            """UPDATE tenders SET
               retry_count = COALESCE(retry_count, 0) + 1,
               last_error  = ?
               WHERE id=?""",
            (str(error_msg), tender_pk)
        )
        conn.commit()
    except Exception as e:
        log.error(f"increment_retry id={tender_pk} : {e}")
    finally:
        if conn:
            conn.close()

def download_with_requests(url, cookies, filename):
    """Télécharge le fichier en utilisant Requests et les cookies du navigateur."""
    local_path = DOWNLOAD_DIR / filename
    session = requests.Session()
    
    # Transfert des cookies de Playwright vers Requests
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        log.info(f"      ⬇️ Téléchargement via Requests...")
        response = session.get(url, headers=headers, stream=True, timeout=300)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        if is_valid_zip(local_path):
            log.info(f"      ✅ Succès ! Taille: {round(os.path.getsize(local_path)/1024/1024, 2)} Mo")
            return str(local_path)
        else:
            if os.path.exists(local_path): os.remove(local_path)
            return None
    except Exception as e:
        log.error(f"      ❌ Erreur Requests : {e}")
        return None

@async_retry(max_attempts=3, delay=6, exceptions=(Exception,))
async def process_download(page, url, reference):
    # Correction URL relative
    if not url.startswith("http"):
        base = BASE_URL.split("index.php")[0].rstrip('/')
        url = f"{base}/{url.lstrip('/')}"

    log.info(f"[{reference}] Accès à : {url}")
    await page.goto(url, wait_until="networkidle", timeout=35000)
    
    # --- CAS 1 : FORMULAIRE DCE ---
    if await page.locator("#ctl0_CONTENU_PAGE_EntrepriseFormulaireDemande_nom").count() > 0:
        await page.locator("#ctl0_CONTENU_PAGE_EntrepriseFormulaireDemande_nom").fill(DCE_NOM)
        await page.locator("#ctl0_CONTENU_PAGE_EntrepriseFormulaireDemande_prenom").fill(DCE_PRENOM)
        await page.locator("#ctl0_CONTENU_PAGE_EntrepriseFormulaireDemande_email").fill(DCE_EMAIL)
        
        cgu = page.locator("#ctl0_CONTENU_PAGE_EntrepriseFormulaireDemande_accepterConditions")
        if await cgu.count() and not await cgu.is_checked():
            await cgu.click()
        
        await human_delay(1000, 2000)
        
        # On clique pour valider le formulaire et établir la session
        await page.locator("#ctl0_CONTENU_PAGE_validateButton").click()
        await page.wait_for_load_state("networkidle")
        
        # Reconstruction de l'URL de téléchargement direct (Stratégie inspirée de votre fichier)
        current_url = page.url
        parsed = urllib.parse.urlparse(current_url)
        params = urllib.parse.parse_qs(parsed.query)
        
        ref_id = params.get('refConsultation', [''])[0]
        org_id = params.get('orgAcronyme', [''])[0]
        
        if ref_id and org_id:
            zip_url = f"https://www.marchespublics.gov.ma/index.php?page=entreprise.EntrepriseDownloadCompleteDce&reference={ref_id}&orgAcronym={org_id}"
            cookies = await page.context.cookies()
            filename = f"DCE_{reference.replace('/', '_')}.zip"
            return download_with_requests(zip_url, cookies, filename)
        
    # --- CAS 2 : LIEN PV DIRECT ---
    pv_link = page.locator("li.picto-link a[href*='DownloadAvis']").first
    if await pv_link.count() > 0:
        async with page.expect_download(timeout=60000) as dl_info:
            await pv_link.click()
        download = await dl_info.value
        dest = DOWNLOAD_DIR / f"PV_{reference.replace('/', '_')}_{download.suggested_filename}"
        await download.save_as(str(dest))
        return str(dest)

    return None

def get_pending(limit=BATCH_SIZE):
    """Récupère les offres à télécharger — retourne id (PK), reference pour logs."""
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            """SELECT id, reference, COALESCE(lien_dce, '') as lien_dce FROM tenders
               WHERE status='TO_DOWNLOAD'
               AND (retry_count IS NULL OR retry_count < ?)
               AND lien_dce IS NOT NULL AND lien_dce != ''
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

def update_status(tender_pk, path, status):
    """Met à jour le statut — utilise la PK."""
    conn = None
    try:
        conn = get_conn()
        conn.execute(
            "UPDATE tenders SET zip_path=?, status=? WHERE id=?",
            (path, status, tender_pk)
        )
        conn.commit()
    except Exception as e:
        log.error(f"update_status id={tender_pk} : {e}")
    finally:
        if conn:
            conn.close()

def get_pending_pvs(limit=50):
    """Récupère les PVs dont le PDF n'a pas encore été téléchargé."""
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            """SELECT id, reference, pdf_url FROM pvs
               WHERE pdf_url IS NOT NULL AND pdf_url != ''
               AND (pdf_path IS NULL OR pdf_path = '')
               ORDER BY created_at DESC
               LIMIT ?""",
            (limit,)
        )
        return c.fetchall()
    except Exception as e:
        log.error(f"get_pending_pvs erreur : {e}")
        return []
    finally:
        if conn:
            conn.close()


def update_pv_path(pv_pk, path):
    """Met à jour le chemin local du PDF dans la table pvs."""
    conn = None
    try:
        conn = get_conn()
        conn.execute(
            "UPDATE pvs SET pdf_path=? WHERE id=?",
            (path, pv_pk)
        )
        conn.commit()
    except Exception as e:
        log.error(f"update_pv_path id={pv_pk} : {e}")
    finally:
        if conn:
            conn.close()


@pipeline_guard("03_downloader")
async def run():
    try:
        init_db()
    except Exception as e:
        notify_critical("03_downloader", e, "Impossible d'initialiser la base de données")
        raise

    try:
        pending = get_pending()
    except Exception as e:
        notify_critical("03_downloader", e, "Impossible de récupérer les offres à télécharger")
        raise

    log.info(f"{len(pending)} offres à télécharger.")
    if not pending:
        return

    ok = errors = 0
    browser = None
    try:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"]
                )
                context = await browser.new_context()
                page = await context.new_page()
            except Exception as e:
                notify_critical("03_downloader", e, "Impossible de lancer Chromium")
                raise

            for row in pending:
                tender_pk = row["id"]
                ref       = row["reference"]
                url       = row["lien_dce"]
                try:
                    path = await process_download(page, url, ref)
                    if path:
                        update_status(tender_pk, path, "TO_ANALYZE")
                        log.info(f"[{ref}] ✅ Téléchargement réussi : {path}")
                        ok += 1
                    else:
                        log.warning(f"[{ref}] Lien non résolu")
                        increment_retry(tender_pk, "Lien non résolu")
                        errors += 1
                except Exception as e:
                    log.error(f"[{ref}] Erreur : {e}")
                    notify_error("03_downloader", e, f"Référence {ref} (id={tender_pk})")
                    increment_retry(tender_pk, str(e))
                    errors += 1

                await asyncio.sleep(2)

            await browser.close()

    except Exception as e:
        notify_critical("03_downloader", e, "Erreur fatale pipeline downloader")
        raise

    # ── Téléchargement PDFs PVs ───────────────────────────────────────────────
    pending_pvs = get_pending_pvs(limit=100)
    log.info(f"{len(pending_pvs)} PDFs PVs à télécharger.")
    pv_ok = pv_errors = 0

    if pending_pvs:
        os.makedirs("downloads/pvs", exist_ok=True)
        # Réutiliser les cookies de la session Playwright si dispo, sinon session simple
        try:
            async with async_playwright() as p2:
                browser2 = await p2.chromium.launch(
                    headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
                )
                context2 = await browser2.new_context()
                page2 = await context2.new_page()

                for row in pending_pvs:
                    pv_pk  = row["id"]
                    ref    = row["reference"]
                    url    = row["pdf_url"]
                    try:
                        # Obtenir les cookies via Playwright, télécharger via requests
                        await page2.goto(url, wait_until="domcontentloaded", timeout=15000)
                        cookies = await context2.cookies()
                        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)

                        safe_ref = re.sub(r'[^\w\-]', '_', ref)[:40]
                        filename = f"downloads/pvs/{pv_pk}_{safe_ref}.pdf"
                        path = download_with_requests(url, cookie_str, filename)
                        if path:
                            update_pv_path(pv_pk, path)
                            log.info(f"[PV {ref}] ✅ {path}")
                            pv_ok += 1
                        else:
                            log.warning(f"[PV {ref}] Téléchargement échoué")
                            pv_errors += 1
                    except Exception as e:
                        log.error(f"[PV {ref}] Erreur : {e}")
                        pv_errors += 1
                    await asyncio.sleep(1.5)

                await browser2.close()
        except Exception as e:
            log.error(f"Erreur téléchargement PVs : {e}")
            notify_error("03_downloader", e, "Erreur téléchargement PDFs PVs")

    notify_success("03_downloader", {
        "Traités":      ok + errors,
        "OK":           ok,
        "Erreurs":      errors,
        "PVs téléchargés": pv_ok,
        "PVs erreurs":  pv_errors,
    })
    log.info(f"=== Fin DCE : {ok} OK | {errors} erreurs | PVs : {pv_ok} OK | {pv_errors} erreurs ===")

if __name__ == "__main__":
    asyncio.run(run())