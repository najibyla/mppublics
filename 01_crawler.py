"""
01_crawler.py - Scraping liste des appels d'offres
Sélecteurs validés v5 | Playwright async | Notifications Telegram

Fix objet/acheteur : text_content() au lieu de inner_text() pour lire
les panneaux collapsés (display:none) du listing.
Déduplication : url_id (id numérique extrait du lien détail), stable et unique.
"""

import asyncio
import logging
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from db import get_conn, init_db, extract_url_id
from utils import (async_retry, compute_hash, human_delay,
                   human_scroll, random_page_delay, human_move_and_click)
from notifier import notify_error, notify_critical, notify_success, notify_warning, pipeline_guard

load_dotenv()

BASE_URL   = os.getenv("BASE_URL", "https://www.marchespublics.gov.ma/index.php")
URL_SEARCH = os.getenv(
    "URL_RECHERCHE_CONCOURS",
    "https://www.marchespublics.gov.ma/index.php"
    "?page=entreprise.EntrepriseAdvancedSearch&searchAnnCons&keyWord=archi"
)
MODULE = "01_crawler"

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRAWLER] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/01_crawler.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


async def get_random_ua():
    try:
        from fake_useragent import UserAgent
        ua = UserAgent(use_cache_server=False, verify_ssl=False)
        agent = ua.random
        log.info(f"User-Agent : {agent[:60]}...")
        return agent
    except Exception as e:
        log.warning(f"fake_useragent indisponible : {e} — fallback UA fixe")
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )


async def get_text_content(locator) -> str:
    """
    Lit le texte d'un élément y compris s'il est caché (display:none).
    text_content() traverse le DOM sans tenir compte de la visibilité CSS,
    contrairement à inner_text() qui ne lit que les éléments visibles.
    Retourne une chaîne vide si l'élément est absent.
    """
    try:
        if not await locator.count():
            return ""
        txt = await locator.text_content() or ""
        # Normaliser les espaces multiples et sauts de ligne
        return re.sub(r'\s+', ' ', txt).strip()
    except Exception:
        return ""


@async_retry(max_attempts=3, delay=5, exceptions=(Exception,))
async def set_filters(page):
    try:
        log.info("Passage à 500 résultats...")
        await random_page_delay(page, min_ms=3000, max_ms=6000)
        sel = page.locator("#ctl0_CONTENU_PAGE_resultSearch_listePageSizeBottom")
        if await sel.count():
            await sel.scroll_into_view_if_needed()
            await human_delay(800, 1500)
            await sel.select_option(value="500")
            await page.wait_for_load_state("networkidle", timeout=25000)
            log.info("Affichage : 500 par page")
        else:
            log.warning("Sélecteur pagination non trouvé — affichage par défaut")
            notify_warning(MODULE, "Sélecteur pagination introuvable")
    except PlaywrightTimeout as e:
        log.error(f"Timeout set_filters : {e}")
        notify_error(MODULE, e, "Timeout lors du passage à 500 résultats")
        raise
    except Exception as e:
        log.error(f"set_filters erreur : {e}")
        notify_error(MODULE, e, "Erreur application des filtres")
        raise


async def parse_rows(page):
    """
    Extrait les offres du tableau — sélecteurs validés depuis v5.

    Correction clé : on utilise text_content() pour les champs qui peuvent être
    dans des panneaux collapsés (objet, acheteur, lieu). text_content() lit le DOM
    complet y compris les nœuds display:none, contrairement à inner_text().

    Fix context destroyed : on attend que le tableau soit stable avant de
    récupérer les lignes. Locator.all() freeze la liste à l'instant T et
    peut planter si la page navigue entre-temps.
    """
    results = []
    try:
        # Attendre que la page soit complètement stable avant de lire le tableau
        await page.wait_for_load_state("networkidle", timeout=20000)

        # Attendre que le tableau soit présent dans le DOM
        table_sel = "table.table-results tbody tr"
        try:
            await page.wait_for_selector(table_sel, timeout=15000)
        except Exception:
            # Fallback sélecteur générique
            table_sel = "table tr:has(td)"
            try:
                await page.wait_for_selector(table_sel, timeout=10000)
            except Exception:
                log.warning("Tableau non trouvé après attente — page vide ou filtrée")
                return results

        # Petite pause pour laisser le JS finir de rendre le DOM
        await asyncio.sleep(1.5)

        # Récupérer les lignes — le contexte est maintenant stable
        rows = await page.locator(table_sel).all()
        if not rows:
            log.warning("0 lignes dans le tableau")
            return results
        log.info(f"{len(rows)} lignes détectées")

        for row in rows:
            try:
                cells = await row.locator("td").all()
                if len(cells) < 5:
                    continue
            except Exception as e:
                # Context destroyed entre deux lignes → on arrête proprement
                if "context was destroyed" in str(e).lower() or "execution context" in str(e).lower():
                    log.warning(f"Contexte détruit pendant la lecture des lignes — {len(results)} offres déjà parsées")
                    return results
                log.debug(f"Ligne ignorée (cells): {e}")
                continue
            try:

                # ── Date publication ──────────────────────────────────────────
                date_pub = ""
                try:
                    cell1_text = await cells[1].inner_text()
                    m = re.search(r'\d{2}/\d{2}/\d{4}', cell1_text)
                    if m:
                        date_pub = m.group(0)
                except Exception:
                    pass

                # ── Référence ─────────────────────────────────────────────────
                reference = ""
                try:
                    ref_el = cells[2].locator("span.ref").first
                    # La référence est toujours visible → inner_text() OK
                    if await ref_el.count():
                        reference = (await ref_el.inner_text()).strip()
                except Exception:
                    pass

                # ── Objet ─────────────────────────────────────────────────────
                # Les panneaux _panelBlocObjet sont collapsés par défaut dans le listing.
                # text_content() lit le texte même si display:none.
                objet = ""
                try:
                    obj_el = cells[2].locator("[id*='_panelBlocObjet'] .info-bulle").first
                    objet = await get_text_content(obj_el)
                    if not objet:
                        # Fallback : prendre tout le panneau et retirer le label
                        obj_el2 = cells[2].locator("[id*='_panelBlocObjet']").first
                        raw = await get_text_content(obj_el2)
                        objet = re.sub(r'^Objet\s*:\s*', '', raw, flags=re.IGNORECASE).strip()
                except Exception:
                    pass

                # ── Acheteur ──────────────────────────────────────────────────
                # Même logique : panneau _panelBlocDenomination souvent caché.
                acheteur = ""
                try:
                    ach_el = cells[2].locator("[id*='_panelBlocDenomination']").first
                    raw = await get_text_content(ach_el)
                    acheteur = re.sub(r'^Acheteur public\s*:\s*', '', raw, flags=re.IGNORECASE).strip()
                except Exception:
                    pass

                # ── Lieu ──────────────────────────────────────────────────────
                lieu = ""
                try:
                    lieu_el = cells[3].locator("[id*='_panelBlocLieuxExec'] .info-bulle").first
                    lieu = await get_text_content(lieu_el)
                    if not lieu:
                        raw = await get_text_content(cells[3].locator("[id*='_panelBlocLieuxExec']").first)
                        lieu = re.sub(r"Lieu d'exécution\s*:?\s*", '', raw, flags=re.IGNORECASE).strip()
                    if not lieu:
                        lieu = re.sub(r"Lieu d'exécution\s*:?\s*", '',
                                      (await cells[3].inner_text()), flags=re.IGNORECASE).strip()
                except Exception:
                    pass

                # ── Date limite ───────────────────────────────────────────────
                date_limite = ""
                try:
                    dl_el = cells[4].locator(".cloture-line").first
                    if await dl_el.count():
                        date_limite = (await dl_el.inner_text()).strip()
                    else:
                        date_limite = (await cells[4].inner_text()).strip()
                except Exception:
                    pass

                # ── Lien détail ───────────────────────────────────────────────
                lien_detail = ""
                try:
                    lnk = cells[5].locator("a[href*='EntrepriseDetailConsultation']").first
                    if await lnk.count():
                        raw = await lnk.get_attribute("href") or ""
                        lien_detail = (BASE_URL + raw) if raw.startswith("?") else raw
                except Exception:
                    pass

                # ── url_id ────────────────────────────────────────────────────
                # Identifiant numérique unique extrait du paramètre id= de l'URL.
                # Clé de déduplication principale — disponible dès le crawler.
                url_id = extract_url_id(lien_detail)

                if reference and len(reference) > 2:
                    results.append({
                        "url_id":           url_id,
                        "reference":        reference,
                        "date_publication": date_pub,
                        "date_limite":      date_limite,
                        "lien_detail":      lien_detail,
                        "objet":            objet,
                        "acheteur":         acheteur,
                        "lieu":             lieu,
                    })

            except Exception as e:
                log.debug(f"Ligne ignorée : {e}")

    except Exception as e:
        log.error(f"Erreur parse_rows : {e}")
        notify_error(MODULE, e, "Erreur parsing tableau résultats")

    return results


async def go_next_page(page):
    try:
        next_btn = page.locator(
            "a:has-text('Suivant'):not(.disabled), a.next:not(.disabled)"
        ).first
        if not await next_btn.count():
            return False
        await next_btn.scroll_into_view_if_needed()
        await human_delay(1500, 3000)
        await human_move_and_click(page, next_btn)
        await page.wait_for_load_state("networkidle", timeout=15000)
        await random_page_delay(page, min_ms=2000, max_ms=5000)
        return True
    except PlaywrightTimeout as e:
        log.warning(f"Timeout pagination : {e}")
        return False
    except Exception as e:
        log.warning(f"go_next_page : {e}")
        return False


def upsert_tenders(rows, page_hash):
    """
    Insère ou met à jour les offres en base.

    Stratégie de déduplication :
    1. Si url_id disponible → dédup sur url_id (clé stable)
    2. Sinon fallback sur reference (comportement ancien, moins fiable)

    Note : reference n'est plus UNIQUE. Une même référence peut exister
    pour plusieurs acheteurs différents — l'url_id les distingue.
    """
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        new_count = updated_count = 0
        now = datetime.utcnow().isoformat()

        for row in rows:
            try:
                uid = row.get("url_id") or ""

                # ── Chercher l'existant ────────────────────────────────────────
                if uid:
                    existing = c.execute(
                        "SELECT id, hash_content FROM tenders WHERE url_id=?",
                        (uid,)
                    ).fetchone()
                else:
                    # Fallback : dédup par reference (cas où l'URL n'a pas d'id=)
                    existing = c.execute(
                        "SELECT id, hash_content FROM tenders WHERE reference=? AND url_id IS NULL",
                        (row["reference"],)
                    ).fetchone()

                if not existing:
                    c.execute(
                        """INSERT INTO tenders
                           (url_id, reference, date_publication, date_limite, lien_detail,
                            objet, acheteur, hash_content, last_seen, status)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'TO_ENRICH')""",
                        (uid or None,
                         row["reference"], row["date_publication"], row["date_limite"],
                         row["lien_detail"], row["objet"], row["acheteur"],
                         page_hash, now)
                    )
                    new_count += 1
                else:
                    if existing["hash_content"] != page_hash:
                        # Contenu modifié → remettre en TO_ENRICH pour ré-enrichissement
                        c.execute(
                            """UPDATE tenders SET
                               date_limite=?, hash_content=?, last_seen=?,
                               objet=CASE WHEN ? != '' THEN ? ELSE objet END,
                               acheteur=CASE WHEN ? != '' THEN ? ELSE acheteur END,
                               status='TO_ENRICH'
                               WHERE id=?""",
                            (row["date_limite"], page_hash, now,
                             row["objet"], row["objet"],
                             row["acheteur"], row["acheteur"],
                             existing["id"])
                        )
                        updated_count += 1
                    else:
                        c.execute(
                            "UPDATE tenders SET last_seen=? WHERE id=?",
                            (now, existing["id"])
                        )
            except Exception as e:
                log.error(f"Upsert {row.get('reference','?')} (url_id={row.get('url_id','')}) : {e}")

        conn.commit()
        return new_count, updated_count

    except Exception as e:
        log.error(f"upsert_tenders DB erreur : {e}")
        notify_error(MODULE, e, "Erreur insertion base de données")
        return 0, 0
    finally:
        if conn:
            conn.close()


@pipeline_guard(MODULE)
async def run():
    try:
        init_db()
    except Exception as e:
        notify_critical(MODULE, e, "Impossible d'initialiser la base de données")
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
                notify_critical(MODULE, e, "Impossible de lancer Chromium")
                raise

            try:
                context = await browser.new_context(
                    user_agent=user_agent,
                    viewport={"width": 1920, "height": 1080},
                    locale="fr-FR",
                )
                page = await context.new_page()
            except Exception as e:
                notify_critical(MODULE, e, "Impossible de créer le contexte navigateur")
                await browser.close()
                raise

            try:
                log.info(f"Chargement : {URL_SEARCH}")
                await page.goto(URL_SEARCH, wait_until="networkidle", timeout=30000)
            except PlaywrightTimeout as e:
                notify_critical(MODULE, e, f"Timeout chargement URL : {URL_SEARCH}")
                await browser.close()
                raise
            except Exception as e:
                notify_critical(MODULE, e, f"Erreur chargement URL : {URL_SEARCH}")
                await browser.close()
                raise

            try:
                await set_filters(page)
            except Exception as e:
                notify_warning(MODULE, f"Filtres non appliqués — on continue quand même : {e}")

            page_num = 1
            total_new = total_updated = total_seen = 0

            while True:
                try:
                    log.info(f"--- Page {page_num} ---")
                    page_content = await page.content()
                    page_hash = compute_hash(page_content)
                    rows = await parse_rows(page)
                    total_seen += len(rows)

                    if not rows:
                        log.warning("Aucune offre — arrêt.")
                        break

                    # Debug : log un sample pour vérifier objet/acheteur
                    if rows and page_num == 1:
                        sample = rows[0]
                        log.info(
                            f"Sample row[0] : ref={sample['reference']} | "
                            f"url_id={sample['url_id']} | "
                            f"objet={sample['objet'][:50] if sample['objet'] else '(vide)'} | "
                            f"acheteur={sample['acheteur'][:50] if sample['acheteur'] else '(vide)'}"
                        )

                    new, updated = upsert_tenders(rows, page_hash)
                    total_new += new
                    total_updated += updated
                    log.info(
                        f"Page {page_num} : {len(rows)} offres | "
                        f"{new} nouvelles | {updated} mises à jour"
                    )

                    if not await go_next_page(page):
                        log.info("Dernière page.")
                        break
                    page_num += 1

                except Exception as e:
                    log.error(f"Erreur page {page_num} : {e}")
                    notify_error(MODULE, e, f"Erreur traitement page {page_num}")
                    break

            await browser.close()

            notify_success(MODULE, {
                "Pages":        page_num,
                "Vues":         total_seen,
                "Nouvelles":    total_new,
                "Mises à jour": total_updated,
            })
            log.info(f"=== Fin : {total_seen} vues | {total_new} nouvelles | {total_updated} mises à jour ===")

    except Exception as e:
        notify_critical(MODULE, e, "Erreur fatale pipeline crawler")
        raise


if __name__ == "__main__":
    asyncio.run(run())
