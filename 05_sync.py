import logging
import os
import gspread
from pathlib import Path
from datetime import datetime, date

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from db import get_conn, init_db
from utils import normalize_date, normalize_montant, normalize_phone
from notifier import notify_error, notify_critical

load_dotenv()
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SYNC] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/05_sync.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# Configuration
TOKEN_PATH      = os.getenv("TOKEN_PATH", "token.json")
SPREADSHEET_ID  = os.getenv("SPREADSHEET_ID", "")
SHEET_NAME      = os.getenv("SHEET_NAME", "Marchés Publics Maroc Test")
BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "100"))

HEADERS_TENDERS = [
    "Référence", "Date Publication", "Date Limite", "Objet", "Acheteur",
    "Estimation (DH)", "Domaines", "Contact Nom", "Contact Email",
    "Contact Tél", "Contact Fax", "Attributaire", 
    "Montant Réel (DH)", "Date Attribution", "Nb Soumissionnaires",
    "Jours Restants", "STATUS"
]

HEADERS_PVS = [
    "Référence", "Procédure", "Date Publication", "Lien PDF", "Synchronisé"
]

def get_gsheet_client():
    if not Path(TOKEN_PATH).exists():
        raise FileNotFoundError("token.json introuvable")
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, [
        "https://www.googleapis.com/auth/spreadsheets", 
        "https://www.googleapis.com/auth/drive"
    ])
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

def compute_jours_restants(date_limite_str):
    try:
        date_part = str(date_limite_str).split(" ")[0]
        dl = datetime.strptime(date_part, "%d/%m/%Y").date()
        diff = (dl - date.today()).days
        return "Expiré" if diff < 0 else str(diff)
    except Exception:
        return ""


def _get_or_create_worksheet(sheet, name, rows=2000, cols=20, headers=None):
    """Récupère ou crée un onglet Google Sheets."""
    try:
        return sheet.worksheet(name)
    except Exception:
        log.info(f"Création de l'onglet '{name}'...")
        ws = sheet.add_worksheet(name, rows=rows, cols=cols)
        if headers:
            ws.append_row(headers)
        return ws


def run():
    conn = None
    try:
        init_db()
    except Exception as e:
        notify_critical("05_sync", e, "Impossible d'initialiser la base de données")
        raise

    try:
        conn = get_conn()
        c = conn.cursor()

        # --- Tenders : TO_SYNC (fallback TO_ANALYZE) ---
        c.execute("SELECT * FROM tenders WHERE status='TO_SYNC' LIMIT ?", (BATCH_SIZE,))
        pending = c.fetchall()

        if not pending:
            log.info("Aucun 'TO_SYNC'. Essai avec 'TO_ANALYZE'...")
            c.execute("SELECT * FROM tenders WHERE status='TO_ANALYZE' LIMIT ?", (BATCH_SIZE,))
            pending = c.fetchall()

        if not pending:
            log.info("Rien à synchroniser.")
            return

        log.info(f"{len(pending)} tenders à synchroniser")

        # --- Connexion Google Sheets ---
        try:
            sheet = get_gsheet_client()
        except Exception as e:
            notify_critical("05_sync", e, "Authentification Google Sheets échouée")
            raise

        # ── Onglet 1 : Tenders ────────────────────────────────────────────────
        try:
            worksheet = _get_or_create_worksheet(
                sheet, SHEET_NAME, cols=20, headers=HEADERS_TENDERS
            )
            existing_refs = {
                str(r[0]).replace("'", "")
                for r in worksheet.get_all_values()[1:]
                if r
            }
        except Exception as e:
            notify_error("05_sync", e, "Impossible d'accéder à l'onglet Tenders")
            raise

        new_rows = []
        processed_ids = []

        for row in pending:
            tender_id = row["id"]
            ref       = row["reference"]
            try:
                if ref in existing_refs:
                    processed_ids.append(tender_id)
                    continue

                estimation = normalize_montant(row["estimation"])
                jours      = compute_jours_restants(row["date_limite"])

                try:
                    est_val = float(row["estimation"] or 0)
                except (ValueError, TypeError):
                    est_val = 0

                new_rows.append([
                    f"'{ref}",
                    normalize_date(row["date_publication"] or ""),
                    normalize_date(row["date_limite"]     or ""),
                    row["objet"]         or "",
                    row["acheteur"]      or "",
                    estimation,
                    row["domaines"]      or "",
                    row["contact_nom"]   or "",
                    row["contact_email"] or "",
                    normalize_phone(row["contact_tel"] or ""),
                    normalize_phone(row["contact_fax"] or ""),
                    row["attributaire"]  or "",
                    normalize_montant(row["montant_reel"]),
                    normalize_date(row["date_attribution"] or ""),
                    row["nb_soumissionnaires"] or "",
                    jours,
                    "OUI" if est_val > 70000000 else "NON",
                ])
                processed_ids.append(tender_id)

            except Exception as e:
                log.error(f"[{ref}] Erreur construction ligne : {e}")

        if new_rows:
            try:
                worksheet.append_rows(new_rows)
                log.info(f"✅ {len(new_rows)} tenders → Google Sheets")
            except Exception as e:
                notify_error("05_sync", e, f"{len(new_rows)} lignes non envoyées")
                raise

        # ── Onglet 2 : PVs ────────────────────────────────────────────────────
        pv_ids_synced = []
        try:
            c.execute(
                """SELECT id, reference, procedure_type, date_publication, pdf_url
                   FROM pvs WHERE synced_to_sheets=0
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (BATCH_SIZE,)
            )
            pending_pvs = c.fetchall()

            if pending_pvs:
                pvs_sheet = _get_or_create_worksheet(
                    sheet, "PVs", cols=10, headers=HEADERS_PVS
                )
                pv_rows = []
                for pv in pending_pvs:
                    try:
                        pv_rows.append([
                            pv["reference"]       or "",
                            pv["procedure_type"]  or "",
                            normalize_date(pv["date_publication"] or ""),
                            pv["pdf_url"]         or "",
                            "✓",
                        ])
                        pv_ids_synced.append(pv["id"])
                    except Exception as e:
                        log.warning(f"PV {pv.get('id')} ignoré : {e}")

                if pv_rows:
                    pvs_sheet.append_rows(pv_rows)
                    log.info(f"✅ {len(pv_rows)} PVs → Google Sheets")

        except Exception as e:
            log.warning(f"Onglet PVs non synchronisé : {e}")

        # ── Commit DB ─────────────────────────────────────────────────────────
        for tender_id in processed_ids:
            try:
                c.execute("UPDATE tenders SET status='DONE' WHERE id=?", (tender_id,))
            except Exception as e:
                log.error(f"UPDATE DONE tender id={tender_id} : {e}")

        if pv_ids_synced:
            placeholders = ",".join("?" * len(pv_ids_synced))
            try:
                c.execute(
                    f"UPDATE pvs SET synced_to_sheets=1 WHERE id IN ({placeholders})",
                    pv_ids_synced
                )
            except Exception as e:
                log.error(f"UPDATE pvs synced : {e}")

        conn.commit()
        log.info(
            f"=== Fin : {len(processed_ids)} tenders DONE | "
            f"{len(pv_ids_synced)} PVs synchronisés ==="
        )

    except Exception as e:
        log.error(f"Erreur run : {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    run()