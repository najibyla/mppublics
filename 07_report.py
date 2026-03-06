"""
07_report.py - Rapport quotidien par email
Résumé : nouvelles offres, prioritaires, expirant bientôt, erreurs
"""

import logging
import os
import smtplib
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from db import get_conn, init_db
from notifier import notify_error, notify_critical, notify_success, pipeline_guard

load_dotenv()
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [REPORT] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/07_report.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "portailmpma@gmail.com")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
REPORT_EMAIL  = os.getenv("REPORT_EMAIL", "portailmpma@gmail.com")

BUDGET_SEUIL = float(os.getenv("BUDGET_SEUIL", "70000000"))
JOURS_ALERTE = int(os.getenv("JOURS_ALERTE", "7"))


def get_stats():
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()

        # Nouvelles offres des dernières 24h
        new_today = c.execute(
            "SELECT COUNT(*) as n FROM tenders WHERE DATE(created_at) = DATE('now')"
        ).fetchone()["n"]

        # Total en base
        total = c.execute(
            "SELECT COUNT(*) as n FROM tenders"
        ).fetchone()["n"]

        # Statuts
        statuts = c.execute(
            "SELECT status, COUNT(*) as n FROM tenders GROUP BY status ORDER BY n DESC"
        ).fetchall()

        # Offres prioritaires (budget > 70M)
        prioritaires = c.execute(
            """SELECT reference, objet, acheteur, estimation, date_limite
               FROM tenders
               WHERE estimation > ? AND status='DONE'
               ORDER BY estimation DESC LIMIT 10""",
            (BUDGET_SEUIL,)
        ).fetchall()

        # Offres expirant bientôt
        all_rows = c.execute(
            """SELECT reference, objet, acheteur, date_limite
               FROM tenders
               WHERE status IN ('TO_ENRICH','TO_DOWNLOAD','TO_ANALYZE','TO_SYNC','DONE')
               AND date_limite IS NOT NULL
               ORDER BY date_limite ASC"""
        ).fetchall()

        expiring_soon = []
        for row in all_rows:
            try:
                date_part = (row["date_limite"] or "").split(" ")[0]
                dl = datetime.strptime(date_part, "%d/%m/%Y").date()
                jours = (dl - date.today()).days
                if 0 <= jours <= JOURS_ALERTE:
                    expiring_soon.append({**dict(row), "jours": jours})
            except Exception:
                pass

        # Erreurs
        errors = c.execute(
            "SELECT COUNT(*) as n FROM tenders WHERE status LIKE 'ERROR%'"
        ).fetchone()["n"]

        # Nouveaux PVs
        try:
            pvs_today = c.execute(
                "SELECT COUNT(*) as n FROM pvs WHERE DATE(created_at) = DATE('now')"
            ).fetchone()["n"]
        except Exception:
            pvs_today = 0

        return {
            "new_today":     new_today,
            "total":         total,
            "statuts":       statuts,
            "prioritaires":  prioritaires,
            "expiring_soon": expiring_soon,
            "errors":        errors,
            "pvs_today":     pvs_today,
        }

    except Exception as e:
        log.error(f"get_stats erreur : {e}")
        raise
    finally:
        if conn:
            conn.close()


def build_email(stats):
    today_str = date.today().strftime("%d/%m/%Y")
    lines = [
        f"📊 RAPPORT QUOTIDIEN — Marchés Publics — {today_str}",
        "=" * 60,
        "",
        f"🆕 Nouvelles offres aujourd'hui : {stats['new_today']}",
        f"📋 Nouveaux PVs aujourd'hui    : {stats.get('pvs_today', 0)}",
        f"📁 Total en base               : {stats['total']}",
        f"❌ Erreurs pipeline            : {stats['errors']}",
        "",
        "── Statuts ──",
    ]
    for s in stats["statuts"]:
        lines.append(f"  {s['status']:<20}: {s['n']}")

    if stats["prioritaires"]:
        lines += [
            "",
            f"🚨 OFFRES PRIORITAIRES (Budget > 70M DH) :",
            "─" * 40,
        ]
        for o in stats["prioritaires"]:
            budget = f"{int(o['estimation']):,}".replace(",", " ") if o["estimation"] else "N/A"
            lines.append(
                f"  [{o['reference']}] {(o['objet'] or '')[:50]}\n"
                f"    Acheteur : {o['acheteur'] or 'N/A'}\n"
                f"    Budget   : {budget} DH | Limite : {o['date_limite'] or 'N/A'}"
            )

    if stats["expiring_soon"]:
        lines += [
            "",
            f"⏰ OFFRES EXPIRANT DANS {JOURS_ALERTE} JOURS :",
            "─" * 40,
        ]
        for o in stats["expiring_soon"]:
            lines.append(
                f"  [{o['reference']}] {(o['objet'] or '')[:50]}\n"
                f"    Limite : {o['date_limite']} — dans {o['jours']} jour(s)"
            )

    lines += ["", "─" * 60, "Portail MPMA — Pipeline automatisé"]
    return "\n".join(lines)


def send_report(body):
    if not SMTP_PASSWORD:
        log.warning("SMTP_PASSWORD non configuré — rapport non envoyé")
        print(body)
        return
    try:
        msg = MIMEMultipart()
        msg["Subject"] = f"[Marchés Publics] Rapport {date.today().strftime('%d/%m/%Y')}"
        msg["From"]    = SMTP_USER
        msg["To"]      = REPORT_EMAIL
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)
        log.info("Rapport envoyé")
    except Exception as e:
        log.error(f"Envoi rapport : {e}")
        notify_error("07_report", e, "Impossible d'envoyer le rapport par email")
        print(body)


@pipeline_guard("07_report")
def run():
    try:
        init_db()
    except Exception as e:
        notify_critical("07_report", e, "Impossible d'initialiser la base de données")
        raise

    try:
        stats = get_stats()
    except Exception as e:
        notify_critical("07_report", e, "Impossible de récupérer les statistiques")
        raise

    body = build_email(stats)
    log.info(f"Rapport : {stats['new_today']} nouvelles | {stats['total']} total | {stats['errors']} erreurs")

    try:
        send_report(body)
    except Exception as e:
        notify_error("07_report", e, "Erreur lors de l'envoi du rapport")

    notify_success("07_report", {
        "Nouvelles aujourd'hui": stats["new_today"],
        "Total":                 stats["total"],
        "Erreurs pipeline":      stats["errors"],
    })


if __name__ == "__main__":
    run()
