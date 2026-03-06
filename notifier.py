"""
notifier.py - Gestionnaire centralisé d'erreurs + notifications Telegram
Utilisé par tous les modules du pipeline.
"""

import logging
import os
import traceback
from datetime import datetime
from functools import wraps

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ENABLE_TELEGRAM  = os.getenv("ENABLE_TELEGRAM", "false").lower() == "true"

# Icônes par niveau
ICONS = {
    "critical": "🚨",
    "error":    "❌",
    "warning":  "⚠️",
    "info":     "ℹ️",
    "success":  "✅",
}


def send_telegram(message: str, level: str = "info") -> bool:
    """
    Envoie un message Telegram formaté.
    Retourne True si envoyé, False sinon.
    """
    if not ENABLE_TELEGRAM or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    icon = ICONS.get(level, "ℹ️")
    ts   = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    text = f"{icon} *[{ts}]*\n{message}"

    # Telegram limite à 4096 caractères
    if len(text) > 4000:
        text = text[:3990] + "\n...[tronqué]"

    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "Markdown",
    }
    try:
        r = requests.post(url, data=data, timeout=10)
        if r.status_code != 200:
            log.warning(f"Telegram HTTP {r.status_code} : {r.text[:100]}")
            return False
        return True
    except requests.exceptions.ConnectionError:
        log.warning("Telegram : pas de connexion internet")
        return False
    except Exception as e:
        log.warning(f"Telegram erreur : {e}")
        return False


def notify_error(module: str, error: Exception, context: str = ""):
    """
    Notifie une erreur sur Telegram avec le traceback.
    """
    tb = traceback.format_exc()
    # Limiter le traceback à 800 chars
    tb_short = tb[-800:] if len(tb) > 800 else tb

    msg = (
        f"*Module :* `{module}`\n"
        f"*Erreur :* `{type(error).__name__}: {str(error)[:200]}`\n"
    )
    if context:
        msg += f"*Contexte :* {context[:200]}\n"
    msg += f"*Traceback :*\n```\n{tb_short}\n```"

    send_telegram(msg, level="error")


def notify_critical(module: str, error: Exception, context: str = ""):
    """Erreur fatale qui stoppe le module."""
    tb = traceback.format_exc()
    tb_short = tb[-800:] if len(tb) > 800 else tb

    msg = (
        f"*🛑 ERREUR FATALE — Module :* `{module}`\n"
        f"*Erreur :* `{type(error).__name__}: {str(error)[:200]}`\n"
    )
    if context:
        msg += f"*Contexte :* {context[:200]}\n"
    msg += f"*Traceback :*\n```\n{tb_short}\n```"

    send_telegram(msg, level="critical")


def notify_success(module: str, stats: dict):
    """Notification de fin de module avec statistiques."""
    lines = [f"*Module :* `{module}`"]
    for k, v in stats.items():
        lines.append(f"  • {k} : *{v}*")
    send_telegram("\n".join(lines), level="success")


def notify_warning(module: str, message: str):
    send_telegram(f"*Module :* `{module}`\n{message}", level="warning")


def pipeline_guard(module_name: str):
    """
    Décorateur pour les fonctions run() de chaque module.
    Capture toutes les exceptions non gérées et les envoie sur Telegram.
    """
    def decorator(fn):
        @wraps(fn)
        async def async_wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except KeyboardInterrupt:
                notify_warning(module_name, "⏹️ Arrêt manuel (KeyboardInterrupt)")
                raise
            except Exception as e:
                notify_critical(module_name, e)
                raise

        @wraps(fn)
        def sync_wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except KeyboardInterrupt:
                notify_warning(module_name, "⏹️ Arrêt manuel (KeyboardInterrupt)")
                raise
            except Exception as e:
                notify_critical(module_name, e)
                raise

        import asyncio
        if asyncio.iscoroutinefunction(fn):
            return async_wrapper
        return sync_wrapper

    return decorator
