"""
utils.py - Utilitaires partagés : retry, hash, normalisation, comportement humain
"""

import asyncio
import hashlib
import logging
import random
import re
from functools import wraps

log = logging.getLogger(__name__)


# ─── Retry decorator (async) ──────────────────────────────────────────────────

def async_retry(max_attempts=3, delay=5, exceptions=(Exception,)):
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        log.error(f"{fn.__name__} échoué après {max_attempts} tentatives : {e}")
                        raise
                    wait = delay * attempt
                    log.warning(f"{fn.__name__} tentative {attempt}/{max_attempts} : {e}. Retry dans {wait}s...")
                    await asyncio.sleep(wait)
        return wrapper
    return decorator


def sync_retry(max_attempts=3, delay=5, exceptions=(Exception,)):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            import time
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        log.error(f"{fn.__name__} échoué après {max_attempts} tentatives : {e}")
                        raise
                    wait = delay * attempt
                    log.warning(f"{fn.__name__} tentative {attempt}/{max_attempts} : {e}. Retry dans {wait}s...")
                    time.sleep(wait)
        return wrapper
    return decorator


# ─── Comportement humain (anti-WAF) ──────────────────────────────────────────

async def human_delay(min_ms=1500, max_ms=4000):
    """Pause aléatoire entre deux actions — imite le temps de lecture humain."""
    delay = random.randint(min_ms, max_ms)
    await asyncio.sleep(delay / 1000)


async def human_scroll(page, steps=None):
    """
    Scroll progressif et aléatoire sur la page.
    Imite un humain qui lit le contenu avant de cliquer.
    """
    if steps is None:
        steps = random.randint(2, 5)
    for _ in range(steps):
        scroll_amount = random.randint(200, 600)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await asyncio.sleep(random.uniform(0.3, 0.8))


async def human_move_and_click(page, element):
    """
    Déplace la souris progressivement vers l'élément avant de cliquer.
    Imite le mouvement naturel de la souris.
    """
    box = await element.bounding_box()
    if box:
        # Point de destination avec légère variation aléatoire
        target_x = box["x"] + box["width"] * random.uniform(0.3, 0.7)
        target_y = box["y"] + box["height"] * random.uniform(0.3, 0.7)
        await page.mouse.move(target_x, target_y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.3))
    await element.click()


async def human_type(element, text, min_delay=50, max_delay=150):
    """
    Saisie caractère par caractère avec délai aléatoire.
    Imite la vitesse de frappe humaine.
    """
    await element.click()
    await asyncio.sleep(random.uniform(0.2, 0.5))
    for char in text:
        await element.type(char, delay=random.randint(min_delay, max_delay))


async def random_page_delay(page, min_ms=3000, max_ms=8000):
    """
    Pause longue entre le chargement d'une page et l'interaction.
    Imite le temps de lecture d'un humain.
    """
    delay = random.randint(min_ms, max_ms)
    log.debug(f"Pause humaine : {delay}ms")
    # Petit scroll pendant la pause pour simuler la lecture
    await page.wait_for_timeout(delay // 2)
    await human_scroll(page, steps=random.randint(1, 3))
    await page.wait_for_timeout(delay // 2)


# ─── Hash ─────────────────────────────────────────────────────────────────────

def compute_hash(content: str) -> str:
    return hashlib.md5(content.encode("utf-8", errors="replace")).hexdigest()


# ─── Normalisation ────────────────────────────────────────────────────────────

def normalize_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if re.match(r'\d{2}/\d{2}/\d{4}', raw):
        return raw
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}:\d{2}))?', raw)
    if m:
        base = f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
        return f"{base} {m.group(4)}" if m.group(4) else base
    return raw


def normalize_montant(val) -> str:
    if val is None:
        return ""
    try:
        entier = int(round(float(val)))
        return f"{entier:,}".replace(",", " ")
    except (ValueError, TypeError):
        return str(val)


def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    cleaned = raw.replace(" ", "").replace("-", "").replace(".", "")
    results = []
    numbers = re.findall(r'(?:\+212|00212|0)([5-7]\d{8})', cleaned)
    for num in numbers:
        results.append(f"0{num}")
    ext_match = re.search(r'(?:\+212|00212|0)([5-7]\d{6})(\d{2})/(\d{2})', cleaned)
    if ext_match:
        base = ext_match.group(1)
        num_a = f"0{base}{ext_match.group(2)}"
        num_b = f"0{base}{ext_match.group(3)}"
        results = list(dict.fromkeys(results + [num_a, num_b]))
    return "\n".join(results) if results else raw


def parse_montant(text: str):
    if not text:
        return None
    cleaned = re.sub(r'[A-Za-zÀ-ÿ\s]', '', text.replace('\xa0', ' ').replace(',', '.'))
    match = re.search(r'\d[\d.]*', cleaned)
    if match:
        try:
            return float(match.group())
        except ValueError:
            pass
    return None
