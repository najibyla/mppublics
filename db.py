"""
db.py - Base SQLite tenders.db
Optimisations :
  - Index complets sur les champs fréquemment interrogés
  - Déduplication sur url_id (id numérique extrait du lien détail) — clé stable et unique par consultation
  - tender_hash = SHA256(reference + date_pub + acheteur) — calculé après enrichissement
  - reference n'est plus UNIQUE (même référence possible chez différents acheteurs)
  - Pragma SQLite pour performance et intégrité
  - Fonctions utilitaires de monitoring
"""

import hashlib
import re
import sqlite3
import os
import logging

log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tenders.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    # ── Pragma SQLite — performance + intégrité ───────────────────────────────
    conn.execute("PRAGMA journal_mode = WAL")       # Write-Ahead Logging : lectures non bloquées par les écritures
    conn.execute("PRAGMA synchronous = NORMAL")     # Bon compromis sécurité/vitesse (vs FULL)
    conn.execute("PRAGMA cache_size = -32000")      # Cache 32 Mo en RAM
    conn.execute("PRAGMA temp_store = MEMORY")      # Tables temporaires en RAM
    conn.execute("PRAGMA foreign_keys = ON")        # Intégrité référentielle
    conn.execute("PRAGMA busy_timeout = 10000")     # Attend 10s si DB verrouillée

    return conn


def compute_tender_hash(reference: str, date_publication: str, acheteur: str) -> str:
    """
    Hash stable = SHA256(reference + date_pub + acheteur normalisé).
    Identifie de façon unique une offre même si la référence est réutilisée
    par un autre acheteur ou à une autre date.
    """
    key = f"{(reference or '').strip()}|{(date_publication or '').strip()}|{(acheteur or '').strip().lower()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def extract_url_id(url: str) -> str:
    """
    Extrait le paramètre id= de l'URL de détail.
    Ex: '?page=...&id=12345' → '12345'
    C'est l'identifiant numérique unique de la consultation sur le portail.
    """
    if not url:
        return ""
    # Le portail utilise refConsultation= comme identifiant unique (pas id=)
    m = re.search(r'[?&]refConsultation=(\d+)', url)
    return m.group(1) if m else ""


def init_db():
    """Crée les tables de base (sans les indexes, ceux-ci sont créés dans migrate_db)."""
    conn = get_conn()
    c = conn.cursor()

    # ── Table principale ──────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS tenders (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Scraping liste (01_crawler)
            url_id               TEXT,            -- id numérique extrait du lien détail (clé unique stable)
            reference            TEXT NOT NULL,   -- référence affichée — PAS unique (même ref possible pour acheteurs différents)
            date_publication     TEXT,
            date_limite          TEXT,
            lien_detail          TEXT,

            -- Enrichissement (02_enricher)
            objet                TEXT,
            acheteur             TEXT,
            estimation           REAL,
            domaines             TEXT,
            contact_nom          TEXT,
            contact_email        TEXT,
            contact_tel          TEXT,
            contact_fax          TEXT,
            lien_dce             TEXT,

            -- Résultats attribution (02_enricher) — PVs sont maintenant dans table pvs
            attributaire         TEXT,
            montant_reel         REAL,
            date_attribution     TEXT,
            nb_soumissionnaires  INTEGER,

            -- Téléchargement (03_downloader)
            zip_path             TEXT,

            -- Analyse (04_analyzer)
            resume_txt           TEXT,

            -- Synchronisation (05_sync)
            drive_folder_url     TEXT,

            -- Déduplication et tracking
            tender_hash          TEXT,            -- SHA256(reference|date_pub|acheteur) calculé après enrichissement
            hash_content         TEXT,            -- MD5 du contenu page détail
            hash_dce             TEXT,            -- MD5 du contenu DCE (détecte si DCE mis à jour)
            last_seen            DATETIME,        -- Dernière fois détecté en ligne
            retry_count          INTEGER DEFAULT 0,
            last_error           TEXT,            -- Dernier message d'erreur

            -- Machine à états
            status               TEXT NOT NULL DEFAULT 'TO_ENRICH',

            -- Timestamps
            created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── Table des PVs (Procès-Verbaux) — 1 tender peut avoir N PVs ─────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS pvs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id            INTEGER NOT NULL,  -- FK vers tenders.id
            reference            TEXT,              -- copie de la référence (pour indexation rapide)
            pdf_url              TEXT,              -- URL du PV PDF
            procedure_type       TEXT,              -- Type de procédure (Concours Arch, Phase 1, Phase 2, etc.)
            date_publication     TEXT,              -- Date de publication du PV
            hash_pdf             TEXT,              -- Hash du PDF (déduplication)
            synced_to_sheets     INTEGER DEFAULT 0, -- Marqueur sync
            created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY(tender_id) REFERENCES tenders(id) ON DELETE CASCADE
        )
    """)

    # ── Table de log des changements (audit trail) ────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS tenders_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id    INTEGER,              -- FK vers tenders.id
            reference    TEXT,                -- copie pour lisibilité
            field        TEXT NOT NULL,       -- Champ modifié
            old_value    TEXT,
            new_value    TEXT,
            changed_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── Trigger : updated_at automatique ─────────────────────────────────────
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_tenders_updated_at
        AFTER UPDATE ON tenders
        BEGIN
            UPDATE tenders SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END
    """)

    # ── Trigger : log des changements de statut ───────────────────────────────
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_tenders_status_history
        AFTER UPDATE OF status ON tenders
        WHEN OLD.status != NEW.status
        BEGIN
            INSERT INTO tenders_history (tender_id, reference, field, old_value, new_value)
            VALUES (NEW.id, NEW.reference, 'status', OLD.status, NEW.status);
        END
    """)

    # ── Trigger : log des changements d'estimation ────────────────────────────
    c.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_tenders_estimation_history
        AFTER UPDATE OF estimation ON tenders
        WHEN OLD.estimation IS NOT NEW.estimation
        BEGIN
            INSERT INTO tenders_history (tender_id, reference, field, old_value, new_value)
            VALUES (NEW.id, NEW.reference, 'estimation',
                    CAST(OLD.estimation AS TEXT), CAST(NEW.estimation AS TEXT));
        END
    """)

    # Les indexes sont créés dans migrate_db() après la migration des colonnes
    # (pour éviter les erreurs si une colonne manque dans une vieille DB)

    conn.commit()
    conn.close()
    log.info(f"[DB] Base initialisée : {DB_PATH}")
    print(f"[DB] Base initialisée : {DB_PATH}")


def migrate_db():
    """
    Ajoute les colonnes/tables manquantes si upgrade depuis une version antérieure.
    Gère la migration vers le schéma url_id / tender_hash / table pvs.
    """
    conn = get_conn()
    c = conn.cursor()

    new_columns = [
        ("url_id",              "TEXT"),
        ("tender_hash",         "TEXT"),
        ("hash_content",        "TEXT"),
        ("hash_dce",            "TEXT"),
        ("last_seen",           "DATETIME"),
        ("retry_count",         "INTEGER DEFAULT 0"),
        ("last_error",          "TEXT"),
        ("attributaire",        "TEXT"),
        ("montant_reel",        "REAL"),
        ("date_attribution",    "TEXT"),
        ("nb_soumissionnaires", "INTEGER"),
    ]

    existing = {row[1] for row in c.execute("PRAGMA table_info(tenders)")}
    migrated = []
    for col_name, col_type in new_columns:
        if col_name not in existing:
            # Ne pas ajouter pv_url (il sera supprimé car PVs sont maintenant dans la table pvs)
            if col_name != "pv_url":
                c.execute(f"ALTER TABLE tenders ADD COLUMN {col_name} {col_type}")
                migrated.append(col_name)

    if migrated:
        conn.commit()
        print(f"[DB] Colonnes ajoutées : {', '.join(migrated)}")

    # ── Créer la table pvs si elle n'existe pas ───────────────────────────────
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS pvs (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id            INTEGER NOT NULL,
                reference            TEXT,
                pdf_url              TEXT,
                procedure_type       TEXT,
                date_publication     TEXT,
                hash_pdf             TEXT,
                synced_to_sheets     INTEGER DEFAULT 0,
                created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY(tender_id) REFERENCES tenders(id) ON DELETE CASCADE
            )
        """)
        conn.commit()
        print("[DB] Table pvs créée")
    except Exception as e:
        print(f"[DB] Table pvs (non bloquant) : {e}")

    # ── Remplir url_id depuis lien_detail pour les enregistrements existants ──
    rows_without_url_id = c.execute(
        "SELECT id, lien_detail FROM tenders WHERE url_id IS NULL AND lien_detail IS NOT NULL"
    ).fetchall()

    if rows_without_url_id:
        updated = 0
        for row in rows_without_url_id:
            uid = extract_url_id(row["lien_detail"] or "")
            if uid:
                try:
                    c.execute("UPDATE tenders SET url_id=? WHERE id=?", (uid, row["id"]))
                    updated += 1
                except Exception:
                    pass  # Collision url_id (doublon) — on laisse NULL
        conn.commit()
        if updated:
            print(f"[DB] url_id rempli pour {updated} enregistrements existants")

    # ── Créer les index manquants ──────────────────────────────────────────────
    try:
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_url_id
            ON tenders(url_id) WHERE url_id IS NOT NULL
        """)
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tender_hash
            ON tenders(tender_hash) WHERE tender_hash IS NOT NULL
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_pvs_tender_id ON pvs(tender_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_pvs_reference ON pvs(reference)")
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pvs_hash
            ON pvs(hash_pdf) WHERE hash_pdf IS NOT NULL
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_pvs_synced ON pvs(synced_to_sheets)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_pvs_created_at ON pvs(created_at)")
        conn.commit()
    except Exception as e:
        print(f"[DB] Index (non bloquant) : {e}")

    if not migrated and not rows_without_url_id:
        print("[DB] Schéma à jour, aucune migration nécessaire")

    conn.close()


def optimize_db():
    """
    Maintenance périodique de la base.
    À lancer via 06_cleanup.sh ou manuellement.
    """
    conn = get_conn()
    print("[DB] Optimisation en cours...")

    conn.execute("VACUUM")
    conn.execute("ANALYZE")

    result = conn.execute("PRAGMA integrity_check").fetchone()
    status = result[0] if result else "unknown"

    conn.close()
    print(f"[DB] Optimisation terminée — intégrité : {status}")
    return status == "ok"


# ── Fonctions utilitaires de monitoring ──────────────────────────────────────

def get_stats():
    """Retourne les statistiques de la base."""
    conn = get_conn()
    c = conn.cursor()

    stats = {}

    rows = c.execute(
        "SELECT status, COUNT(*) n FROM tenders GROUP BY status ORDER BY n DESC"
    ).fetchall()
    stats["by_status"] = {r["status"]: r["n"] for r in rows}
    stats["total"] = sum(stats["by_status"].values())

    stats["new_today"] = c.execute(
        "SELECT COUNT(*) n FROM tenders WHERE DATE(created_at) = DATE('now')"
    ).fetchone()["n"]

    stats["high_budget"] = c.execute(
        "SELECT COUNT(*) n FROM tenders WHERE estimation > 70000000"
    ).fetchone()["n"]

    stats["errors"] = c.execute(
        "SELECT COUNT(*) n FROM tenders WHERE status LIKE 'ERROR%'"
    ).fetchone()["n"]

    stats["without_url_id"] = c.execute(
        "SELECT COUNT(*) n FROM tenders WHERE url_id IS NULL"
    ).fetchone()["n"]

    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    stats["db_size_mb"] = round(db_size, 2)

    conn.close()
    return stats





def reset_errors(status_filter="ERROR%"):
    """Remet en queue les offres en erreur pour un nouveau cycle."""
    conn = get_conn()
    c = conn.cursor()

    error_map = {
        "ERROR_ENRICH":   "TO_ENRICH",
        "ERROR_DOWNLOAD": "TO_DOWNLOAD",
        "ERROR_ANALYZE":  "TO_ANALYZE",
        "ERROR_SYNC":     "TO_SYNC",
    }

    total = 0
    for error_status, prev_status in error_map.items():
        c.execute(
            """UPDATE tenders SET status=?, retry_count=0, last_error=NULL
               WHERE status=?""",
            (prev_status, error_status)
        )
        total += c.rowcount

    conn.commit()
    conn.close()
    print(f"[DB] {total} offres remises en queue")
    return total


if __name__ == "__main__":
    import sys
    if "--optimize" in sys.argv:
        optimize_db()
        sys.exit(0)
    init_db()
    migrate_db()

    stats = get_stats()
    if stats["total"] > 0:
        print(f"\n[DB] Statistiques :")
        for status, count in stats["by_status"].items():
            print(f"  {status:<25} : {count}")
        print(f"  {'Total':<25} : {stats['total']}")
        new_today_label = "Nouvelles aujourd'hui"
        print(f"  {new_today_label:<25} : {stats['new_today']}")
        print(f"  {'Budget > 70M':<25} : {stats['high_budget']}")
        print(f"  {'Erreurs':<25} : {stats['errors']}")
        print(f"  {'Sans url_id':<25} : {stats['without_url_id']}")
        print(f"  {'Taille base':<25} : {stats['db_size_mb']} Mo")
