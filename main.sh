#!/bin/bash
# main.sh - Orchestrateur pipeline ETL Marchés Publics v2
#
# CRONTAB RECOMMANDÉE :
# ─────────────────────────────────────────────────────────────────────
# # Scraping + enrichissement : 1 fois par jour à 7h (heure creuse)
# 0 7 * * * /chemin/vers/main.sh --scrape-only
#
# # Téléchargement + analyse + sync : toutes les 2h de 9h à 19h
# 0 9,11,13,15,17,19 * * * /chemin/vers/main.sh --process-only
#
# # Rapport quotidien : 8h chaque matin
# 0 8 * * * /chemin/vers/main.sh --report-only
# ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$(which python3)"
LOG_DIR="$SCRIPT_DIR/logs"
LOGFILE="$LOG_DIR/main.log"
LOCK_FILE="/tmp/marchespublics_pipeline.lock"

mkdir -p "$LOG_DIR"

# ─── Lock anti-concurrence ────────────────────────────────────────────────────
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MAIN] Déjà en cours (PID $PID), abandon." | tee -a "$LOGFILE"
        exit 0
    else
        rm -f "$LOCK_FILE"
    fi
fi
echo $$ > "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

# ─── Environnement ────────────────────────────────────────────────────────────
[ -f "$SCRIPT_DIR/.env" ] && export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
cd "$SCRIPT_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [MAIN] $1" | tee -a "$LOGFILE"; }

# ─── Notification Telegram en cas d'erreur shell ─────────────────────────────
notify_fail() {
    local MODULE="$1"
    local CODE="$2"
    local TOKEN="${TELEGRAM_TOKEN:-}"
    local CHAT_ID="${TELEGRAM_CHAT_ID:-}"
    local ENABLED="${ENABLE_TELEGRAM:-false}"

    [ "$ENABLED" != "true" ] && return
    [ -z "$TOKEN" ] || [ -z "$CHAT_ID" ] && return

    local TS
    TS=$(date '+%d/%m/%Y %H:%M:%S')
    local MSG
    MSG=$(printf '🚨 *[%s]*\n*Module :* `%s`\n*Erreur :* Code de sortie `%s`\n*Hôte :* `%s`' \
        "$TS" "$MODULE" "$CODE" "$(hostname)")

    curl -s -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
        -d "chat_id=${CHAT_ID}" \
        -d "parse_mode=Markdown" \
        --data-urlencode "text=${MSG}" \
        --connect-timeout 10 > /dev/null 2>&1 || true
}

# Vérifier la charge système avant de lancer
check_load() {
    local LOAD=$(awk '{print int($1)}' /proc/loadavg)
    local CPUS=$(nproc)
    local THRESHOLD=$((CPUS * 2))
    if [ "$LOAD" -gt "$THRESHOLD" ]; then
        log "⚠️  Charge système élevée (load=$LOAD, seuil=$THRESHOLD) — report de 15 min"
        sleep 900
        # Revérifier après attente
        LOAD=$(awk '{print int($1)}' /proc/loadavg)
        if [ "$LOAD" -gt "$THRESHOLD" ]; then
            log "Charge toujours élevée — abandon de ce cycle"
            rm -f "$LOCK_FILE"
            exit 0
        fi
    fi
}

# Vérifier la RAM disponible
check_ram() {
    local FREE_GB=$(awk '/MemAvailable/ {printf "%d", $2/1024/1024}' /proc/meminfo)
    if [ "$FREE_GB" -lt 3 ]; then
        log "⚠️  RAM disponible faible (${FREE_GB} Go libres) — report de 15 min"
        sleep 900
        FREE_GB=$(awk '/MemAvailable/ {printf "%d", $2/1024/1024}' /proc/meminfo)
        if [ "$FREE_GB" -lt 3 ]; then
            log "RAM toujours insuffisante — abandon de ce cycle"
            rm -f "$LOCK_FILE"
            exit 0
        fi
    fi
    log "RAM disponible : ${FREE_GB} Go"
}

run_module() {
    local MODULE=$1
    log ">>> $MODULE"
    # nice +10 = priorité basse CPU | ionice -c3 = priorité basse disque
    # Moodle et ERPNext gardent la priorité
    nice -n "${CPU_NICE:-10}" ionice -c3 \
        "$PYTHON" "$SCRIPT_DIR/$MODULE" >> "$LOGFILE" 2>&1
    local CODE=$?
    if [ $CODE -ne 0 ]; then
        log "!!! $MODULE erreur (code $CODE)"
        notify_fail "$MODULE" "$CODE"
    else
        log "<<< $MODULE OK"
    fi
    return $CODE
}

# ─── Modes d'exécution ────────────────────────────────────────────────────────

if [ "$1" = "--report-only" ]; then
    log "Mode : rapport quotidien"
    run_module "07_report.py"
    exit 0
fi

if [ "$1" = "--scrape-only" ]; then
    # Scraping + enrichissement (1x/jour)
    log "======== SCRAPING QUOTIDIEN ========"
    check_load
    check_ram
    run_module "01_crawler.py"      # Liste des consultations
    run_module "02_enricher.py"     # Détail + champs complémentaires
    run_module "02b_scrape_pv.py"   # PVs d'ouverture des plis (procédures 40/4/47)
    log "======== SCRAPING TERMINÉ ========"
    exit 0
fi

if [ "$1" = "--process-only" ]; then
    # Téléchargement + analyse + sync (toutes les 2h)
    # 02b_scrape_pv.py inclus ici car les PVs sont publiés plusieurs fois par jour
    log "======== TRAITEMENT ========"
    check_load
    check_ram
    run_module "02b_scrape_pv.py"   # PVs mis à jour plusieurs fois/jour
    run_module "03_downloader.py"
    run_module "04_analyzer.py"
    run_module "05_sync.py"

    # Nettoyage à 19h uniquement
    HOUR=$(date +%H)
    if [ "$HOUR" = "19" ]; then
        log ">>> Nettoyage quotidien"
        bash "$SCRIPT_DIR/06_cleanup.sh" >> "$LOGFILE" 2>&1
    fi
    log "======== TRAITEMENT TERMINÉ ========"
    exit 0
fi

# ─── Pipeline complet (fallback) ──────────────────────────────────────────────
log "======== PIPELINE COMPLET ========"
check_load
check_ram
run_module "01_crawler.py"
run_module "02_enricher.py"
run_module "02b_scrape_pv.py"   # PVs — ajouté ici aussi
run_module "03_downloader.py"
run_module "04_analyzer.py"
run_module "05_sync.py"
log "======== PIPELINE TERMINÉ ========"
exit 0
