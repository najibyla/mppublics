#!/bin/bash
# 06_cleanup.sh - Nettoyage des fichiers locaux > 7 jours
# Appelé par main.sh ou directement via cron

LOG_DIR="$(dirname "$0")/logs"
DOWNLOAD_DIR="$(dirname "$0")/downloads"
TEMP_DIR="$(dirname "$0")/temp_extract"
RETENTION_DAYS=7

mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/06_cleanup.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] Démarrage nettoyage (rétention: ${RETENTION_DAYS}j)" | tee -a "$LOGFILE"

# Supprimer les fichiers ZIP > 7 jours
if [ -d "$DOWNLOAD_DIR" ]; then
    COUNT=$(find "$DOWNLOAD_DIR" -type f -name "*.zip" -mtime +${RETENTION_DAYS} | wc -l)
    find "$DOWNLOAD_DIR" -type f -name "*.zip" -mtime +${RETENTION_DAYS} -delete
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] $COUNT ZIP supprimés" | tee -a "$LOGFILE"
fi

# Supprimer les dossiers temporaires > 7 jours
if [ -d "$TEMP_DIR" ]; then
    COUNT=$(find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -type d -mtime +${RETENTION_DAYS} | wc -l)
    find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -type d -mtime +${RETENTION_DAYS} -exec rm -rf {} +
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] $COUNT dossiers temp supprimés" | tee -a "$LOGFILE"
fi

# Rotation des logs > 30 jours
find "$LOG_DIR" -name "*.log" -mtime +30 -delete
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] Logs anciens supprimés" | tee -a "$LOGFILE"

# Nettoyage des résumés PDF > 30 jours
SUMMARIES_DIR="$(dirname "$0")/summaries"
if [ -d "$SUMMARIES_DIR" ]; then
    COUNT=$(find "$SUMMARIES_DIR" -type f -name "*.txt" -mtime +30 | wc -l)
    find "$SUMMARIES_DIR" -type f -name "*.txt" -mtime +30 -delete
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] $COUNT résumés supprimés" | tee -a "$LOGFILE"
fi

# Optimisation SQLite hebdomadaire (le dimanche)
DOW=$(date +%u)
if [ "$DOW" = "7" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] Optimisation SQLite..." | tee -a "$LOGFILE"
    python3 "$(dirname "$0")/db.py" --optimize >> "$LOGFILE" 2>&1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CLEANUP] Terminé" | tee -a "$LOGFILE"
