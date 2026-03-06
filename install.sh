#!/bin/bash
# =============================================================================
# install.sh - Installation automatique du pipeline ETL Marchés Publics
# Compatible : Ubuntu 20.04 / 22.04 / 24.04
# Usage : chmod +x install.sh && ./install.sh
# =============================================================================

set -e  # Arrêt sur erreur

# ─── Couleurs ─────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

log()     { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Pipeline ETL — Marchés Publics Marocains           ║${NC}"
echo -e "${BLUE}║   Installation automatique Ubuntu                    ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# ─── Vérification OS ──────────────────────────────────────────────────────────
if ! grep -qi "ubuntu" /etc/os-release 2>/dev/null; then
    warn "Ce script est optimisé pour Ubuntu. Continuer quand même ? (o/N)"
    read -r answer
    [[ "$answer" =~ ^[oO]$ ]] || exit 0
fi

# ─── Vérification droits sudo ─────────────────────────────────────────────────
if ! sudo -n true 2>/dev/null; then
    log "Droits sudo nécessaires. Entrez votre mot de passe si demandé."
fi

# ─── 1. Mise à jour système ───────────────────────────────────────────────────
log "Mise à jour des paquets système..."
sudo apt update -qq
success "APT à jour"

# ─── 2. Dépendances système ───────────────────────────────────────────────────
log "Installation des dépendances système..."
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    tesseract-ocr \
    tesseract-ocr-fra \
    tesseract-ocr-ara \
    poppler-utils \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    curl \
    wget \
    unzip \
    git \
    build-essential \
    2>/dev/null

success "Dépendances système installées"

# ─── 3. Vérification Python ───────────────────────────────────────────────────
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 10 ]; }; then
    error "Python 3.10+ requis (trouvé : $PYTHON_VERSION)"
fi
success "Python $PYTHON_VERSION détecté"

# ─── 4. Environnement virtuel Python ─────────────────────────────────────────
log "Création de l'environnement virtuel Python..."
cd "$SCRIPT_DIR"

if [ ! -d "venv" ]; then
    python3 -m venv venv
    success "Environnement virtuel créé"
else
    success "Environnement virtuel existant réutilisé"
fi

# Activer le venv
source "$SCRIPT_DIR/venv/bin/activate"
pip install --upgrade pip -q
success "pip mis à jour"

# ─── 5. Dépendances Python ────────────────────────────────────────────────────
log "Installation des dépendances Python..."
pip install -r "$SCRIPT_DIR/requirements.txt" -q
success "Dépendances Python installées"

# ─── 6. Playwright + Chromium ─────────────────────────────────────────────────
log "Installation de Playwright et Chromium..."
playwright install chromium 2>/dev/null
playwright install-deps chromium 2>/dev/null || \
    sudo "$SCRIPT_DIR/venv/bin/python" -m playwright install-deps chromium 2>/dev/null || \
    warn "playwright install-deps a échoué — peut nécessiter sudo manuel"
success "Playwright + Chromium installés"

# ─── 7. NLTK data (pour sumy) ─────────────────────────────────────────────────
log "Téléchargement des données NLTK..."
python3 -c "
import nltk
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)
" 2>/dev/null || warn "NLTK data partiellement téléchargé"
success "Données NLTK prêtes"

# ─── 8. Ollama + Mistral ──────────────────────────────────────────────────────
log "Vérification d'Ollama..."
if command -v ollama &>/dev/null; then
    success "Ollama déjà installé ($(ollama --version 2>/dev/null || echo 'version inconnue'))"
else
    log "Installation d'Ollama..."
    curl -fsSL https://ollama.com/install.sh | sh
    success "Ollama installé"
fi

# Démarrer Ollama en arrière-plan si pas en cours
if ! pgrep -x "ollama" > /dev/null 2>&1; then
    log "Démarrage du service Ollama..."
    ollama serve &>/dev/null &
    sleep 3
fi

# Télécharger Mistral (avec choix si RAM limitée)
TOTAL_RAM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
log "RAM détectée : ${TOTAL_RAM_GB} Go"

if [ "$TOTAL_RAM_GB" -ge 22 ]; then
    OLLAMA_MODEL="mixtral:8x7b"
    log "RAM ≥ 22Go détectée — modèle premium : mixtral:8x7b (meilleure qualité)"
elif [ "$TOTAL_RAM_GB" -ge 8 ]; then
    OLLAMA_MODEL="mistral"
    log "RAM ≥ 8Go — modèle : mistral"
elif [ "$TOTAL_RAM_GB" -ge 5 ]; then
    OLLAMA_MODEL="gemma2"
    warn "RAM < 8Go — utilisation de gemma2"
else
    OLLAMA_MODEL="phi3"
    warn "RAM < 5Go — utilisation de phi3 (modèle léger)"
fi

log "Téléchargement du modèle Ollama : $OLLAMA_MODEL (~quelques Go, patience...)..."
ollama pull "$OLLAMA_MODEL" 2>/dev/null && success "Modèle $OLLAMA_MODEL prêt" || \
    warn "Téléchargement Ollama échoué — l'IA sera désactivée (non bloquant)"

# Mettre à jour le modèle dans 04_analyzer.py
sed -i "s/OLLAMA_MODEL   = \"mistral\"/OLLAMA_MODEL   = \"$OLLAMA_MODEL\"/" \
    "$SCRIPT_DIR/04_analyzer.py" 2>/dev/null || true

# ─── 9. Initialisation base de données ───────────────────────────────────────
log "Initialisation de la base SQLite..."
python3 "$SCRIPT_DIR/db.py"
success "Base tenders.db initialisée"

# ─── 10. Fichier .env ─────────────────────────────────────────────────────────
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
    warn ".env créé depuis .env.example — à compléter avant utilisation !"
else
    success ".env existant conservé"
fi

# ─── 11. Permissions ──────────────────────────────────────────────────────────
chmod +x "$SCRIPT_DIR/main.sh"
chmod +x "$SCRIPT_DIR/06_cleanup.sh"
mkdir -p "$SCRIPT_DIR/logs" "$SCRIPT_DIR/downloads" \
         "$SCRIPT_DIR/summaries" "$SCRIPT_DIR/temp_extract"
success "Permissions et dossiers configurés"

# ─── 12. Tesseract — vérification langues ─────────────────────────────────────
log "Vérification Tesseract..."
LANGS=$(tesseract --list-langs 2>/dev/null | tr '\n' ' ')
if echo "$LANGS" | grep -q "fra"; then
    success "Tesseract : français OK"
else
    warn "Tesseract : langue française manquante — sudo apt install tesseract-ocr-fra"
fi
if echo "$LANGS" | grep -q "ara"; then
    success "Tesseract : arabe OK"
else
    warn "Tesseract : langue arabe manquante — sudo apt install tesseract-ocr-ara"
fi

# ─── 13. Résumé final ─────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              ✅  Installation terminée !              ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BLUE}Répertoire${NC} : $SCRIPT_DIR"
echo -e "  ${BLUE}Python venv${NC} : $SCRIPT_DIR/venv"
echo -e "  ${BLUE}Modèle IA${NC}   : $OLLAMA_MODEL"
echo -e "  ${BLUE}Base SQLite${NC} : $SCRIPT_DIR/tenders.db"
echo ""
echo -e "${YELLOW}📋 Prochaines étapes :${NC}"
echo ""
echo -e "  1️⃣  Compléter la configuration :"
echo -e "      ${BLUE}nano $SCRIPT_DIR/.env${NC}"
echo ""
echo -e "  2️⃣  Configurer Google API (token.json) :"
echo -e "      Voir README.md → section 'Google API'"
echo ""
echo -e "  3️⃣  Tester le pipeline :"
echo -e "      ${BLUE}source $SCRIPT_DIR/venv/bin/activate${NC}"
echo -e "      ${BLUE}python3 $SCRIPT_DIR/01_crawler.py${NC}"
echo ""
echo -e "  4️⃣  Configurer le cron :"
echo -e "      ${BLUE}crontab -e${NC}"
echo -e "      Ajouter :"
echo -e "      ${BLUE}# Scraping 1x/jour à 7h"
echo -e "      0 7 * * * $SCRIPT_DIR/main.sh --scrape-only"
echo -e "      # Traitement toutes les 2h"
echo -e "      0 9,11,13,15,17,19 * * * $SCRIPT_DIR/main.sh --process-only"
echo -e "      # Rapport quotidien à 8h"
echo -e "      0 8 * * * $SCRIPT_DIR/main.sh --report-only${NC}"
echo ""
echo -e "  5️⃣  Consulter les logs :"
echo -e "      ${BLUE}tail -f $SCRIPT_DIR/logs/main.log${NC}"
echo ""
echo -e "  📖 Documentation complète : ${BLUE}$SCRIPT_DIR/README.md${NC}"
echo ""
