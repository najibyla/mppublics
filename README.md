# 🏛️ Pipeline ETL — Marchés Publics Marocains

Système automatisé de scraping, analyse et synchronisation des appels d'offres
du portail [marchespublics.gov.ma](https://www.marchespublics.gov.ma).

---

## 📋 Table des matières

1. [Architecture](#architecture)
2. [Prérequis](#prérequis)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Déploiement rapide](#déploiement-rapide)
6. [Utilisation](#utilisation)
7. [Structure des données](#structure-des-données)
8. [Modules détaillés](#modules-détaillés)
9. [Extraction PDF](#extraction-pdf)
10. [Google Sheets](#google-sheets)
11. [Cron & Automatisation](#cron--automatisation)
12. [Dépannage](#dépannage)

---

## Architecture

```
marchespublics/
├── main.sh              ← Orchestrateur (lancé par cron)
├── install.sh           ← Installation automatique Ubuntu
├── db.py                ← Base SQLite (tenders.db)
├── utils.py             ← Retry, hash, normalisation (partagé)
├── notifier.py          ← Notifications Telegram + pipeline_guard
├── 01_crawler.py        ← Scraping liste des AO
├── 02_enricher.py       ← Enrichissement détail + attributions
├── 02b_scrape_pv.py     ← Scraping PVs d'ouverture des plis
├── 03_downloader.py     ← Téléchargement ZIP des DCE (Playwright + Requests)
├── 04_analyzer.py       ← Extraction PDF à 4 niveaux + OCR + IA locale
├── 05_sync.py           ← Synchronisation Google Sheets (gspread)
├── 06_cleanup.sh        ← Nettoyage fichiers > 7 jours
├── 07_report.py         ← Rapport quotidien par email
├── requirements.txt     ← Dépendances Python
├── .env.example         ← Variables d'environnement (modèle)
└── README.md            ← Ce fichier
```

### Machine à états (SQLite)

```
TO_ENRICH → TO_DOWNLOAD → TO_ANALYZE → TO_SYNC → DONE
    ↓             ↓             ↓           ↓
ERROR_ENRICH  ERROR_DOWNLOAD  ERROR_ANALYZE  ERROR_SYNC
```

Chaque module traite uniquement les lignes au statut précédent.
En cas d'erreur, `retry_count` est incrémenté (max 3 tentatives).

### Déduplication

| Clé | Source | Usage |
|---|---|---|
| `url_id` | Paramètre `id=` extrait de l'URL détail | Clé stable et unique par consultation (disponible dès le crawler) |
| `tender_hash` | SHA256(reference \| date_pub \| acheteur) | Identifie les offres avec même référence mais acheteurs différents |
| `hash_pdf` | MD5(pdf_url) dans table `pvs` | Évite les PVs dupliqués |

---

## Prérequis

| Élément | Version minimale |
|---|---|
| Ubuntu | 20.04 LTS |
| Python | 3.10+ |
| RAM | 4 Go (8 Go recommandé pour Ollama) |
| Disque | 10 Go libres (Ollama ~4 Go) |
| Connexion | Requise pour le scraping |

---

## Installation

### Installation automatique (recommandée)

```bash
git clone <repo> ~/marchespublics
cd ~/marchespublics
chmod +x install.sh
./install.sh
```

`install.sh` installe automatiquement :
- Dépendances système (Tesseract, Poppler, libgl...)
- Python 3 + venv
- Playwright + Chromium intégré
- Ollama + modèle IA adapté à la RAM disponible
- Toutes les dépendances Python (`requirements.txt`)
- Base SQLite initialisée

### Installation manuelle

```bash
# 1. Dépendances système
sudo apt update && sudo apt install -y \
    python3 python3-pip python3-venv \
    tesseract-ocr tesseract-ocr-fra tesseract-ocr-ara \
    poppler-utils libgl1 curl wget

# 2. Environnement Python
python3 -m venv venv
source venv/bin/activate

# 3. Dépendances Python
pip install -r requirements.txt

# 4. Playwright (télécharge son propre Chromium)
playwright install chromium
playwright install-deps chromium

# 5. Ollama + Mistral (IA locale — optionnel)
curl -fsSL https://ollama.com/install.sh | sh
ollama pull mistral

# 6. Base de données
python3 db.py

# 7. Configuration
cp .env.example .env
nano .env
```

---

## Configuration

### Fichier `.env`

```bash
# ── Google Sheets ──────────────────────────────────────────────
# ID dans l'URL : https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/
SPREADSHEET_ID=ton_spreadsheet_id
SHEET_NAME=Marchés Publics Maroc

# ── Google Drive ───────────────────────────────────────────────
# ID du dossier racine pour les DCE
DRIVE_FOLDER_ID=ton_drive_folder_id

# ── Email SMTP (Gmail App Password) ───────────────────────────
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=portailmpma@gmail.com
SMTP_PASSWORD=ton_app_password_gmail
REPORT_EMAIL=portailmpma@gmail.com

# ── Notifications Telegram (optionnel) ────────────────────────
ENABLE_TELEGRAM=true
TELEGRAM_TOKEN=ton_bot_token
TELEGRAM_CHAT_ID=ton_chat_id

# ── Pipeline ───────────────────────────────────────────────────
BATCH_SIZE=100
MAX_RETRY=3
BUDGET_SEUIL=70000000    # Alerte si budget > 70M DH
JOURS_SEUIL=30           # Et jours restants > 30

# ── IA Locale ─────────────────────────────────────────────────
OLLAMA_MODEL=mistral
OLLAMA_ENABLED=true

# ── DCE (identité pour formulaire téléchargement) ─────────────
DCE_NOM=VEILLE
DCE_PRENOM=AUTOMATISEE
DCE_EMAIL=portailmpma@gmail.com
```

### Google API — Obtenir `token.json`

1. [console.cloud.google.com](https://console.cloud.google.com) → Créer un projet
2. Activer **Google Sheets API** et **Google Drive API**
3. Identifiants → OAuth 2.0 → Application de bureau → Télécharger `credentials.json`
4. Générer le token :

```bash
source venv/bin/activate
python3 - << 'EOF'
from google_auth_oauthlib.flow import InstalledAppFlow
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
creds = flow.run_local_server(port=0)
with open('token.json', 'w') as f:
    f.write(creds.to_json())
print("token.json créé !")
EOF
```

### Gmail — App Password

1. Compte Google → Sécurité → Validation en 2 étapes (activer)
2. Sécurité → Mots de passe des applications → Générer pour "Mail / Linux"
3. Utiliser ce mot de passe dans `SMTP_PASSWORD`

---

## Déploiement rapide

### Checklist avant premier lancement

```bash
# 1. Copier le projet sur le serveur
scp -r marchespublics/ user@serveur:~/

# 2. Installer
cd ~/marchespublics
chmod +x install.sh && ./install.sh

# 3. Configurer l'environnement
cp .env.example .env
nano .env              # SPREADSHEET_ID, TELEGRAM_TOKEN, etc.

# 4. Placer token.json Google
# (généré depuis votre machine locale, voir section Configuration)
cp /chemin/token.json ~/marchespublics/token.json

# 5. Activer le venv
source venv/bin/activate

# 6. Initialiser la base
python3 db.py
```

### Tester module par module

```bash
source venv/bin/activate

# Étape 1 — Scraper la liste des AO
python3 01_crawler.py
tail -f logs/01_crawler.log

# Étape 2 — Enrichir les détails
python3 02_enricher.py
tail -f logs/02_enricher.log

# Étape 3 — Scraper les PVs (peut être lancé indépendamment)
python3 02b_scrape_pv.py
tail -f logs/02b_scrape_pv.log

# Étape 4 — Télécharger les DCE
python3 03_downloader.py

# Étape 5 — Analyser les PDF
python3 04_analyzer.py

# Étape 6 — Synchroniser vers Google Sheets (crée les 2 onglets)
python3 05_sync.py

# Rapport
python3 07_report.py
```

### Vérifier l'état de la base

```bash
python3 db.py
# Affiche les stats : total, statuts, taille, PVs, etc.
```

### Configurer le cron

```bash
crontab -e
```

```cron
# Scraping + enrichissement : 1x/jour à 7h
0 7 * * * /home/user/marchespublics/main.sh --scrape-only

# PVs + téléchargement + analyse + sync : toutes les 2h de 9h à 19h
0 9,11,13,15,17,19 * * * /home/user/marchespublics/main.sh --process-only

# Rapport quotidien : 8h chaque matin
0 8 * * * /home/user/marchespublics/main.sh --report-only
```

---

## Utilisation

### Modes de `main.sh`

| Commande | Modules lancés | Fréquence recommandée |
|---|---|---|
| `main.sh --scrape-only` | 01 → 02 → 02b | 1x/jour (7h) |
| `main.sh --process-only` | 02b → 03 → 04 → 05 | Toutes les 2h |
| `main.sh --report-only` | 07 | 1x/jour (8h) |
| `main.sh` | Pipeline complet | Manuel / test |

> **Note :** `02b_scrape_pv.py` est inclus dans `--process-only` car les PVs
> peuvent être publiés plusieurs fois par jour pour la même consultation.

### Réinitialiser les erreurs

```bash
# Via Python
python3 -c "from db import reset_errors; reset_errors()"

# Ou manuellement par module
python3 -c "
import sqlite3
conn = sqlite3.connect('tenders.db')
conn.execute(\"UPDATE tenders SET status='TO_ENRICH', retry_count=0 WHERE status='ERROR_ENRICH'\")
conn.commit(); print('Reset OK')
"
```

### Consulter les logs

```bash
tail -f logs/main.log           # Log orchestrateur
tail -f logs/01_crawler.log     # Scraping
tail -f logs/02_enricher.log    # Enrichissement
tail -f logs/05_sync.log        # Google Sheets
tail -f logs/*.log              # Tous en temps réel
```

---

## Structure des données

### Table `tenders`

| Colonne | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Clé primaire auto |
| `url_id` | TEXT UNIQUE | ID numérique extrait de l'URL (clé de dédup principale) |
| `reference` | TEXT | Référence de l'AO (non unique — même ref possible pour différents acheteurs) |
| `date_publication` | TEXT | Date de publication (JJ/MM/AAAA) |
| `date_limite` | TEXT | Date limite de remise |
| `lien_detail` | TEXT | URL page détail |
| `objet` | TEXT | Intitulé de l'appel d'offres |
| `acheteur` | TEXT | Nom de l'acheteur public |
| `estimation` | REAL | Budget estimé en DH |
| `domaines` | TEXT | Domaines d'activité |
| `contact_nom` | TEXT | Responsable |
| `contact_email` | TEXT | Email |
| `contact_tel` | TEXT | Téléphone normalisé |
| `contact_fax` | TEXT | Fax normalisé |
| `lien_dce` | TEXT | URL formulaire téléchargement DCE |
| `attributaire` | TEXT | Entreprise attributaire |
| `montant_reel` | REAL | Montant final attribué (DH) |
| `date_attribution` | TEXT | Date d'attribution |
| `nb_soumissionnaires` | INTEGER | Nombre de candidats |
| `zip_path` | TEXT | Chemin local du ZIP |
| `resume_txt` | TEXT | Chemin du résumé PDF |
| `drive_folder_url` | TEXT | URL dossier Google Drive |
| `tender_hash` | TEXT | SHA256(ref\|date_pub\|acheteur) — calculé après enrichissement |
| `hash_content` | TEXT | MD5 contenu page (détecte les mises à jour) |
| `hash_dce` | TEXT | MD5 du DCE (détecte si le dossier est mis à jour) |
| `last_seen` | DATETIME | Dernière détection en ligne |
| `retry_count` | INTEGER | Tentatives échouées (max 3) |
| `last_error` | TEXT | Dernier message d'erreur |
| `status` | TEXT | État dans la machine à états |

### Table `pvs` (1 tender → N PVs)

| Colonne | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Clé primaire |
| `tender_id` | INTEGER FK | Référence vers `tenders.id` |
| `reference` | TEXT | Référence (copie pour recherche rapide) |
| `pdf_url` | TEXT | URL du PDF du PV |
| `procedure_type` | TEXT | Type (Concours Arch, Phase 1, Phase 2) |
| `date_publication` | TEXT | Date de publication du PV |
| `hash_pdf` | TEXT UNIQUE | MD5(pdf_url) — déduplication |
| `synced_to_sheets` | INTEGER | 0 = non synchronisé, 1 = synchronisé |

---

## Modules détaillés

### `01_crawler.py` — Scraping liste

- Filtre : Catégorie = **Services** (codes 40, 4, 47)
- Pagination automatique (500 résultats/page)
- Lecture des panneaux cachés via `text_content()` (anti-WAF)
- `url_id` extrait du paramètre `id=` de l'URL pour la déduplication
- Détection des mises à jour via hash MD5 du contenu

### `02_enricher.py` — Enrichissement

- Déplie les panneaux cachés (accordéons, toggles)
- Extrait : Objet, Acheteur, Estimation, Domaines, Contact complet
- Champs supplémentaires : Caution, Qualifications, Agréments, Lieux
- Récupère l'URL du DCE
- Scrape les résultats d'attribution (attributaire, montant réel, nb soumissionnaires)
- Calcule `tender_hash` = SHA256(reference | date_pub | acheteur)
- ⚠️ **Ne scrape pas les PVs** → c'est le rôle de `02b_scrape_pv.py`

### `02b_scrape_pv.py` — Scraping PVs

- Codes procédure : `40` (Concours Arch), `4` (Phase 1), `47` (Phase 2)
- Insertion dans la **table `pvs`** (1 tender → N PVs)
- Déduplication par `hash_pdf` = MD5(pdf_url)
- Crée des tenders orphelins si le PV arrive avant le crawler
- Lancé à chaque cycle `--process-only` (PVs publiés plusieurs fois/jour)

### `03_downloader.py` — Téléchargement DCE

- Stratégie hybride **Playwright + Requests** :
  1. Playwright remplit le formulaire et établit la session
  2. Requests télécharge le fichier en réutilisant les cookies
- Remplit : Nom=VEILLE, Prénom=AUTOMATISEE, Email=portailmpma@gmail.com, CGU=✓
- Vérification intégrité du ZIP (`zipfile.testzip()`)
- Nommage : `downloads/DCE_{reference}.zip`

### `04_analyzer.py` — Analyse PDF

Voir section [Extraction PDF](#extraction-pdf).

### `05_sync.py` — Synchronisation Google Sheets

- Utilise `gspread` (client léger, plus fiable que l'API REST directe)
- **Onglet 1 "Marchés Publics"** : toutes les consultations
- **Onglet 2 "PVs"** : tous les PVs, mis à jour à chaque cycle
- Fallback `TO_ANALYZE` si aucun `TO_SYNC` (évite les blocages)
- Alerte email si budget > 70M DH ET jours restants > 30

### `06_cleanup.sh` — Nettoyage

- ZIP téléchargés > 7 jours → supprimés
- Dossiers temporaires > 7 jours → supprimés
- Résumés PDF > 30 jours → supprimés
- Logs > 30 jours → supprimés
- Optimisation SQLite (VACUUM + ANALYZE) chaque dimanche

### `07_report.py` — Rapport quotidien

- Nouvelles offres du jour + nouveaux PVs
- Offres prioritaires (budget > 70M DH)
- Offres expirant dans les 7 prochains jours
- Bilan des statuts et erreurs pipeline

---

## Extraction PDF

Chaîne à 4 niveaux, du plus rapide au plus puissant :

```
Niveau 1 : PyMuPDF       → Texte natif           (< 1s/page)   ← toujours exécuté
     +
Niveau 2 : pdfplumber    → Tableaux structurés    (< 2s/page)   ← toujours exécuté
     ↓ si texte < 100 chars
Niveau 3 : Tesseract OCR → Scans / images         (5-30s/page)
     ↓ si montant non trouvé
Niveau 4 : Ollama/Mistral → Extraction IA locale  (5-15s/doc)
```

### Modèles Ollama selon la RAM disponible

| Modèle | Taille | RAM requise | Qualité FR |
|---|---|---|---|
| `mixtral:8x7b` | 26 Go | 24 Go | ⭐⭐⭐⭐⭐ |
| `mistral` | 4.1 Go | 6 Go | ⭐⭐⭐⭐⭐ |
| `gemma2` | 5.4 Go | 8 Go | ⭐⭐⭐⭐ |
| `phi3` | 2.3 Go | 4 Go | ⭐⭐⭐ |

> `install.sh` sélectionne automatiquement le meilleur modèle selon la RAM.

Modifier dans `.env` :
```bash
OLLAMA_MODEL=phi3      # Modèle léger
OLLAMA_ENABLED=false   # Désactiver complètement le niveau 4
```

---

## Google Sheets

### Structure des onglets

**Onglet 1 — "Marchés Publics"** (tenders)

```
Référence | Date Publication | Date Limite | Objet | Acheteur |
Estimation (DH) | Domaines | Contact Nom | Contact Email | Contact Tél | Contact Fax |
Attributaire | Montant Réel (DH) | Date Attribution | Nb Soumissionnaires |
Jours Restants | STATUS
```

**Onglet 2 — "PVs"** (procès-verbaux)

```
Référence | Procédure | Date Publication | Lien PDF | Synchronisé
```

### Colonne STATUS

| Valeur | Critères |
|---|---|
| `OUI` | Budget > 70M DH **ET** Jours restants > 30 → alerte email envoyée |
| `NON` | Ne répond pas aux deux critères |

### Formule dynamique Jours Restants (optionnelle)

Si tu veux que la colonne se mette à jour automatiquement :
```
=SI(C2<AUJOURDHUI();"Expiré";ENT(C2-AUJOURDHUI()))
```
(colonne C = Date Limite au format date Google Sheets)

---

## Cron & Automatisation

### Crontab complète recommandée

```bash
crontab -e
```

```cron
# Scraping + enrichissement : 1x/jour à 7h (heure creuse)
0 7 * * * /home/user/marchespublics/main.sh --scrape-only

# PVs + téléchargement + analyse + sync : toutes les 2h
0 9,11,13,15,17,19 * * * /home/user/marchespublics/main.sh --process-only

# Rapport quotidien : 8h chaque matin
0 8 * * * /home/user/marchespublics/main.sh --report-only
```

> Remplacer `/home/user/` par le chemin réel du projet sur le serveur.

---

## Dépannage

### Playwright bloqué (WAF / timeout)

```python
# Augmenter les délais dans 01_crawler.py
await random_page_delay(page, min_ms=5000, max_ms=10000)
```

### Tesseract — langue française manquante

```bash
tesseract --list-langs   # vérifier
sudo apt install tesseract-ocr-fra tesseract-ocr-ara
```

### Ollama — erreur mémoire

```bash
systemctl status ollama     # vérifier que le service tourne
# Passer à un modèle plus léger dans .env :
OLLAMA_MODEL=phi3
```

### Google token expiré

```bash
rm token.json
# Relancer la génération (voir section Configuration → Google API)
```

### Pipeline bloqué en erreur

```bash
# Reset automatique de toutes les erreurs
python3 -c "from db import reset_errors; reset_errors()"

# Stats de la base
python3 db.py
```

### Vérifier un module en isolation

```bash
source venv/bin/activate
python3 -c "
from db import get_conn, get_stats
stats = get_stats()
for k, v in stats.items():
    print(f'{k}: {v}')
"
```

---

## Licence

Usage interne — Portail MPMA.
