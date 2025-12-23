# LOGPIPE-RELAY

Pipeline de collecte et d'extraction de logs compressés multi-serveurs.

## Description

LOGPIPE-RELAY est un pipeline robuste pour collecter des fichiers de logs compressés (`.gz`) depuis plusieurs serveurs vers un serveur central, les extraire, et les organiser dans une structure claire. Le pipeline inclut :

- **Collecte avec retry automatique** : Utilise `rsync` pour copier les fichiers avec retry configurable
- **Extraction avec validation** : Extrait les fichiers gzip avec validation d'intégrité
- **Gestion d'erreurs complète** : Catégorise les erreurs (copy, extract, corruption) et les déplace vers des répertoires appropriés
- **Idempotence** : Utilise des checksums et un système d'état pour éviter les doublons
- **Configuration flexible** : Tout est configurable via fichiers `.env` et `.conf`
- **Logging structuré** : Logs JSON pour faciliter l'analyse

## Structure du projet

```
logspipe-relay/
├── src/                    # Code source Python
│   ├── __init__.py
│   ├── config.py          # Gestion configuration
│   ├── logger.py          # Configuration logging
│   ├── retry.py           # Mécanisme retry
│   ├── state.py           # Gestion état/idempotence
│   ├── collectors.py      # Collecte via rsync
│   ├── extractor.py       # Extraction gzip
│   └── pipeline.py        # Orchestrateur principal
├── bin/                   # Scripts exécutables
│   ├── run                # Orchestrateur principal
│   ├── collect            # Collecte manuelle
│   ├── extract            # Extraction manuelle
│   └── recover            # Récupération fichiers en erreur
├── conf/                  # Configuration
│   ├── .env.example       # Variables d'environnement
│   ├── sources.conf.example    # Configuration serveurs
│   └── pipeline.conf.example   # Configuration pipeline
├── requirements.txt
├── setup.py
└── README.md
```

## Installation

### Prérequis

- Python 3.7+
- `rsync` installé sur le système
- Accès SSH configuré vers les serveurs sources

### Installation du package

```bash
# Cloner le repository (ou télécharger les fichiers)
cd logspipe-relay

# Créer un environnement virtuel (recommandé)
python3 -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt
```

### Configuration

1. **Copier les fichiers de configuration exemple** :

```bash
cd conf
cp .env.example .env
cp sources.conf.example sources.conf
cp pipeline.conf.example pipeline.conf
```

2. **Configurer `.env`** :

Éditez `conf/.env` et ajustez les chemins et paramètres :

```env
DATA_ROOT=/opt/logpipe-relay/data
STATE_DIR=/opt/logpipe-relay/state
LOG_DIR=/opt/logpipe-relay/logs
TMP_DIR=/opt/logpipe-relay/tmp
MAX_RETRY_COPY=3
MAX_RETRY_EXTRACT=3
RETRY_DELAY_BASE=60
```

3. **Configurer `sources.conf`** :

Éditez `conf/sources.conf` et ajoutez vos serveurs :

```json
{
  "servers": [
    {
      "name": "srv1",
      "host": "srv1.example.com",
      "user": "loguser",
      "remote_path": "/var/log/apps/*.gz",
      "enabled": true
    }
  ]
}
```

4. **Configurer SSH** :

Assurez-vous que l'utilisateur qui exécute le pipeline a :
- Accès SSH sans mot de passe aux serveurs sources (clés SSH)
- Permissions pour lire les fichiers dans `remote_path` sur chaque serveur

## Usage

### Exécution complète du pipeline

```bash
# Depuis le répertoire logspipe-relay
python bin/run

# Options disponibles
python bin/run --config-dir /path/to/conf  # Spécifier répertoire config
python bin/run --no-incoming               # Ne pas traiter incoming/
python bin/run --sequential                # Traitement séquentiel
```

### Collecte manuelle d'un fichier

```bash
python bin/collect SERVER_NAME /remote/path/to/file.log.gz
```

### Extraction manuelle

```bash
python bin/extract /path/to/file.gz [SERVER_NAME]
```

### Récupération de fichiers en erreur

```bash
# Récupérer tous les fichiers en erreur
python bin/recover

# Récupérer seulement les erreurs d'extraction
python bin/recover --error-type extract

# Récupérer pour un serveur spécifique
python bin/recover --server srv1
```

## Structure des données

Le pipeline organise les fichiers dans la structure suivante :

```
DATA_ROOT/
├── incoming/              # Fichiers collectés (avant extraction)
│   ├── srv1/
│   ├── srv2/
│   └── srv3/
├── extracted/             # Fichiers extraits
│   ├── srv1/
│   ├── srv2/
│   └── srv3/
├── processed/             # Fichiers traités (si delete_source=False)
│   ├── srv1/
│   ├── srv2/
│   └── srv3/
└── error/                 # Fichiers en erreur
    ├── copy/              # Erreurs de copie
    │   ├── srv1/
    │   └── ...
    ├── extract/           # Erreurs d'extraction
    │   ├── srv1/
    │   └── ...
    └── quarantine/        # Fichiers corrompus
        ├── srv1/
        └── ...
```

## Gestion d'état et idempotence

Le pipeline utilise un système d'état pour garantir l'idempotence :

- Chaque fichier a un fichier d'état dans `STATE_DIR/`
- Les checksums SHA256 sont calculés et stockés
- Le statut de chaque fichier est suivi (pending, copied, extracted, processed, error)
- Les compteurs de retry sont maintenus par fichier

Cela permet de relancer le pipeline sans créer de doublons.

## Logging

Les logs sont écrits dans `LOG_DIR/logpipe-relay.log` avec rotation automatique.

Format JSON par défaut (configurable via `LOG_FORMAT` dans `.env`) :

```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "INFO",
  "logger": "logpipe_relay",
  "message": "File collected successfully",
  "server": "srv1",
  "file": "app.log.gz",
  "operation": "copy"
}
```

## Retry et backoff

Le pipeline implémente un mécanisme de retry avec backoff exponentiel :

- **Backoff exponentiel** : Délai augmente exponentiellement entre les tentatives
- **Jitter** : Ajout d'aléatoire pour éviter le thundering herd
- **Configurable** : Nombre de retries, délai de base, délai max via `.env`

## Utilisation d'un mot de passe SSH

L'authentification recommandée est par **clés SSH** (sans mot de passe).  
Cependant, si vous devez absolument utiliser un **mot de passe SSH**, le collecteur supporte `sshpass` via la variable d'environnement `SSH_PASSWORD`.

### Prérequis

- Installer `sshpass` :
  - Debian/Ubuntu : `sudo apt-get install sshpass`
  - CentOS/RHEL : `sudo yum install sshpass`
  - Windows : utiliser WSL avec `sshpass`, ou privilégier les clés SSH

### Configuration

1. Exporter le mot de passe dans l'environnement avant d'exécuter le pipeline :

```bash
export SSH_PASSWORD='monMotDePasseSsh'
python bin/run
```

2. Le code construira alors automatiquement une commande de ce type :

```bash
sshpass -p "$SSH_PASSWORD" rsync ...
```

⚠️ **Attention sécurité** :

- Ne commitez jamais votre mot de passe dans un fichier (`.env`, script, etc.).
- Préférez l'utilisation de clés SSH pour la production.
- Restreignez les droits de l'utilisateur qui exécute le pipeline.

## Limitations et améliorations futures

### Limitations actuelles

1. **Liste des fichiers distants** : La méthode `collect_all_from_server()` n'est pas complètement implémentée. Il faudrait :
   - Se connecter via SSH pour lister les fichiers correspondant au pattern `remote_path`
   - Utiliser une bibliothèque comme `paramiko` ou `fabric`

2. **Parallélisation** : La collecte depuis plusieurs serveurs peut être améliorée pour traiter plusieurs fichiers en parallèle par serveur

### Améliorations suggérées

- Support de formats de compression supplémentaires (bz2, xz)
- Intégration avec des systèmes de monitoring (Prometheus)
- Support de la compression des fichiers extraits
- Mécanisme de nettoyage automatique des fichiers anciens
- Support de la collecte incrémentale (uniquement nouveaux fichiers)
- Webhook/notifications en cas d'erreurs critiques

## Développement

### Structure des tests

```bash
# Installer les dépendances de développement
pip install -r requirements.txt[dev]

# Exécuter les tests
pytest tests/
```

### Formatage du code

```bash
black src/
flake8 src/
mypy src/
```

## Licence

MIT License (ou selon votre préférence)

## Auteur

Votre nom / Organisation
