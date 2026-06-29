# LOGPIPE-RELAY

Pipeline de collecte et d'extraction de logs compressés multi-serveurs.

## Description

LOGPIPE-RELAY collecte des fichiers de logs compressés (`.gz`) depuis plusieurs serveurs vers un serveur central, les extrait et les dépose dans un répertoire `inputs/` prêt pour l'ingestion downstream (ex. `middleware-abis-logs-ingestor`).

Fonctionnalités principales :

- **Collecte avec retry** : `rsync` + SSH, backoff exponentiel configurable
- **Extraction gzip** : validation d'intégrité, gestion des fichiers corrompus
- **Idempotence** : checksums SHA256 et fichiers d'état par fichier
- **Configuration simple** : un seul `ROOT_DIR`, arborescence créée automatiquement
- **Nettoyage automatique** : par âge (rétention) et par espace disque disponible
- **Logging structuré** : logs JSON avec rotation

## Structure du projet

```
logspipe-relay/
├── src/
│   ├── config.py          # Configuration (ROOT_DIR, dérivation des chemins)
│   ├── pipeline.py        # Orchestrateur principal
│   ├── collectors.py      # Collecte rsync / SSH
│   ├── extractor.py       # Extraction gzip → inputs/
│   ├── cleanup.py         # Nettoyage par âge et par disque
│   ├── disk.py            # Mesure de l'espace disque
│   ├── state.py           # État / idempotence
│   ├── retry.py           # Retry avec backoff
│   └── logger.py          # Logging JSON
├── bin/
│   ├── run                # Pipeline complet
│   ├── cleanup            # Nettoyage seul
│   ├── collect            # Collecte manuelle
│   ├── extract            # Extraction manuelle
│   └── recover            # Récupération fichiers en erreur
├── conf/
│   ├── env.example        # Variables d'environnement
│   ├── sources.conf.example
│   └── pipeline.conf.example
├── requirements.txt
└── setup.py
```

## Installation

### Prérequis

- Python 3.7+
- `rsync` et `ssh` disponibles sur le système
- Accès SSH (de préférence par clés) vers les serveurs sources

```bash
cd logspipe-relay
python -m venv venv
source venv/bin/activate   # Windows : venv\Scripts\activate
pip install -r requirements.txt
```

### Configuration

```bash
cd conf
cp env.example .env
cp sources.conf.example sources.conf
cp pipeline.conf.example pipeline.conf
```

#### `.env` — une seule variable de chemin

Seul `ROOT_DIR` est requis. Tous les sous-dossiers sont **créés automatiquement** au démarrage (permissions `755` sur Linux) :

```env
# Linux
ROOT_DIR=/opt/logpipe-relay

# Windows
# ROOT_DIR=D:\logpipe-relay
```

Arborescence générée :

```
ROOT_DIR/
├── inputs/          # Fichiers .log extraits (sortie vers l'ingestor)
├── data/            # Pipeline interne
│   ├── incoming/<serveur>/
│   ├── extracted/<serveur>/
│   ├── processed/<serveur>/
│   └── error/copy|extract|quarantine/<serveur>/
├── state/           # Fichiers d'état (idempotence)
├── logs/            # logpipe-relay.log
└── tmp/
```

Les autres variables dans `env.example` concernent le retry, rsync, l'extraction et le logging.

#### `sources.conf` — serveurs sources

```json
{
  "servers": [
    {
      "name": "brs1",
      "host": "brs1.example.com",
      "user": "loguser",
      "remote_path": "/var/log/abis/*.log.gz",
      "enabled": true
    }
  ]
}
```

La collecte liste les fichiers distants via SSH + `find` (un niveau de profondeur, pattern glob dans le nom de fichier).

#### `pipeline.conf` — parallélisation et nettoyage

```json
{
  "parallel_workers": 2,
  "cleanup_processed_after_days": 30,
  "cleanup_error_after_days": 90,
  "cleanup_inputs_after_days": 0,
  "cleanup_tmp_after_days": 7,
  "disk_space_threshold_gb": 10,
  "disk_space_target_gb": 15,
  "disk_cleanup_include_inputs": true
}
```

| Paramètre | Description |
|-----------|-------------|
| `cleanup_processed_after_days` | Rétention des `.gz` archivés dans `data/processed/` et `data/extracted/` |
| `cleanup_error_after_days` | Rétention des fichiers dans `data/error/` |
| `cleanup_inputs_after_days` | Rétention dans `inputs/` (`0` = désactivé) |
| `cleanup_tmp_after_days` | Rétention dans `tmp/` |
| `disk_space_threshold_gb` | Seuil d'espace libre minimal (Go) — déclenche le cleanup disque |
| `disk_space_target_gb` | Espace libre visé après cleanup disque (`0` = égal au seuil) |
| `disk_cleanup_include_inputs` | Autoriser la purge de `inputs/` en dernier recours sous pression disque |

Mettre une rétention à `0` désactive le nettoyage par âge pour cette cible.  
Mettre `disk_space_threshold_gb` à `0` désactive le nettoyage par disque.

## Usage

### Pipeline complet

```bash
python bin/run
python bin/run --config-dir /path/to/conf
python bin/run --no-incoming      # Ignorer incoming/, collecter uniquement depuis les serveurs
python bin/run --sequential       # Traiter les serveurs un par un
python bin/run --no-cleanup       # Sans nettoyage
```

Flux d'exécution :

1. **Cleanup disque** (si espace libre < seuil) — supprime les fichiers les plus anciens
2. Collecte (`rsync`) depuis les serveurs configurés
3. Extraction des `.gz`
4. Déplacement des `.log` extraits vers `ROOT_DIR/inputs/`
5. **Cleanup par âge**, puis **cleanup disque** si nécessaire

### Nettoyage seul

```bash
python bin/cleanup
python bin/cleanup --config-dir /path/to/conf
```

### Commandes unitaires

```bash
python bin/collect SERVER_NAME /remote/path/to/file.log.gz
python bin/extract /path/to/file.gz [SERVER_NAME]
python bin/recover [--error-type extract] [--server brs1]
```

## Intégration avec l'ingestor ABIS

Pour alimenter directement `middleware-abis-logs-ingestor`, pointez `ROOT_DIR` vers le dossier `storage` de l'ingestor :

```env
ROOT_DIR=/chemin/vers/middleware-abis-logs-ingestor/storage
```

Les fichiers extraits seront déposés dans `storage/inputs/`.  
L'ingestor lit les `.log` depuis `storage/inputs/processing_data/` en mode batch — adapter l'un ou l'autre côté si besoin.

## Gestion d'état et idempotence

- Un fichier JSON par fichier traité dans `ROOT_DIR/state/` (clé = hash `serveur:filename`)
- Statuts : `pending` → `copied` → `extracted` → `processed` (ou `error`)
- Checksums SHA256 pour détecter les copies déjà valides
- Lors d'un cleanup, l'état associé est supprimé pour permettre une re-collecte

## Nettoyage

### Par âge

Supprime les fichiers dont la date de modification dépasse la rétention configurée dans `pipeline.conf`.

### Par espace disque

Si l'espace libre sur `ROOT_DIR` descend sous `disk_space_threshold_gb`, les fichiers les plus anciens sont supprimés par ordre de priorité :

1. `tmp/`
2. `data/processed/`
3. `data/extracted/`
4. `data/error/copy`, `extract`, `quarantine`
5. `inputs/` (si `disk_cleanup_include_inputs` est `true`)

Le cleanup s'arrête lorsque l'espace libre atteint `disk_space_target_gb`.

## Logging

Fichier : `ROOT_DIR/logs/logpipe-relay.log` (rotation configurable via `.env`).

```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "INFO",
  "logger": "logpipe_relay",
  "message": "File collected successfully",
  "server": "brs1",
  "file": "auditlog-2025-11-10_07.0.log.gz",
  "operation": "copy"
}
```

## Authentification SSH

**Recommandé** : clés SSH sans mot de passe.

En alternative (non recommandé en production), le mot de passe peut être passé via la variable d'environnement `SSH_PASSWORD` (nécessite `sshpass`) :

```bash
export SSH_PASSWORD='monMotDePasseSsh'
python bin/run
```

Ne jamais commiter de mot de passe dans `.env` ou dans le dépôt.

## Limitations connues

- `find -maxdepth 1` : seuls les fichiers directement dans le répertoire du `remote_path` sont listés
- Pas de collecte parallèle de plusieurs fichiers sur un même serveur (parallélisation inter-serveurs uniquement)
- Form requirement `rsync` + `ssh` (sur Windows, utiliser WSL ou un port rsync)

## Développement

```bash
pip install -e ".[dev]"
black src/
flake8 src/
```

## Licence

MIT License
