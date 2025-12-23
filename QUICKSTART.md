# Guide de démarrage rapide

Ce guide vous aide à démarrer rapidement avec LOGPIPE-RELAY.

## Installation rapide

```bash
# 1. Aller dans le répertoire du projet
cd logspipe-relay

# 2. Créer un environnement virtuel (optionnel mais recommandé)
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate

# 3. Installer les dépendances
pip install -r requirements.txt
```

## Configuration minimale

1. **Créer le fichier `.env`** :

```bash
cd conf
cp env.example .env
```

2. **Éditer `.env`** avec vos chemins (sur Linux) :

```env
DATA_ROOT=/opt/logpipe-relay/data
STATE_DIR=/opt/logpipe-relay/state
LOG_DIR=/opt/logpipe-relay/logs
TMP_DIR=/opt/logpipe-relay/tmp
```

Ou sur Windows :

```env
DATA_ROOT=D:\logpipe-relay\data
STATE_DIR=D:\logpipe-relay\state
LOG_DIR=D:\logpipe-relay\logs
TMP_DIR=D:\logpipe-relay\tmp
```

3. **Configurer vos serveurs** :

```bash
cp sources.conf.example sources.conf
```

Éditez `sources.conf` :

```json
{
  "servers": [
    {
      "name": "mon-serveur",
      "host": "192.168.1.100",
      "user": "monuser",
      "remote_path": "/var/log/app/*.gz",
      "enabled": true
    }
  ]
}
```

## Test rapide

### Tester la configuration

```bash
# Depuis le répertoire logspipe-relay
python bin/run --help
```

### Traiter les fichiers déjà dans incoming/

Si vous avez déjà des fichiers `.gz` dans `data/incoming/`, vous pouvez les traiter :

```bash
python bin/run --no-incoming  # Pour ne pas essayer de collecter depuis les serveurs
```

### Collecter un fichier manuellement

```bash
python bin/collect mon-serveur /var/log/app/application.log.gz
```

### Extraire un fichier manuellement

```bash
python bin/extract data/incoming/mon-serveur/application.log.gz mon-serveur
```

## Prochaines étapes

1. Configurez SSH pour accès sans mot de passe aux serveurs sources
2. Testez avec un serveur avant de déployer en production
3. Consultez le README.md complet pour plus de détails

## Dépannage

### Erreur "rsync not found"

Installez rsync :
- Linux: `sudo apt-get install rsync` ou `sudo yum install rsync`
- Windows: Utilisez WSL ou installez via Chocolatey: `choco install rsync`

### Erreur de permissions SSH

Assurez-vous que :
- Vos clés SSH sont configurées : `ssh-copy-id user@server`
- Vous pouvez vous connecter : `ssh user@server`

### Fichiers non trouvés

Vérifiez que les chemins dans `sources.conf` sont corrects et que vous avez les permissions pour y accéder.

