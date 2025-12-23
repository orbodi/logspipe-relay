"""Module de collecte des fichiers depuis les serveurs sources."""
import os
import subprocess
import shutil
import shlex
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass

from .config import Config, ServerConfig, RsyncConfig
from .retry import RetryableOperation
from .state import StateManager
from .logger import get_logger

logger = get_logger()


class CopyError(Exception):
    """Exception pour erreurs de copie."""
    pass


class Collector:
    """Collecteur de fichiers depuis les serveurs sources."""
    
    def __init__(self, config: Config, state_manager: StateManager):
        """
        Initialise le collecteur.
        
        Args:
            config: Configuration du pipeline.
            state_manager: Gestionnaire d'état.
        """
        self.config = config
        self.state_manager = state_manager
        self.retry_operation = RetryableOperation(
            max_retries=config.retry.max_retry_copy,
            config=config.retry,
            operation_name="copy",
        )
    
    def _check_rsync_available(self) -> bool:
        """
        Vérifie si rsync est disponible.
        
        Returns:
            True si rsync est disponible.
        """
        return shutil.which("rsync") is not None

    def _build_ssh_base_cmd(self) -> List[str]:
        """
        Construit la base de commande SSH, en tenant compte éventuellement
        d'un mot de passe fourni via SSH_PASSWORD (sshpass).

        Returns:
            Liste de base pour exécuter ssh/sshpass.
        """
        ssh_password = os.getenv("SSH_PASSWORD")
        base_cmd: List[str] = []
        if ssh_password:
            base_cmd = ["sshpass", "-p", ssh_password]
        return base_cmd + ["ssh"]
    
    def _build_rsync_command(
        self,
        server: ServerConfig,
        remote_path: str,
        local_dest: Path,
    ) -> List[str]:
        """
        Construit la commande rsync.
        
        Args:
            server: Configuration du serveur.
            remote_path: Chemin distant du fichier.
            local_dest: Destination locale.
        
        Returns:
            Liste des arguments de la commande rsync.
        """
        # Construire le chemin distant avec user@host
        remote = f"{server.user}@{server.host}:{remote_path}"
        
        # Options rsync
        options = self.config.rsync.options.split()

        # Support d'un mot de passe SSH via la variable d'environnement SSH_PASSWORD.
        # Attention: ceci nécessite l'outil `sshpass` et n'est pas recommandé en production,
        # préférez l'authentification par clés SSH quand c'est possible.
        ssh_password = os.getenv("SSH_PASSWORD")

        base_cmd: List[str] = []
        if ssh_password:
            # Préfixer la commande rsync avec sshpass
            base_cmd = ["sshpass", "-p", ssh_password]

        # Construire la commande complète
        cmd = base_cmd + ["rsync"] + options + [
            "--timeout", str(self.config.rsync.timeout),
            remote,
            str(local_dest),
        ]
        
        return cmd
    
    def _copy_file(
        self,
        server: ServerConfig,
        remote_path: str,
        local_dest: Path,
    ) -> Path:
        """
        Copie un fichier depuis un serveur distant avec rsync.
        
        Args:
            server: Configuration du serveur.
            remote_path: Chemin distant du fichier.
            local_dest: Répertoire de destination locale.
        
        Returns:
            Chemin du fichier copié localement.
        
        Raises:
            CopyError: Si la copie échoue.
        """
        if not self._check_rsync_available():
            raise CopyError("rsync is not available on this system")
        
        # S'assurer que le répertoire de destination existe
        local_dest.mkdir(parents=True, exist_ok=True)
        
        # Construire la commande rsync
        cmd = self._build_rsync_command(server, remote_path, local_dest)
        
        logger.debug(
            f"Running rsync command: {' '.join(cmd)}",
            extra={"server": server.name, "operation": "copy"},
        )
        
        try:
            # Exécuter rsync
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config.rsync.timeout + 10,  # Petit buffer pour le timeout
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown rsync error"
                raise CopyError(f"rsync failed: {error_msg}")
            
            # Trouver le fichier copié (rsync garde le nom du fichier source)
            filename = Path(remote_path).name
            local_file = local_dest / filename
            
            # Vérifier que le fichier existe
            if not local_file.exists():
                raise CopyError(f"File {local_file} was not copied successfully")
            
            logger.info(
                f"File copied successfully: {local_file}",
                extra={"server": server.name, "file": str(local_file), "operation": "copy"},
            )
            
            return local_file
            
        except subprocess.TimeoutExpired:
            raise CopyError(f"rsync timeout after {self.config.rsync.timeout}s")
        except Exception as e:
            if isinstance(e, CopyError):
                raise
            raise CopyError(f"Unexpected error during copy: {str(e)}")
    
    def collect_file(
        self,
        server: ServerConfig,
        remote_path: str,
    ) -> Optional[Path]:
        """
        Collecte un fichier depuis un serveur avec retry.
        
        Args:
            server: Configuration du serveur.
            remote_path: Chemin distant du fichier.
        
        Returns:
            Chemin du fichier collecté ou None si échec après retries.
        """
        filename = Path(remote_path).name
        local_dest = self.config.data_root / "incoming" / server.name
        
        # Vérifier l'état actuel
        state = self.state_manager.get_state(filename, server.name)
        
        # Si le fichier est déjà traité, skip
        if state and state.status in ["processed", "extracted"]:
            logger.info(
                f"File {filename} already processed, skipping",
                extra={"server": server.name, "file": filename, "operation": "copy"},
            )
            return local_dest / filename if (local_dest / filename).exists() else None
        
        # Si le fichier est déjà copié et valide, le retourner
        local_file = local_dest / filename
        if local_file.exists() and state and state.status == "copied":
            # Vérifier le checksum pour s'assurer que le fichier est intact
            if state.checksum:
                current_checksum = self.state_manager.calculate_checksum(local_file)
                if current_checksum == state.checksum:
                    logger.debug(
                        f"File {filename} already copied and verified",
                        extra={"server": server.name, "file": filename, "operation": "copy"},
                    )
                    return local_file
        
        # Mettre à jour le compteur de retry
        retry_count = (state.copy_retry_count if state else 0) + 1
        self.state_manager.update_state(
            filename,
            server.name,
            copy_retry_count=retry_count,
            status="pending",
        )
        
        try:
            # Copier avec retry
            def copy_operation():
                return self._copy_file(server, remote_path, local_dest)
            
            local_file = self.retry_operation.execute(copy_operation)
            
            # Calculer le checksum
            checksum = self.state_manager.calculate_checksum(local_file)
            size = local_file.stat().st_size
            
            # Mettre à jour l'état
            self.state_manager.update_state(
                filename,
                server.name,
                status="copied",
                checksum=checksum,
                copy_retry_count=0,  # Reset après succès
                size=size,
            )
            
            logger.info(
                f"File collected successfully: {local_file}",
                extra={"server": server.name, "file": filename, "operation": "copy"},
            )
            
            return local_file
            
        except Exception as e:
            # Déplacer vers error/copy en cas d'échec définitif
            error_dir = self.config.data_root / "error" / "copy" / server.name
            error_dir.mkdir(parents=True, exist_ok=True)
            
            if local_file.exists():
                error_file = error_dir / filename
                if error_file.exists():
                    error_file.unlink()  # Supprimer l'ancien fichier d'erreur
                local_file.rename(error_file)
                logger.error(
                    f"File moved to error/copy: {error_file}",
                    extra={
                        "server": server.name,
                        "file": filename,
                        "operation": "copy",
                        "error_type": "copy",
                    },
                )
            
            # Mettre à jour l'état
            self.state_manager.update_state(
                filename,
                server.name,
                status="error",
                error_type="copy",
            )
            
            logger.error(
                f"Failed to collect file {filename} from {server.name}: {str(e)}",
                extra={
                    "server": server.name,
                    "file": filename,
                    "operation": "copy",
                    "error_type": "copy",
                },
                exc_info=True,
            )
            
            return None
    
    def collect_all_from_server(self, server: ServerConfig) -> List[Path]:
        """
        Collecte tous les fichiers correspondant au pattern remote_path
        depuis un serveur, en les listant d'abord via SSH.
        
        Args:
            server: Configuration du serveur.
        
        Returns:
            Liste des chemins locaux des fichiers collectés.
        """
        # Découper remote_path en répertoire + pattern
        remote_path = server.remote_path
        # Par défaut, si pas de wildcard, on considère que c'est un chemin complet
        base_dir = os.path.dirname(remote_path) or "."
        pattern = os.path.basename(remote_path)

        # Construire la commande distante pour lister les fichiers.
        # On utilise `find base_dir -maxdepth 1 -type f -name pattern`
        # pour supporter un pattern comme *.log.gz de façon sûre.
        remote_find_cmd = (
            f"find {shlex.quote(base_dir)} -maxdepth 1 -type f -name {shlex.quote(pattern)}"
        )

        ssh_cmd = self._build_ssh_base_cmd() + [
            f"{server.user}@{server.host}",
            remote_find_cmd,
        ]

        logger.debug(
            f"Listing remote files with: {' '.join(ssh_cmd)}",
            extra={"server": server.name, "operation": "list_remote"},
        )

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=self.config.rsync.timeout + 10,
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown ssh error"
                logger.error(
                    f"Failed to list remote files on {server.name}: {error_msg}",
                    extra={"server": server.name, "operation": "list_remote"},
                )
                return []

            remote_files = [
                line.strip()
                for line in result.stdout.splitlines()
                if line.strip()
            ]

            if not remote_files:
                logger.info(
                    f"No remote files found for pattern {remote_path}",
                    extra={"server": server.name, "operation": "list_remote"},
                )
                return []

            logger.info(
                f"Found {len(remote_files)} remote files on {server.name}",
                extra={"server": server.name, "operation": "list_remote"},
            )

            collected: List[Path] = []
            for remote_file in remote_files:
                local = self.collect_file(server, remote_file)
                if local:
                    collected.append(local)

            return collected

        except subprocess.TimeoutExpired:
            logger.error(
                f"Timeout while listing remote files on {server.name}",
                extra={"server": server.name, "operation": "list_remote"},
            )
            return []
        except Exception as e:
            logger.error(
                f"Unexpected error while listing remote files on {server.name}: {e}",
                extra={"server": server.name, "operation": "list_remote"},
                exc_info=True,
            )
            return []

