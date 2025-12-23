"""Gestion de l'état des fichiers pour garantir l'idempotence."""
import json
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict

from .logger import get_logger

logger = get_logger()


@dataclass
class FileState:
    """État d'un fichier dans le pipeline."""
    filename: str
    server: str
    checksum: Optional[str] = None
    copy_retry_count: int = 0
    extract_retry_count: int = 0
    status: str = "pending"  # pending, copied, extracted, processed, error
    error_type: Optional[str] = None  # copy, extract, corruption
    last_updated: str = ""
    size: Optional[int] = None
    
    def __post_init__(self):
        """Initialiser last_updated si vide."""
        if not self.last_updated:
            self.last_updated = datetime.utcnow().isoformat() + "Z"


class StateManager:
    """Gestionnaire d'état pour suivre les fichiers."""
    
    def __init__(self, state_dir: Path):
        """
        Initialise le gestionnaire d'état.
        
        Args:
            state_dir: Répertoire pour stocker les fichiers d'état.
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_state_file(self, filename: str, server: str) -> Path:
        """
        Retourne le chemin du fichier d'état pour un fichier donné.
        
        Args:
            filename: Nom du fichier.
            server: Nom du serveur.
        
        Returns:
            Chemin du fichier d'état.
        """
        # Utiliser un hash pour éviter les problèmes de caractères spéciaux
        safe_name = hashlib.md5(f"{server}:{filename}".encode()).hexdigest()
        return self.state_dir / f"{safe_name}.json"
    
    def get_state(self, filename: str, server: str) -> Optional[FileState]:
        """
        Récupère l'état d'un fichier.
        
        Args:
            filename: Nom du fichier.
            server: Nom du serveur.
        
        Returns:
            FileState ou None si inexistant.
        """
        state_file = self._get_state_file(filename, server)
        
        if not state_file.exists():
            return None
        
        try:
            with open(state_file, "r") as f:
                data = json.load(f)
                return FileState(**data)
        except Exception as e:
            logger.warning(f"Error reading state file {state_file}: {e}")
            return None
    
    def save_state(self, state: FileState) -> None:
        """
        Sauvegarde l'état d'un fichier.
        
        Args:
            state: État à sauvegarder.
        """
        state_file = self._get_state_file(state.filename, state.server)
        state.last_updated = datetime.utcnow().isoformat() + "Z"
        
        try:
            with open(state_file, "w") as f:
                json.dump(asdict(state), f, indent=2)
        except Exception as e:
            logger.error(f"Error saving state file {state_file}: {e}", exc_info=True)
    
    def update_state(
        self,
        filename: str,
        server: str,
        status: Optional[str] = None,
        checksum: Optional[str] = None,
        copy_retry_count: Optional[int] = None,
        extract_retry_count: Optional[int] = None,
        error_type: Optional[str] = None,
        size: Optional[int] = None,
    ) -> FileState:
        """
        Met à jour l'état d'un fichier.
        
        Args:
            filename: Nom du fichier.
            server: Nom du serveur.
            status: Nouveau statut.
            checksum: Nouveau checksum.
            copy_retry_count: Nouveau compteur de retry copy.
            extract_retry_count: Nouveau compteur de retry extract.
            error_type: Type d'erreur.
            size: Taille du fichier.
        
        Returns:
            FileState mis à jour.
        """
        state = self.get_state(filename, server)
        
        if state is None:
            state = FileState(filename=filename, server=server)
        
        if status is not None:
            state.status = status
        if checksum is not None:
            state.checksum = checksum
        if copy_retry_count is not None:
            state.copy_retry_count = copy_retry_count
        if extract_retry_count is not None:
            state.extract_retry_count = extract_retry_count
        if error_type is not None:
            state.error_type = error_type
        if size is not None:
            state.size = size
        
        self.save_state(state)
        return state
    
    def delete_state(self, filename: str, server: str) -> None:
        """
        Supprime l'état d'un fichier.
        
        Args:
            filename: Nom du fichier.
            server: Nom du serveur.
        """
        state_file = self._get_state_file(filename, server)
        
        if state_file.exists():
            try:
                state_file.unlink()
            except Exception as e:
                logger.warning(f"Error deleting state file {state_file}: {e}")
    
    def calculate_checksum(self, filepath: Path) -> str:
        """
        Calcule le checksum SHA256 d'un fichier.
        
        Args:
            filepath: Chemin du fichier.
        
        Returns:
            Checksum hexadécimal.
        """
        sha256 = hashlib.sha256()
        
        try:
            with open(filepath, "rb") as f:
                # Lire par chunks pour les gros fichiers
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    sha256.update(chunk)
            return sha256.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum for {filepath}: {e}", exc_info=True)
            raise

