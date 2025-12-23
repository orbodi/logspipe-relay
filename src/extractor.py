"""Module d'extraction des fichiers gzip."""
import gzip
import shutil
from pathlib import Path
from typing import Optional

from .config import Config
from .retry import RetryableOperation
from .state import StateManager
from .logger import get_logger

logger = get_logger()


class ExtractError(Exception):
    """Exception pour erreurs d'extraction."""
    pass


class CorruptionError(ExtractError):
    """Exception pour fichiers corrompus."""
    pass


class Extractor:
    """Extracteur de fichiers gzip."""
    
    def __init__(self, config: Config, state_manager: StateManager):
        """
        Initialise l'extracteur.
        
        Args:
            config: Configuration du pipeline.
            state_manager: Gestionnaire d'état.
        """
        self.config = config
        self.state_manager = state_manager
        self.retry_operation = RetryableOperation(
            max_retries=config.retry.max_retry_extract,
            config=config.retry,
            operation_name="extract",
        )
    
    def _validate_gzip(self, filepath: Path) -> bool:
        """
        Valide qu'un fichier gzip n'est pas corrompu.
        
        Args:
            filepath: Chemin du fichier à valider.
        
        Returns:
            True si le fichier est valide.
        
        Raises:
            CorruptionError: Si le fichier est corrompu.
        """
        try:
            with gzip.open(filepath, "rb") as f:
                # Tenter de lire tout le fichier pour détecter la corruption
                while f.read(8192):
                    pass
            return True
        except (gzip.BadGzipFile, OSError, EOFError) as e:
            raise CorruptionError(f"Gzip file is corrupted: {str(e)}")
        except Exception as e:
            raise CorruptionError(f"Error validating gzip file: {str(e)}")
    
    def _extract_file(self, gzip_file: Path, dest_dir: Path) -> Path:
        """
        Extrait un fichier gzip.
        
        Args:
            gzip_file: Chemin du fichier gzip.
            dest_dir: Répertoire de destination.
        
        Returns:
            Chemin du fichier extrait.
        
        Raises:
            ExtractError: Si l'extraction échoue.
            CorruptionError: Si le fichier est corrompu.
        """
        # Valider le fichier gzip si demandé
        if self.config.extract.validate_gzip:
            self._validate_gzip(gzip_file)
        
        # S'assurer que le répertoire de destination existe
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Nom du fichier extrait (sans .gz)
        extracted_name = gzip_file.stem
        extracted_path = dest_dir / extracted_name
        
        # Si le fichier extrait existe déjà, le supprimer
        if extracted_path.exists():
            extracted_path.unlink()
        
        try:
            # Extraire le fichier
            with gzip.open(gzip_file, "rb") as f_in:
                with open(extracted_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(
                f"File extracted successfully: {extracted_path}",
                extra={
                    "file": str(extracted_path),
                    "source": str(gzip_file),
                    "operation": "extract",
                },
            )
            
            return extracted_path
            
        except CorruptionError:
            raise
        except Exception as e:
            raise ExtractError(f"Error extracting file: {str(e)}")
    
    def extract_file(self, gzip_file: Path, server: str) -> Optional[Path]:
        """
        Extrait un fichier gzip avec retry.
        
        Args:
            gzip_file: Chemin du fichier gzip à extraire.
            server: Nom du serveur source.
        
        Returns:
            Chemin du fichier extrait ou None si échec après retries.
        """
        if not gzip_file.exists():
            logger.error(
                f"Gzip file does not exist: {gzip_file}",
                extra={"file": str(gzip_file), "operation": "extract"},
            )
            return None
        
        filename = gzip_file.name
        dest_dir = self.config.data_root / "extracted" / server
        
        # Vérifier l'état actuel
        state = self.state_manager.get_state(filename, server)
        
        # Si le fichier est déjà extrait, vérifier qu'il existe toujours
        if state and state.status == "extracted":
            extracted_name = gzip_file.stem
            extracted_path = dest_dir / extracted_name
            if extracted_path.exists():
                logger.debug(
                    f"File {filename} already extracted",
                    extra={"server": server, "file": filename, "operation": "extract"},
                )
                return extracted_path
        
        # Mettre à jour le compteur de retry
        retry_count = (state.extract_retry_count if state else 0) + 1
        self.state_manager.update_state(
            filename,
            server,
            extract_retry_count=retry_count,
            status="copied",  # État après copie, avant extraction
        )
        
        try:
            # Extraire avec retry
            def extract_operation():
                return self._extract_file(gzip_file, dest_dir)
            
            extracted_path = self.retry_operation.execute(extract_operation)
            
            # Calculer le checksum du fichier extrait
            checksum = self.state_manager.calculate_checksum(extracted_path)
            size = extracted_path.stat().st_size
            
            # Mettre à jour l'état
            self.state_manager.update_state(
                filename,
                server,
                status="extracted",
                checksum=checksum,
                extract_retry_count=0,  # Reset après succès
                size=size,
            )
            
            # Supprimer le fichier source si configuré
            if self.config.extract.delete_source:
                try:
                    gzip_file.unlink()
                    logger.debug(
                        f"Source file deleted: {gzip_file}",
                        extra={"file": str(gzip_file), "operation": "extract"},
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to delete source file {gzip_file}: {e}",
                        extra={"file": str(gzip_file), "operation": "extract"},
                    )
            
            logger.info(
                f"File extracted successfully: {extracted_path}",
                extra={
                    "server": server,
                    "file": filename,
                    "extracted": str(extracted_path),
                    "operation": "extract",
                },
            )
            
            return extracted_path
            
        except CorruptionError as e:
            # Fichier corrompu: déplacer vers quarantine
            quarantine_dir = self.config.data_root / "error" / "quarantine" / server
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            
            quarantine_file = quarantine_dir / filename
            if quarantine_file.exists():
                quarantine_file.unlink()
            
            gzip_file.rename(quarantine_file)
            
            logger.error(
                f"Corrupted file moved to quarantine: {quarantine_file}",
                extra={
                    "server": server,
                    "file": filename,
                    "operation": "extract",
                    "error_type": "corruption",
                },
            )
            
            # Mettre à jour l'état
            self.state_manager.update_state(
                filename,
                server,
                status="error",
                error_type="corruption",
            )
            
            return None
            
        except Exception as e:
            # Autre erreur: déplacer vers error/extract
            error_dir = self.config.data_root / "error" / "extract" / server
            error_dir.mkdir(parents=True, exist_ok=True)
            
            error_file = error_dir / filename
            if error_file.exists():
                error_file.unlink()
            
            gzip_file.rename(error_file)
            
            logger.error(
                f"File moved to error/extract: {error_file}",
                extra={
                    "server": server,
                    "file": filename,
                    "operation": "extract",
                    "error_type": "extract",
                },
            )
            
            # Mettre à jour l'état
            self.state_manager.update_state(
                filename,
                server,
                status="error",
                error_type="extract",
            )
            
            logger.error(
                f"Failed to extract file {filename}: {str(e)}",
                extra={
                    "server": server,
                    "file": filename,
                    "operation": "extract",
                    "error_type": "extract",
                },
                exc_info=True,
            )
            
            return None
    
    def move_to_processed(self, gzip_file: Path, server: str) -> Path:
        """
        Déplace un fichier gzip vers le répertoire processed.
        
        Args:
            gzip_file: Chemin du fichier gzip.
            server: Nom du serveur source.
        
        Returns:
            Nouveau chemin du fichier.
        """
        processed_dir = self.config.data_root / "processed" / server
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        processed_file = processed_dir / gzip_file.name
        
        # Si le fichier existe déjà dans processed, le supprimer
        if processed_file.exists():
            processed_file.unlink()
        
        gzip_file.rename(processed_file)
        
        # Mettre à jour l'état
        filename = gzip_file.name
        self.state_manager.update_state(
            filename,
            server,
            status="processed",
        )
        
        logger.info(
            f"File moved to processed: {processed_file}",
            extra={
                "server": server,
                "file": filename,
                "operation": "process",
            },
        )
        
        return processed_file

