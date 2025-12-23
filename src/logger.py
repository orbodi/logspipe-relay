"""Configuration du système de logging."""
import json
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Any, Dict
from datetime import datetime

from .config import LogConfig


class JSONFormatter(logging.Formatter):
    """Formateur JSON pour les logs structurés."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Formate un log record en JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Ajouter les champs supplémentaires s'ils existent
        if hasattr(record, "server"):
            log_data["server"] = record.server
        if hasattr(record, "file"):
            log_data["file"] = record.file
        if hasattr(record, "operation"):
            log_data["operation"] = record.operation
        if hasattr(record, "retry_count"):
            log_data["retry_count"] = record.retry_count
        if hasattr(record, "error_type"):
            log_data["error_type"] = record.error_type
        
        # Ajouter l'exception si présente
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def setup_logger(config: LogConfig, log_dir: Path) -> logging.Logger:
    """
    Configure et retourne le logger principal.
    
    Args:
        config: Configuration du logging.
        log_dir: Répertoire pour les fichiers de log.
    
    Returns:
        Logger configuré.
    """
    logger = logging.getLogger("logpipe_relay")
    logger.setLevel(getattr(logging, config.level.upper()))
    
    # Supprimer les handlers existants pour éviter les doublons
    logger.handlers.clear()
    
    # Handler pour fichier avec rotation
    if config.rotation:
        log_file = log_dir / "logpipe-relay.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
        )
    else:
        log_file = log_dir / "logpipe-relay.log"
        file_handler = logging.FileHandler(log_file)
    
    # Handler pour console
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Appliquer le formateur
    if config.format == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str = "logpipe_relay") -> logging.Logger:
    """
    Récupère un logger par nom.
    
    Args:
        name: Nom du logger.
    
    Returns:
        Logger.
    """
    return logging.getLogger(name)

