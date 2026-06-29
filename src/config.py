"""Gestion de la configuration du pipeline."""
import os
import json
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Permissions par défaut des répertoires (rwxr-xr-x), appliquées sur Unix uniquement
_DEFAULT_DIR_MODE = 0o755


def _ensure_dir(path: Path, mode: int = _DEFAULT_DIR_MODE) -> None:
    """Crée un répertoire (et ses parents) et applique les permissions si possible."""
    path.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        try:
            os.chmod(path, mode)
        except OSError:
            pass


def _layout_from_root(root_dir: Path) -> dict:
    """Dérive l'arborescence standard à partir de ROOT_DIR."""
    return {
        "root_dir": root_dir,
        "inputs_dir": root_dir / "inputs",
        "data_root": root_dir / "data",
        "state_dir": root_dir / "state",
        "log_dir": root_dir / "logs",
        "tmp_dir": root_dir / "tmp",
    }


@dataclass
class RetryConfig:
    """Configuration des mécanismes de retry."""
    max_retry_copy: int = 3
    max_retry_extract: int = 3
    delay_base: int = 60
    delay_max: int = 3600
    backoff_multiplier: float = 2.0


@dataclass
class RsyncConfig:
    """Configuration pour rsync."""
    timeout: int = 300
    options: str = "-avz --partial"


@dataclass
class ExtractConfig:
    """Configuration pour l'extraction."""
    validate_gzip: bool = True
    delete_source: bool = False


@dataclass
class LogConfig:
    """Configuration du logging."""
    level: str = "INFO"
    format: str = "json"  # json ou text
    rotation: bool = True
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class ServerConfig:
    """Configuration d'un serveur source."""
    name: str
    host: str
    user: str
    remote_path: str
    enabled: bool = True


@dataclass
class PipelineConfig:
    """Configuration globale du pipeline."""
    parallel_workers: int = 2
    file_check_interval: int = 60
    cleanup_processed_after_days: int = 30
    cleanup_error_after_days: int = 90
    cleanup_inputs_after_days: int = 0
    cleanup_tmp_after_days: int = 7
    max_concurrent_extractions: int = 4
    disk_space_threshold_gb: int = 10
    disk_space_target_gb: int = 0
    disk_cleanup_include_inputs: bool = True


@dataclass
class Config:
    """Configuration principale du projet."""
    root_dir: Path
    inputs_dir: Path
    data_root: Path
    state_dir: Path
    log_dir: Path
    tmp_dir: Path
    retry: RetryConfig = field(default_factory=RetryConfig)
    rsync: RsyncConfig = field(default_factory=RsyncConfig)
    extract: ExtractConfig = field(default_factory=ExtractConfig)
    log: LogConfig = field(default_factory=LogConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    servers: List[ServerConfig] = field(default_factory=list)

    @property
    def share_dir(self) -> Path:
        """Alias rétrocompatible : inputs_dir est la destination des fichiers extraits."""
        return self.inputs_dir

    def __post_init__(self):
        """Crée ROOT_DIR et toute l'arborescence nécessaire."""
        for path in (self.root_dir, self.inputs_dir, self.data_root, self.state_dir, self.log_dir, self.tmp_dir):
            _ensure_dir(path)

        for stage in ["incoming", "extracted", "processed", "error"]:
            _ensure_dir(self.data_root / stage)
            if stage == "error":
                for error_type in ["copy", "extract", "quarantine"]:
                    _ensure_dir(self.data_root / stage / error_type)

        for server in self.servers:
            for stage in ["incoming", "extracted", "processed"]:
                _ensure_dir(self.data_root / stage / server.name)
            for error_type in ["copy", "extract", "quarantine"]:
                _ensure_dir(self.data_root / "error" / error_type / server.name)


def load_config(config_dir: Optional[Path] = None) -> Config:
    """
    Charge la configuration depuis les fichiers .env et .conf.
    
    Args:
        config_dir: Répertoire contenant les fichiers de configuration.
                    Si None, utilise conf/ dans le projet.
    
    Returns:
        Objet Config configuré.
    """
    if config_dir is None:
        # Chercher le répertoire conf/ relatif au projet
        project_root = Path(__file__).parent.parent
        config_dir = project_root / "conf"
    
    config_dir = Path(config_dir)
    
    # Charger .env
    env_file = config_dir / ".env"
    if env_file.exists():
        load_dotenv(env_file)
    else:
        # Essayer env.example en fallback
        env_example = config_dir / "env.example"
        if env_example.exists():
            load_dotenv(env_example)
    
    project_root = Path(__file__).parent.parent
    default_root = project_root / "data"

    root_dir_env = os.getenv("ROOT_DIR", "").strip()
    if root_dir_env:
        layout = _layout_from_root(Path(root_dir_env))
    else:
        layout = _layout_from_root(default_root)
    
    # Configuration retry
    retry = RetryConfig(
        max_retry_copy=int(os.getenv("MAX_RETRY_COPY", "3")),
        max_retry_extract=int(os.getenv("MAX_RETRY_EXTRACT", "3")),
        delay_base=int(os.getenv("RETRY_DELAY_BASE", "60")),
        delay_max=int(os.getenv("RETRY_DELAY_MAX", "3600")),
        backoff_multiplier=float(os.getenv("RETRY_BACKOFF_MULTIPLIER", "2.0")),
    )
    
    # Configuration rsync
    rsync = RsyncConfig(
        timeout=int(os.getenv("RSYNC_TIMEOUT", "300")),
        options=os.getenv("RSYNC_OPTIONS", "-avz --partial"),
    )
    
    # Configuration extraction
    extract = ExtractConfig(
        validate_gzip=os.getenv("GZIP_VALIDATE", "True").lower() == "true",
        delete_source=os.getenv("EXTRACT_DELETE_SOURCE", "False").lower() == "true",
    )
    
    # Configuration logging
    log = LogConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format=os.getenv("LOG_FORMAT", "json"),
        rotation=os.getenv("LOG_ROTATION", "True").lower() == "true",
        max_bytes=int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))),
        backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
    )
    
    def _load_json_allow_comments(path: Path) -> dict:
        """
        Charge un fichier JSON en ignorant les lignes de commentaires (commençant par #).

        Cela permet d'utiliser des fichiers .conf avec des commentaires en tête.
        """
        try:
            with open(path, "r") as f:
                lines = []
                for line in f:
                    stripped = line.lstrip()
                    if stripped.startswith("#") or stripped == "":
                        continue
                    lines.append(line)
            content = "".join(lines).strip()
            if not content:
                return {}
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file {path}: {e}") from e

    # Charger configuration pipeline
    pipeline_config = {}
    pipeline_file = config_dir / "pipeline.conf"
    if not pipeline_file.exists():
        pipeline_file = config_dir / "pipeline.conf.example"
    
    if pipeline_file.exists():
        pipeline_config = _load_json_allow_comments(pipeline_file)
    
    pipeline = PipelineConfig(
        parallel_workers=pipeline_config.get("parallel_workers", 2),
        file_check_interval=pipeline_config.get("file_check_interval", 60),
        cleanup_processed_after_days=pipeline_config.get("cleanup_processed_after_days", 30),
        cleanup_error_after_days=pipeline_config.get("cleanup_error_after_days", 90),
        cleanup_inputs_after_days=pipeline_config.get("cleanup_inputs_after_days", 0),
        cleanup_tmp_after_days=pipeline_config.get("cleanup_tmp_after_days", 7),
        max_concurrent_extractions=pipeline_config.get("max_concurrent_extractions", 4),
        disk_space_threshold_gb=pipeline_config.get("disk_space_threshold_gb", 10),
        disk_space_target_gb=pipeline_config.get("disk_space_target_gb", 0),
        disk_cleanup_include_inputs=pipeline_config.get("disk_cleanup_include_inputs", True),
    )
    
    # Charger configuration serveurs
    servers = []
    sources_file = config_dir / "sources.conf"
    if not sources_file.exists():
        sources_file = config_dir / "sources.conf.example"
    
    if sources_file.exists():
        sources_data = _load_json_allow_comments(sources_file)
        for server_data in sources_data.get("servers", []):
            servers.append(ServerConfig(
                name=server_data["name"],
                host=server_data["host"],
                user=server_data["user"],
                remote_path=server_data["remote_path"],
                enabled=server_data.get("enabled", True),
            ))
    
    config = Config(
        **layout,
        retry=retry,
        rsync=rsync,
        extract=extract,
        log=log,
        pipeline=pipeline,
        servers=servers,
    )
    
    return config

