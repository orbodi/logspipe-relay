"""Nettoyage des fichiers (rétention par âge et pression disque)."""
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from .config import Config
from .disk import get_available_gb
from .state import StateManager
from .logger import get_logger

logger = get_logger()

# (répertoire, libellé, layout: flat | server, priorité — plus bas = supprimé en premier)
_DISK_TIERS: Tuple[Tuple[str, str, str], ...] = (
    ("tmp", "tmp", "flat"),
    ("processed", "processed", "server"),
    ("extracted", "extracted", "server"),
    ("error_copy", "error/copy", "server"),
    ("error_extract", "error/extract", "server"),
    ("error_quarantine", "error/quarantine", "server"),
    ("inputs", "inputs", "flat"),
)


@dataclass
class _FileCandidate:
    path: Path
    label: str
    server: Optional[str]
    mtime: float
    size: int


def _file_age_days(path: Path) -> float:
    """Âge du fichier en jours (basé sur mtime)."""
    return (time.time() - path.stat().st_mtime) / 86400


class CleanupManager:
    """Supprime les fichiers selon l'âge ou l'espace disque disponible."""

    def __init__(self, config: Config, state_manager: StateManager):
        self.config = config
        self.state_manager = state_manager

    def _resolve_tier_dir(self, tier_key: str) -> Path:
        data_root = self.config.data_root
        if tier_key == "tmp":
            return self.config.tmp_dir
        if tier_key == "inputs":
            return self.config.inputs_dir
        if tier_key == "processed":
            return data_root / "processed"
        if tier_key == "extracted":
            return data_root / "extracted"
        if tier_key == "error_copy":
            return data_root / "error" / "copy"
        if tier_key == "error_extract":
            return data_root / "error" / "extract"
        if tier_key == "error_quarantine":
            return data_root / "error" / "quarantine"
        raise ValueError(f"Unknown tier: {tier_key}")

    def _iter_tier_files(self, base_dir: Path, layout: str) -> Iterator[_FileCandidate]:
        if not base_dir.is_dir():
            return

        if layout == "flat":
            for filepath in base_dir.iterdir():
                if filepath.is_file():
                    stat = filepath.stat()
                    yield _FileCandidate(
                        path=filepath,
                        label=base_dir.name,
                        server=None,
                        mtime=stat.st_mtime,
                        size=stat.st_size,
                    )
            return

        for server_dir in base_dir.iterdir():
            if not server_dir.is_dir():
                continue
            for filepath in server_dir.iterdir():
                if filepath.is_file():
                    stat = filepath.stat()
                    yield _FileCandidate(
                        path=filepath,
                        label=server_dir.parent.name,
                        server=server_dir.name,
                        mtime=stat.st_mtime,
                        size=stat.st_size,
                    )

    def _delete_file(self, candidate: _FileCandidate) -> None:
        candidate.path.unlink()
        if candidate.server:
            self.state_manager.delete_state(candidate.path.name, candidate.server)

    def _cleanup_server_tree(
        self,
        base_dir: Path,
        max_age_days: int,
        label: str,
    ) -> Dict[str, int]:
        if max_age_days <= 0:
            logger.info(
                f"Cleanup disabled for {label} (retention <= 0)",
                extra={"operation": "cleanup", "target": label},
            )
            return {"deleted": 0, "errors": 0, "skipped": 1}

        deleted = 0
        errors = 0

        for server_dir in sorted(base_dir.iterdir()) if base_dir.is_dir() else []:
            if not server_dir.is_dir():
                continue

            server = server_dir.name
            for filepath in sorted(server_dir.iterdir()):
                if not filepath.is_file():
                    continue

                try:
                    age_days = _file_age_days(filepath)
                    if age_days < max_age_days:
                        continue

                    self._delete_file(
                        _FileCandidate(filepath, label, server, filepath.stat().st_mtime, 0)
                    )
                    deleted += 1

                    logger.info(
                        f"Deleted old file {filepath.name} from {label}/{server} "
                        f"(age: {age_days:.1f} days, retention: {max_age_days} days)",
                        extra={
                            "operation": "cleanup",
                            "mode": "age",
                            "target": label,
                            "server": server,
                            "file": filepath.name,
                        },
                    )
                except OSError as exc:
                    errors += 1
                    logger.warning(
                        f"Failed to delete {filepath}: {exc}",
                        extra={
                            "operation": "cleanup",
                            "mode": "age",
                            "target": label,
                            "server": server,
                            "file": filepath.name,
                        },
                    )

        return {"deleted": deleted, "errors": errors, "skipped": 0}

    def _cleanup_flat_dir(
        self,
        directory: Path,
        max_age_days: int,
        label: str,
    ) -> Dict[str, int]:
        if max_age_days <= 0:
            return {"deleted": 0, "errors": 0, "skipped": 1}

        if not directory.is_dir():
            return {"deleted": 0, "errors": 0, "skipped": 0}

        deleted = 0
        errors = 0

        for filepath in sorted(directory.iterdir()):
            if not filepath.is_file():
                continue

            try:
                age_days = _file_age_days(filepath)
                if age_days < max_age_days:
                    continue

                filepath.unlink()
                deleted += 1

                logger.info(
                    f"Deleted old file {filepath.name} from {label} "
                    f"(age: {age_days:.1f} days, retention: {max_age_days} days)",
                    extra={
                        "operation": "cleanup",
                        "mode": "age",
                        "target": label,
                        "file": filepath.name,
                    },
                )
            except OSError as exc:
                errors += 1
                logger.warning(
                    f"Failed to delete {filepath}: {exc}",
                    extra={"operation": "cleanup", "mode": "age", "target": label, "file": filepath.name},
                )

        return {"deleted": deleted, "errors": errors, "skipped": 0}

    def run_age_cleanup(self) -> Dict[str, Dict[str, int]]:
        """Nettoyage par âge selon pipeline.conf."""
        pipeline = self.config.pipeline
        data_root = self.config.data_root

        stats: Dict[str, Dict[str, int]] = {
            "processed": self._cleanup_server_tree(
                data_root / "processed",
                pipeline.cleanup_processed_after_days,
                "processed",
            ),
            "extracted": self._cleanup_server_tree(
                data_root / "extracted",
                pipeline.cleanup_processed_after_days,
                "extracted",
            ),
            "inputs": self._cleanup_flat_dir(
                self.config.inputs_dir,
                pipeline.cleanup_inputs_after_days,
                "inputs",
            ),
            "tmp": self._cleanup_flat_dir(
                self.config.tmp_dir,
                pipeline.cleanup_tmp_after_days,
                "tmp",
            ),
        }

        error_root = data_root / "error"
        for error_type in ("copy", "extract", "quarantine"):
            stats[f"error_{error_type}"] = self._cleanup_server_tree(
                error_root / error_type,
                pipeline.cleanup_error_after_days,
                f"error/{error_type}",
            )

        return stats

    def _collect_disk_candidates(self) -> List[_FileCandidate]:
        """Liste les fichiers supprimables, par priorité puis du plus ancien au plus récent."""
        candidates: List[_FileCandidate] = []

        for tier_key, label, layout in _DISK_TIERS:
            if tier_key == "inputs" and not self.config.pipeline.disk_cleanup_include_inputs:
                continue

            base_dir = self._resolve_tier_dir(tier_key)
            tier_files = sorted(
                self._iter_tier_files(base_dir, layout),
                key=lambda c: c.mtime,
            )
            candidates.extend(tier_files)

        return candidates

    def run_disk_cleanup(self) -> Dict[str, int]:
        """
        Supprime les fichiers les plus anciens jusqu'à retrouver l'espace cible.

        Déclenché lorsque l'espace libre < disk_space_threshold_gb.
        S'arrête lorsque l'espace libre >= disk_space_target_gb.
        """
        pipeline = self.config.pipeline
        threshold_gb = pipeline.disk_space_threshold_gb

        if threshold_gb <= 0:
            return {
                "deleted": 0,
                "errors": 0,
                "skipped": 1,
                "triggered": False,
                "available_gb_before": get_available_gb(self.config.root_dir),
                "available_gb_after": get_available_gb(self.config.root_dir),
            }

        target_gb = pipeline.disk_space_target_gb or threshold_gb
        root = self.config.root_dir
        available_before = get_available_gb(root)

        if available_before >= threshold_gb:
            return {
                "deleted": 0,
                "errors": 0,
                "skipped": 0,
                "triggered": False,
                "available_gb_before": available_before,
                "available_gb_after": available_before,
            }

        logger.warning(
            f"Low disk space ({available_before:.2f} GB free, threshold: {threshold_gb} GB). "
            f"Starting disk-pressure cleanup (target: {target_gb} GB)",
            extra={"operation": "cleanup", "mode": "disk"},
        )

        deleted = 0
        errors = 0
        freed_bytes = 0

        for candidate in self._collect_disk_candidates():
            if get_available_gb(root) >= target_gb:
                break

            try:
                self._delete_file(candidate)
                deleted += 1
                freed_bytes += candidate.size

                logger.info(
                    f"Disk cleanup: deleted {candidate.path.name} from {candidate.label} "
                    f"({candidate.size / (1024 ** 2):.1f} MB freed)",
                    extra={
                        "operation": "cleanup",
                        "mode": "disk",
                        "target": candidate.label,
                        "file": candidate.path.name,
                    },
                )
            except OSError as exc:
                errors += 1
                logger.warning(
                    f"Disk cleanup failed for {candidate.path}: {exc}",
                    extra={
                        "operation": "cleanup",
                        "mode": "disk",
                        "target": candidate.label,
                        "file": candidate.path.name,
                    },
                )

        available_after = get_available_gb(root)

        if available_after < threshold_gb:
            logger.error(
                f"Disk cleanup finished but space still low: {available_after:.2f} GB free "
                f"(threshold: {threshold_gb} GB, deleted: {deleted} file(s))",
                extra={"operation": "cleanup", "mode": "disk"},
            )
        else:
            logger.info(
                f"Disk cleanup completed: {deleted} file(s) deleted, "
                f"{freed_bytes / (1024 ** 3):.2f} GB freed, "
                f"{available_before:.2f} → {available_after:.2f} GB available",
                extra={"operation": "cleanup", "mode": "disk"},
            )

        return {
            "deleted": deleted,
            "errors": errors,
            "skipped": 0,
            "triggered": True,
            "available_gb_before": round(available_before, 2),
            "available_gb_after": round(available_after, 2),
            "freed_gb": round(freed_bytes / (1024 ** 3), 2),
        }

    def run(self) -> Dict[str, object]:
        """Nettoyage par âge puis libération d'espace disque si nécessaire."""
        age_stats = self.run_age_cleanup()
        disk_stats = self.run_disk_cleanup()

        total_deleted = sum(s.get("deleted", 0) for s in age_stats.values()) + disk_stats.get("deleted", 0)
        total_errors = sum(s.get("errors", 0) for s in age_stats.values()) + disk_stats.get("errors", 0)

        logger.info(
            f"Cleanup completed: {total_deleted} file(s) deleted, {total_errors} error(s)",
            extra={"operation": "cleanup", "age": age_stats, "disk": disk_stats},
        )

        return {"age": age_stats, "disk": disk_stats}
