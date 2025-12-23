"""Orchestrateur principal du pipeline."""
import os
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import Config, ServerConfig
from .collectors import Collector
from .extractor import Extractor
from .state import StateManager
from .logger import setup_logger, get_logger


class Pipeline:
    """Orchestrateur principal du pipeline de traitement des logs."""
    
    def __init__(self, config: Config):
        """
        Initialise le pipeline.
        
        Args:
            config: Configuration du pipeline.
        """
        self.config = config
        
        # Initialiser le logger
        self.logger = setup_logger(config.log, config.log_dir)
        
        # Initialiser les composants
        self.state_manager = StateManager(config.state_dir)
        self.collector = Collector(config, self.state_manager)
        self.extractor = Extractor(config, self.state_manager)
        
        self.logger.info("Pipeline initialized", extra={"operation": "init"})
    
    def _check_disk_space(self) -> bool:
        """
        Vérifie l'espace disque disponible.
        
        Returns:
            True si assez d'espace disponible.
        """
        try:
            stat = os.statvfs(self.config.data_root)
            available_gb = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)
            
            if available_gb < self.config.pipeline.disk_space_threshold_gb:
                self.logger.warning(
                    f"Low disk space: {available_gb:.2f} GB available "
                    f"(threshold: {self.config.pipeline.disk_space_threshold_gb} GB)",
                    extra={"operation": "disk_check"},
                )
                return False
            
            return True
        except AttributeError:
            # Windows n'a pas statvfs, utiliser shutil.disk_usage
            import shutil
            total, used, free = shutil.disk_usage(self.config.data_root)
            available_gb = free / (1024 ** 3)
            
            if available_gb < self.config.pipeline.disk_space_threshold_gb:
                self.logger.warning(
                    f"Low disk space: {available_gb:.2f} GB available "
                    f"(threshold: {self.config.pipeline.disk_space_threshold_gb} GB)",
                    extra={"operation": "disk_check"},
                )
                return False
            
            return True
        except Exception as e:
            self.logger.error(
                f"Error checking disk space: {e}",
                extra={"operation": "disk_check"},
                exc_info=True,
            )
            return True  # Continuer en cas d'erreur de vérification
    
    def process_file_from_server(
        self,
        server: ServerConfig,
        remote_path: str,
    ) -> bool:
        """
        Traite un fichier depuis un serveur (collecte + extraction).
        
        Args:
            server: Configuration du serveur.
            remote_path: Chemin distant du fichier.
        
        Returns:
            True si le traitement a réussi.
        """
        filename = Path(remote_path).name
        
        try:
            # Étape 1: Collecte
            self.logger.info(
                f"Collecting file {filename} from {server.name}",
                extra={"server": server.name, "file": filename, "operation": "collect"},
            )
            
            collected_file = self.collector.collect_file(server, remote_path)
            
            if collected_file is None:
                self.logger.error(
                    f"Failed to collect file {filename} from {server.name}",
                    extra={"server": server.name, "file": filename, "operation": "collect"},
                )
                return False
            
            # Étape 2: Extraction
            self.logger.info(
                f"Extracting file {collected_file}",
                extra={"server": server.name, "file": filename, "operation": "extract"},
            )
            
            extracted_file = self.extractor.extract_file(collected_file, server.name)
            
            if extracted_file is None:
                self.logger.error(
                    f"Failed to extract file {filename}",
                    extra={"server": server.name, "file": filename, "operation": "extract"},
                )
                return False
            
            # Étape 3: Déplacer le .gz vers processed (si extraction OK et delete_source=False)
            if not self.config.extract.delete_source:
                self.extractor.move_to_processed(collected_file, server.name)

            # Étape 4: Déplacer le fichier extrait vers SHARE_DIR si configuré
            if extracted_file:
                self.extractor.move_extracted_to_share(extracted_file, server.name)
            
            self.logger.info(
                f"File {filename} processed successfully",
                extra={"server": server.name, "file": filename, "operation": "process"},
            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Error processing file {filename} from {server.name}: {e}",
                extra={"server": server.name, "file": filename, "operation": "process"},
                exc_info=True,
            )
            return False
    
    def process_server(self, server: ServerConfig) -> dict:
        """
        Traite tous les fichiers d'un serveur.
        
        Args:
            server: Configuration du serveur.
        
        Returns:
            Dictionnaire avec statistiques de traitement.
        """
        if not server.enabled:
            self.logger.info(
                f"Server {server.name} is disabled, skipping",
                extra={"server": server.name, "operation": "process_server"},
            )
            return {"server": server.name, "processed": 0, "failed": 0, "skipped": True}
        
        self.logger.info(
            f"Processing server {server.name}",
            extra={"server": server.name, "operation": "process_server"},
        )

        # Étape 1 : lister les fichiers distants via SSH en utilisant le pattern remote_path
        collected_files = self.collector.collect_all_from_server(server)

        processed = 0
        failed = 0

        # Étape 2 : extraire les fichiers collectés (ils sont maintenant dans incoming/)
        for local_file in collected_files:
            try:
                self.logger.info(
                    f"Extracting file {local_file.name} from collected set",
                    extra={
                        "server": server.name,
                        "file": local_file.name,
                        "operation": "extract",
                    },
                )

                extracted_file = self.extractor.extract_file(local_file, server.name)

                if extracted_file:
                    if not self.config.extract.delete_source:
                        self.extractor.move_to_processed(local_file, server.name)
                    # Déplacer le fichier extrait vers SHARE_DIR si configuré
                    self.extractor.move_extracted_to_share(extracted_file, server.name)
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                self.logger.error(
                    f"Error processing collected file {local_file.name} from {server.name}: {e}",
                    extra={
                        "server": server.name,
                        "file": local_file.name,
                        "operation": "process_server",
                    },
                    exc_info=True,
                )
                failed += 1

        return {
            "server": server.name,
            "processed": processed,
            "failed": failed,
            "skipped": False,
        }
    
    def process_incoming_files(self) -> dict:
        """
        Traite les fichiers déjà présents dans incoming/.
        
        Returns:
            Dictionnaire avec statistiques de traitement.
        """
        self.logger.info("Processing files in incoming/", extra={"operation": "process_incoming"})
        
        stats = {"processed": 0, "failed": 0}
        
        incoming_dir = self.config.data_root / "incoming"
        
        # Parcourir tous les serveurs
        for server_config in self.config.servers:
            if not server_config.enabled:
                continue
            
            server_incoming = incoming_dir / server_config.name
            
            if not server_incoming.exists():
                continue
            
            # Parcourir les fichiers .gz dans incoming
            for gz_file in server_incoming.glob("*.gz"):
                try:
                    self.logger.info(
                        f"Extracting file {gz_file.name} from incoming",
                        extra={
                            "server": server_config.name,
                            "file": gz_file.name,
                            "operation": "extract",
                        },
                    )
                    
                    extracted_file = self.extractor.extract_file(gz_file, server_config.name)
                    
                    if extracted_file:
                        if not self.config.extract.delete_source:
                            self.extractor.move_to_processed(gz_file, server_config.name)
                        # Déplacer le fichier extrait vers SHARE_DIR si configuré
                        self.extractor.move_extracted_to_share(extracted_file, server_config.name)
                        stats["processed"] += 1
                    else:
                        stats["failed"] += 1
                        
                except Exception as e:
                    self.logger.error(
                        f"Error processing file {gz_file.name}: {e}",
                        extra={
                            "server": server_config.name,
                            "file": gz_file.name,
                            "operation": "process_incoming",
                        },
                        exc_info=True,
                    )
                    stats["failed"] += 1
        
        self.logger.info(
            f"Processed {stats['processed']} files, {stats['failed']} failed",
            extra={"operation": "process_incoming", **stats},
        )
        
        return stats
    
    def run(self, process_incoming: bool = True, parallel: bool = True) -> dict:
        """
        Exécute le pipeline complet.
        
        Args:
            process_incoming: Si True, traite les fichiers déjà dans incoming/.
            parallel: Si True, traite les serveurs en parallèle.
        
        Returns:
            Dictionnaire avec statistiques globales.
        """
        self.logger.info("Starting pipeline execution", extra={"operation": "run"})
        
        # Vérifier l'espace disque
        if not self._check_disk_space():
            self.logger.warning(
                "Low disk space detected, but continuing",
                extra={"operation": "run"},
            )
        
        overall_stats = {
            "servers": {},
            "incoming": {"processed": 0, "failed": 0},
        }
        
        # Traiter les fichiers incoming si demandé
        if process_incoming:
            overall_stats["incoming"] = self.process_incoming_files()
        
        # Traiter les serveurs
        if parallel and len(self.config.servers) > 1:
            # Parallélisation limitée par parallel_workers
            with ThreadPoolExecutor(max_workers=self.config.pipeline.parallel_workers) as executor:
                futures = {
                    executor.submit(self.process_server, server): server.name
                    for server in self.config.servers
                }
                
                for future in as_completed(futures):
                    server_name = futures[future]
                    try:
                        result = future.result()
                        overall_stats["servers"][server_name] = result
                    except Exception as e:
                        self.logger.error(
                            f"Error processing server {server_name}: {e}",
                            extra={"server": server_name, "operation": "run"},
                            exc_info=True,
                        )
                        overall_stats["servers"][server_name] = {
                            "processed": 0,
                            "failed": 0,
                            "error": str(e),
                        }
        else:
            # Traitement séquentiel
            for server in self.config.servers:
                try:
                    result = self.process_server(server)
                    overall_stats["servers"][server.name] = result
                except Exception as e:
                    self.logger.error(
                        f"Error processing server {server.name}: {e}",
                        extra={"server": server.name, "operation": "run"},
                        exc_info=True,
                    )
                    overall_stats["servers"][server.name] = {
                        "processed": 0,
                        "failed": 0,
                        "error": str(e),
                    }
        
        self.logger.info(
            "Pipeline execution completed",
            extra={"operation": "run", "stats": overall_stats},
        )
        
        return overall_stats

