"""Mécanisme de retry avec backoff exponentiel."""
import time
import random
from functools import wraps
from typing import Callable, TypeVar, Optional, Any
from .config import RetryConfig
from .logger import get_logger

logger = get_logger()
T = TypeVar("T")


def calculate_backoff_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    multiplier: float,
    use_jitter: bool = True,
) -> float:
    """
    Calcule le délai de backoff pour une tentative donnée.
    
    Args:
        attempt: Numéro de la tentative (commence à 1).
        base_delay: Délai de base en secondes.
        max_delay: Délai maximum en secondes.
        multiplier: Multiplicateur pour backoff exponentiel.
        use_jitter: Ajouter du jitter pour éviter le thundering herd.
    
    Returns:
        Délai en secondes.
    """
    delay = base_delay * (multiplier ** (attempt - 1))
    delay = min(delay, max_delay)
    
    if use_jitter:
        # Ajouter jusqu'à 25% de jitter
        jitter = delay * 0.25 * random.random()
        delay = delay + jitter
    
    return delay


def retry_with_backoff(
    max_retries: int,
    config: RetryConfig,
    operation_name: str = "operation",
    log_retries: bool = True,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Décorateur pour retry avec backoff exponentiel.
    
    Args:
        max_retries: Nombre maximum de tentatives.
        config: Configuration du retry.
        operation_name: Nom de l'opération pour les logs.
        log_retries: Logger les retries.
    
    Returns:
        Décorateur.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Optional[Exception] = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    
                    # Succès après retry
                    if attempt > 1 and log_retries:
                        logger.info(
                            f"{operation_name} succeeded after {attempt} attempts",
                            extra={"operation": operation_name, "retry_count": attempt},
                        )
                    
                    return result
                    
                except Exception as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        delay = calculate_backoff_delay(
                            attempt,
                            config.delay_base,
                            config.delay_max,
                            config.backoff_multiplier,
                        )
                        
                        if log_retries:
                            logger.warning(
                                f"{operation_name} failed (attempt {attempt}/{max_retries}): {str(e)}. "
                                f"Retrying in {delay:.2f}s...",
                                extra={
                                    "operation": operation_name,
                                    "retry_count": attempt,
                                    "error_type": type(e).__name__,
                                },
                            )
                        
                        time.sleep(delay)
                    else:
                        # Dernière tentative échouée
                        if log_retries:
                            logger.error(
                                f"{operation_name} failed after {max_retries} attempts: {str(e)}",
                                extra={
                                    "operation": operation_name,
                                    "retry_count": attempt,
                                    "error_type": type(e).__name__,
                                },
                                exc_info=True,
                            )
            
            # Toutes les tentatives ont échoué
            if last_exception:
                raise last_exception
            else:
                raise RuntimeError(f"{operation_name} failed after {max_retries} attempts")
        
        return wrapper
    return decorator


class RetryableOperation:
    """Classe helper pour opérations avec retry."""
    
    def __init__(
        self,
        max_retries: int,
        config: RetryConfig,
        operation_name: str = "operation",
    ):
        """
        Initialise une opération avec retry.
        
        Args:
            max_retries: Nombre maximum de tentatives.
            config: Configuration du retry.
            operation_name: Nom de l'opération.
        """
        self.max_retries = max_retries
        self.config = config
        self.operation_name = operation_name
    
    def execute(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Exécute une fonction avec retry.
        
        Args:
            func: Fonction à exécuter.
            *args: Arguments positionnels.
            **kwargs: Arguments nommés.
        
        Returns:
            Résultat de la fonction.
        
        Raises:
            Exception: Si toutes les tentatives échouent.
        """
        last_exception: Optional[Exception] = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                
                if attempt > 1:
                    logger.info(
                        f"{self.operation_name} succeeded after {attempt} attempts",
                        extra={"operation": self.operation_name, "retry_count": attempt},
                    )
                
                return result
                
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    delay = calculate_backoff_delay(
                        attempt,
                        self.config.delay_base,
                        self.config.delay_max,
                        self.config.backoff_multiplier,
                    )
                    
                    logger.warning(
                        f"{self.operation_name} failed (attempt {attempt}/{self.max_retries}): {str(e)}. "
                        f"Retrying in {delay:.2f}s...",
                        extra={
                            "operation": self.operation_name,
                            "retry_count": attempt,
                            "error_type": type(e).__name__,
                        },
                    )
                    
                    time.sleep(delay)
                else:
                    logger.error(
                        f"{self.operation_name} failed after {self.max_retries} attempts: {str(e)}",
                        extra={
                            "operation": self.operation_name,
                            "retry_count": attempt,
                            "error_type": type(e).__name__,
                        },
                        exc_info=True,
                    )
        
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"{self.operation_name} failed after {self.max_retries} attempts")

