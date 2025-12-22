import logging
from collections import defaultdict
from typing import Any, Dict

logger = logging.getLogger("farm.services")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

_mismatch_counters: Dict[str, int] = defaultdict(int)


def trace_log(module: str, phase: str, details: Dict[str, Any]) -> None:
    """Log structured trace data at DEBUG level.
    
    Args:
        module: Module name (e.g., 'reconciliation', 'analysis')
        phase: Phase/function name (e.g., 'build_candidates', 'compute_expected')
        details: Dict of key-value pairs to log
    """
    details_str = ", ".join(f"{k}={v}" for k, v in details.items())
    logger.debug(f"[{module}:{phase}] {details_str}")


def increment_mismatch_counter(category: str) -> None:
    """Increment counter for a mismatch category."""
    _mismatch_counters[category] += 1


def get_mismatch_counters() -> Dict[str, int]:
    """Get current mismatch counters."""
    return dict(_mismatch_counters)


def reset_mismatch_counters() -> None:
    """Reset all mismatch counters."""
    _mismatch_counters.clear()
