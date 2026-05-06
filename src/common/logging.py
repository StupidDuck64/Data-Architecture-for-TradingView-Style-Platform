"""
Standardized logging setup for all data processing entry points.
"""

import logging


def setup_logging(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """Configure root logger and return a named logger."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)
