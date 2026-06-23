"""
Shared logging utility - rotating file handler + console handler.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_logger(name: str, log_config: dict = None) -> logging.Logger:
    log_config = log_config or {}
    level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    log_dir = Path(log_config.get("log_dir", "./logs"))
    max_bytes = log_config.get("max_bytes", 5 * 1024 * 1024)
    backup_count = log_config.get("backup_count", 5)

    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured (avoid duplicate handlers on re-import)
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # Rotating file handler
    os.makedirs(log_dir, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_dir / f"{name}.log", maxBytes=max_bytes, backupCount=backup_count
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
