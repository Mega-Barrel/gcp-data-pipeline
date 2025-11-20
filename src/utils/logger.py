"""
logger_setup.py

A reusable logging utility for Python applications.
This module configures a file-only logger that writes logs
to a structured log directory without printing anything
to console or stdout.

Features:
- Auto-creates `logs/` folder if missing
- Generates log files with module name + date
- Prevents duplicate handlers on repeated imports
- Easy plug-and-play across projects (Airflow, FastAPI, ETL scripts, etc.)

Example:
    from logger_setup import setup_logger
    logger = setup_logger()
    logger.info("Logger initialized successfully.")
"""

import logging
import os
from datetime import datetime

def setup_logger(name="data-pipeline", level=logging.INFO, log_dir="logs"):
    """
    Sets up a file-only logger (no console output).

    Args:
        name (str): Logger name (usually __name__).
        level (int): Logging level.
        log_dir (str): Directory to store log files.

    Returns:
        logging.Logger: Configured logger instance.
    """

    # Create logs directory if not exists
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    # Prevent adding multiple handlers on re-import
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # File path: logs/module_2025-11-20.log
    log_file = os.path.join(
        log_dir, f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log"
    )

    # File handler only
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
