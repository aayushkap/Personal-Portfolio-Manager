# core/logger.py

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

LOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "logs", "app.log"
)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

_file_handler = RotatingFileHandler(
    LOG_PATH, maxBytes=1_000_000, backupCount=0
)  # backupCount=0 : just delete
_file_handler.setFormatter(_fmt)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    if name is None:
        name = "app"
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.addHandler(_file_handler)
        logger.addHandler(_console_handler)
        logger.propagate = False
    return logger
