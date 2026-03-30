"""
log_config.py — Centralised logging configuration.

Usage:
    from log_config import setup_logging
    setup_logging()          # INFO to terminal
    setup_logging("DEBUG")   # verbose
"""
import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """
    Configure root logger to print to terminal (stdout).

    Args:
        level: Logging level string — DEBUG, INFO, WARNING, ERROR, CRITICAL.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level!r}")

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root = logging.getLogger()
    root.setLevel(numeric_level)
    root.handlers.clear()          # avoid duplicate handlers on re-calls
    root.addHandler(handler)
