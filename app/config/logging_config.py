"""
Logging configuration for Excel ETL pipeline.

Configures application-wide logging with:
    - Console output for real-time monitoring
    - File output with rotation for persistent logs
    - Structured log format with timestamps and context
    - Separate log levels per handler
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime


def setup_logging(log_level: str = "INFO", enable_file_logging: bool = True) -> None:
    """Configure application logging with console and optional file handlers.

    Args:
        log_level: Minimum log level to capture (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        enable_file_logging: If True, logs are also written to rotating log files.

    Logging structure:
        - Console: Colorized output for immediate feedback
        - File: Rotating log files in logs/ directory (10MB per file, 5 backups)

    Log format:
        %(asctime)s - %(name)s - %(levelname)s - %(message)s

    Example:
        >>> from app.config.logging_config import setup_logging
        >>> setup_logging(log_level="DEBUG", enable_file_logging=True)
        >>> import logging
        >>> logger = logging.getLogger(__name__)
        >>> logger.info("Pipeline started")
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear existing handlers to avoid duplicates
    if root_logger.handlers:
        root_logger.handlers.clear()

    # Define log format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Console handler (always enabled)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (optional, with rotation)
    if enable_file_logging:
        logs_dir = Path(__file__).resolve().parent.parent.parent / "logs"
        logs_dir.mkdir(exist_ok=True)

        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = logs_dir / f"etl_pipeline_{timestamp}.log"

        # Rotating file handler (10MB max, 5 backups)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        root_logger.info(f"File logging enabled: {log_file}")

    root_logger.info(f"Logging configured at {log_level} level")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module.

    Args:
        name: Module name (typically __name__).

    Returns:
        Configured logger instance.

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Starting data extraction")
    """
    return logging.getLogger(name)


# Optional: Configure logging on module import with defaults
# Uncomment the following line to auto-configure logging when this module is imported:
# setup_logging(log_level=os.getenv("LOG_LEVEL", "INFO"), enable_file_logging=True)
