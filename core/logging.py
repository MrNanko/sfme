#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/8 22:46
# @Author     : @MrNanko
# @File       : logging
# @Software   : PyCharm
# @Description: Advanced logging configuration with console and file rotation support

import logging
import sys
import threading
from pathlib import Path
from typing import Optional, Union
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

# Lock and flag to prevent duplicate configuration
_config_lock = threading.Lock()
_configured = False

def setup_logging(
        log_dir: str = "logs",
        log_filename: str = "app.log",
        level: Union[int, str] = logging.INFO,
        when: Optional[str] = 'midnight',
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
        force_reconfigure: bool = False
):
    """
    Global logging configuration. Sets up root logger handlers with support for console and file rotation.

    Args:
        log_dir: Log directory path
        log_filename: Log filename, defaults to app.log
        level: Logging level, supports both integer and string values (e.g., 'INFO', 'DEBUG')
        when: If set, uses TimedRotatingFileHandler; if None, uses RotatingFileHandler
        max_bytes: Maximum file size for rotation (default 10MB, only effective when when=None)
        backup_count: Number of backup log files to keep
        force_reconfigure: Whether to force reconfiguration even if already configured
    """
    global _configured

    # Prevent duplicate configuration (unless forced)
    if _configured and not force_reconfigure:
        return

    with _config_lock:
        if _configured and not force_reconfigure:
            return

        # Handle string level
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)

        # ===== Critical fix: Complete logging system cleanup =====

        # 1. Disable lastResort handler
        logging.lastResort = None

        # 2. Clean up root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # Close and remove all handlers
        for handler in root_logger.handlers[:]:
            try:
                handler.close()
            except Exception:
                pass
            root_logger.removeHandler(handler)

        # 3. Clean up all existing named loggers
        logger_dict = logging.Logger.manager.loggerDict
        for name in list(logger_dict.keys()):
            logger_obj = logger_dict[name]
            if isinstance(logger_obj, logging.Logger):
                # Clean up child logger handlers
                for handler in logger_obj.handlers[:]:
                    try:
                        handler.close()
                    except Exception:
                        pass
                    logger_obj.removeHandler(handler)

                # Ensure propagation settings are correct
                logger_obj.propagate = True

                # Reset level to inherit from root logger
                logger_obj.setLevel(logging.NOTSET)

        # 4. Reset log record factory
        logging.setLogRecordFactory(logging.LogRecord)

        # ===== Configure new logging system =====

        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # File handler
        if log_dir:
            try:
                log_path = Path(log_dir)
                log_path.mkdir(parents=True, exist_ok=True)
                log_file = log_path / log_filename

                if when:
                    file_handler = TimedRotatingFileHandler(
                        str(log_file),
                        when=when,
                        backupCount=backup_count,
                        encoding='utf-8'
                    )
                else:
                    file_handler = RotatingFileHandler(
                        str(log_file),
                        maxBytes=max_bytes,
                        backupCount=backup_count,
                        encoding='utf-8'
                    )

                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)

            except Exception as e:
                # If file handler setup fails, ensure at least console output works
                console_handler.setLevel(logging.WARNING)
                root_logger.warning(f"Failed to setup file logging: {e}")

        _configured = True


def reset_logging():
    """Reset logging configuration state to allow reconfiguration"""
    global _configured
    with _config_lock:
        _configured = False


def get_logger(name: str = None) -> logging.Logger:
    """
    Convenience function to get a logger instance

    Args:
        name: Logger name, defaults to the calling module's name

    Returns:
        logging.Logger: Logger instance
    """
    # Use default configuration if not yet configured
    if not _configured:
        setup_logging()

    logger = logging.getLogger(name)

    # Ensure named logger settings are correct
    if name and name != '':
        logger.propagate = True  # Ensure propagation to root logger
        logger.setLevel(logging.NOTSET)  # Inherit root logger level

        # Ensure no own handlers (avoid duplication)
        if logger.handlers:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

    return logger


def is_configured() -> bool:
    """Check if logging is configured"""
    return _configured