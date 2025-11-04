"""
===================================================
Core infrastructure package for the data warehouse.
===================================================

This package provides centralized configuration management and logging
infrastructure used throughout the data warehouse application.

Modules:
    config: Configuration management from environment variables
    logger: Centralized logging configuration and utilities

Example:
    >>> from core.config import config
    >>> from core.logger import get_logger
    >>> 
    >>> logger = get_logger(__name__)
    >>> logger.info(f"Connecting to {config.db_host}")
"""

__version__ = "0.1.0"
__all__ = ['get_logger', 'setup_logging', 'get_module_logger', 'config', 'Config']

from core.config import Config, config
from core.logger import get_logger, get_module_logger, setup_logging
