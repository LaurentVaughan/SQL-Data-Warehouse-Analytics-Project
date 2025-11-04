"""
=========================================================
Centralized logging configuration for the data warehouse.
=========================================================

Provides consistent logging setup across all modules with:
- File and console output
- Configurable log levels
- Colored console output with emojis
- Structured log formatting
- Module-specific loggers

Example:
    >>> from core.logger import get_logger, setup_logging
    >>> 
    >>> # Setup logging at application start
    >>> setup_logging(log_level='DEBUG', log_file='warehouse.log')
    >>> 
    >>> # Get module logger
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing started")
    >>> logger.error("An error occurred", exc_info=True)
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color support for console output.
    
    Adds ANSI color codes and emoji indicators to log messages for
    improved readability in terminal output.
    
    Attributes:
        COLORS: Dict mapping log levels to ANSI color codes
        EMOJI: Dict mapping log levels to emoji indicators
    """
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    EMOJI = {
        'DEBUG': '🔍',
        'INFO': 'ℹ️ ',
        'WARNING': '⚠️ ',
        'ERROR': '❌',
        'CRITICAL': '🔥'
    }
    
    def format(self, record):
        """Format log record with colors and emojis.
        
        Args:
            record: LogRecord instance to format
            
        Returns:
            Formatted log message string with ANSI colors and emoji
        """
        # Add color
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.COLORS['RESET']}"
            record.emoji = self.EMOJI.get(levelname, '')
        
        # Format the message
        result = super().format(record)
        return result


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Get a logger instance for the specified module.
    
    Args:
        name: Logger name (typically __name__ of calling module)
        level: Optional logging level override (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        
    Returns:
        Configured Logger instance
        
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Module initialized")
        >>> 
        >>> # With custom level
        >>> debug_logger = get_logger(__name__, level='DEBUG')
    """
    logger = logging.getLogger(name)
    
    if level:
        logger.setLevel(getattr(logging, level.upper()))
    
    return logger


def setup_logging(
    log_level: str = 'INFO',
    log_file: Optional[str] = None,
    log_dir: Optional[str] = None,
    console_output: bool = True,
    use_colors: bool = True
) -> None:
    """Setup centralized logging configuration.
    
    Configures the root logger with console and/or file handlers.
    Should be called once at application startup.
    
    Args:
        log_level: Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        log_file: Optional log file name (e.g., 'warehouse.log')
        log_dir: Optional log directory path (defaults to 'logs/')
        console_output: If True, output to console (stdout)
        use_colors: If True, use colored output for console
        
    Example:
        >>> # Basic setup
        >>> setup_logging(log_level='INFO')
        >>> 
        >>> # With file output
        >>> setup_logging(
        ...     log_level='DEBUG',
        ...     log_file='warehouse.log',
        ...     log_dir='logs',
        ...     use_colors=True
        ... )
    """
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        if use_colors:
            console_format = '%(emoji)s %(asctime)s - %(name)s - %(levelname)s - %(message)s'
            console_formatter = ColoredFormatter(
                console_format,
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            console_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            console_formatter = logging.Formatter(
                console_format,
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        # Create log directory if specified
        if log_dir:
            log_path = Path(log_dir)
        else:
            log_path = Path('logs')
        
        log_path.mkdir(parents=True, exist_ok=True)
        
        # Create file handler
        file_handler = logging.FileHandler(
            log_path / log_file,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        file_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        file_formatter = logging.Formatter(
            file_format,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)


def get_module_logger(module_name: str) -> logging.Logger:
    """Get a logger for a specific module with consistent formatting.
    
    Convenience function that calls get_logger() with the module name.
    
    Args:
        module_name: Name of the module (typically use __name__)
        
    Returns:
        Configured Logger instance
        
    Example:
        >>> logger = get_module_logger(__name__)
        >>> logger.debug("Debugging information")
    """
    return logging.getLogger(module_name)


def _init_default_logging():
    """Initialize default logging configuration if not already setup.
    
    Called automatically on module import to ensure basic logging
    is always available even if setup_logging() is not called explicitly.
    """
    if not logging.getLogger().handlers:
        setup_logging(
            log_level='INFO',
            console_output=True,
            use_colors=True
        )


# Auto-initialize on import
_init_default_logging()
