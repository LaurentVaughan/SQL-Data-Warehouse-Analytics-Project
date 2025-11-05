"""
========================================
ORM Models for Data Warehouse
========================================

Centralized SQLAlchemy ORM model definitions for the medallion architecture.

This package contains all database table models, separated from setup/creation logic
to prevent circular imports and improve maintainability.

Modules:
    logs_models: Audit logging and process tracking models
    schema_models: (Future) Bronze/Silver/Gold layer table models

Architecture:
    - Models defined independently of setup logic
    - Used by both logs/ package (for operations) and setup/ package (for table creation)
    - Eliminates circular dependency between logs and setup packages

Example:
    >>> from models import ProcessLog, ConfigurationLog, ErrorLog
    >>> 
    >>> # Use models in logging operations
    >>> process_log = ProcessLog(
    ...     process_name='bronze_ingestion',
    ...     process_description='Load CRM data'
    ... )
    >>> 
    >>> # Use models in setup/table creation
    >>> from models.logs_models import Base
    >>> Base.metadata.create_all(engine)
"""

__version__ = "0.1.0"
__all__ = [
    # Logs schema models
    'ProcessLog',
    'ConfigurationLog',
    'ErrorLog',
    'PerformanceMetrics',
    'DataLineage',
    # Base for table creation
    'Base',
]

from .logs_models import (
    Base,
    ConfigurationLog,
    DataLineage,
    ErrorLog,
    PerformanceMetrics,
    ProcessLog,
)
