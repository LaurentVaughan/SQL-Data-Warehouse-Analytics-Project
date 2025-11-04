"""
========================================================
Setup package for medallion architecture data warehouse.
========================================================

This package provides SQLAlchemy-based setup utilities for initializing
the data warehouse with medallion architecture (bronze/silver/gold layers).

The setup package replaces the original SQL scripts with modular Python
implementations that provide better error handling, dependency management,
and integration with the centralized configuration system.

Modules:
    create_database: Database creation and connection management
    create_schemas: Medallion schema creation (bronze/silver/gold/logs)
    create_logs: Logging infrastructure and audit tables
    setup_orchestrator: Coordinated setup process

Architecture:
    - Configuration: Centralized in core/config.py (loads from .env)
    - SQL Generation: Modular functions in sql/ package
    - Logging: Audit and process tracking in logs/ package
    - No raw SQL: All SQL generated through sql.ddl and sql.dml modules

Example:
    >>> from setup import SetupOrchestrator
    >>> 
    >>> # Run complete setup
    >>> orchestrator = SetupOrchestrator()
    >>> results = orchestrator.run_complete_setup()
    >>> 
    >>> # Individual components
    >>> from setup import DatabaseCreator, SchemaCreator
    >>> db_creator = DatabaseCreator()
    >>> db_creator.create_database()

Requirements:
    - SQLAlchemy >= 2.0.0
    - psycopg2-binary >= 2.9.0
    - python-dotenv >= 1.0.0

Migration from SQL Scripts:
    - setup/create_db.sql → setup/create_database.py
    - setup/create_schemas.sql → setup/create_schemas.py
    - setup/create_logs.sql → setup/create_logs.py
"""

__version__ = "0.1.0"
__author__ = "Laurent's Architecture Team"
__all__ = [
    'SetupOrchestrator',
    'DatabaseCreator', 
    'SchemaCreator',
    'LoggingInfrastructure'
]

from .create_database import DatabaseCreator
from .create_logs import LoggingInfrastructure
from .create_schemas import SchemaCreator
from .setup_orchestrator import SetupOrchestrator
