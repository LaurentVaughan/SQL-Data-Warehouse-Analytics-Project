"""
=====================================================
Utility functions and helpers for the data warehouse.
=====================================================

This package provides reusable utility functions for database operations,
validation, and other common tasks across the data warehouse project.

Modules:
    database_utils: Database verification and utility functions

Example:
    >>> from utils import verify_database_creation, get_database_info
    >>> 
    >>> # Verify database was created correctly
    >>> db_info = verify_database_creation(
    ...     host='localhost',
    ...     port=5432,
    ...     user='postgres',
    ...     password='pwd',
    ...     admin_db='postgres',
    ...     target_db='warehouse'
    ... )
"""

__version__ = "0.1.0"
__all__ = [
    'verify_database_creation',
    'get_database_info'
]

from utils.database_utils import get_database_info, verify_database_creation
