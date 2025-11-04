"""
===========================================================
Database utility functions for verification and inspection.
===========================================================

Provides utility functions for database verification, inspection, and
management using SQLAlchemy and PostgreSQL system catalogs.

Functions:
    get_engine: Create SQLAlchemy engine with connection parameters
    check_database_exists: Check if a database exists
    get_database_info: Get detailed database configuration
    verify_database_creation: Verify database creation with expected settings

Example:
    >>> from utils.database_utils import verify_database_creation
    >>> 
    >>> db_info = verify_database_creation(
    ...     host='localhost',
    ...     port=5432,
    ...     user='postgres',
    ...     password='password',
    ...     admin_db='postgres',
    ...     target_db='warehouse',
    ...     expected_encoding='UTF8'
    ... )
    >>> print(f"Database size: {db_info['size']}")
"""

import logging
from typing import Any, Dict, Optional
from urllib.parse import quote_plus

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class DatabaseUtilsError(Exception):
    """Exception raised for database utility operation errors.
    
    Raised when database verification, inspection, or connection
    operations fail.
    """
    pass


def get_engine(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    isolation_level: Optional[str] = None
) -> Engine:
    """Create a SQLAlchemy engine for database connections.
    
    Args:
        host: Database server hostname or IP address
        port: Database server port number
        user: Database username
        password: Database password
        database: Database name to connect to
        isolation_level: SQL isolation level (e.g., 'AUTOCOMMIT')
        
    Returns:
        Configured SQLAlchemy Engine instance
        
    Example:
        >>> engine = get_engine('localhost', 5432, 'user', 'pwd', 'mydb')
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT 1"))
    """
    connection_string = (
        f"postgresql://{user}:{quote_plus(password)}"
        f"@{host}:{port}/{database}"
    )
    
    engine_kwargs = {'echo': False}
    if isolation_level:
        engine_kwargs['isolation_level'] = isolation_level
    
    return create_engine(connection_string, **engine_kwargs)


def check_database_exists(
    host: str,
    port: int,
    user: str,
    password: str,
    admin_db: str,
    target_db: str
) -> bool:
    """Check if a database exists.
    
    Connects to admin database and queries pg_database catalog to check
    if the target database exists.
    
    Args:
        host: Database server hostname
        port: Database server port
        user: Database username
        password: Database password
        admin_db: Admin database name (typically 'postgres')
        target_db: Target database name to check
        
    Returns:
        True if database exists, False otherwise
        
    Raises:
        DatabaseUtilsError: If connection or query fails
        
    Example:
        >>> exists = check_database_exists(
        ...     'localhost', 5432, 'postgres', 'pwd', 'postgres', 'warehouse'
        ... )
        >>> if exists:
        ...     print("Database found")
    """
    try:
        engine = get_engine(host, port, user, password, admin_db)
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": target_db}
            )
            exists = result.fetchone() is not None
        
        engine.dispose()
        return exists
        
    except SQLAlchemyError as e:
        logger.error(f"Error checking database existence: {e}")
        raise DatabaseUtilsError(f"Failed to check database existence: {e}")


def get_database_info(
    host: str,
    port: int,
    user: str,
    password: str,
    admin_db: str,
    target_db: str
) -> Dict[str, Any]:
    """Get detailed information about a database.
    
    Retrieves database configuration including encoding, collation,
    locale settings, and size from PostgreSQL system catalogs.
    
    Args:
        host: Database server hostname
        port: Database server port
        user: Database username
        password: Database password
        admin_db: Admin database name (typically 'postgres')
        target_db: Target database name
        
    Returns:
        Dictionary with keys:
            - database: Database name
            - encoding: Character encoding (e.g., 'UTF8')
            - lc_collate: Collation setting
            - lc_ctype: Character classification setting
            - size: Human-readable database size
            
    Raises:
        DatabaseUtilsError: If database not found or query fails
        
    Example:
        >>> info = get_database_info(
        ...     'localhost', 5432, 'postgres', 'pwd', 'postgres', 'warehouse'
        ... )
        >>> print(f"Encoding: {info['encoding']}, Size: {info['size']}")
    """
    try:
        engine = get_engine(host, port, user, password, admin_db)
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT 
                        datname,
                        pg_encoding_to_char(encoding) AS encoding,
                        datcollate AS lc_collate,
                        datctype AS lc_ctype,
                        pg_size_pretty(pg_database_size(datname)) AS size
                    FROM pg_database
                    WHERE datname = :db_name
                """),
                {"db_name": target_db}
            )
            
            row = result.fetchone()
            if not row:
                raise DatabaseUtilsError(f"Database {target_db} not found")
            
            db_info = {
                'database': row.datname,
                'encoding': row.encoding,
                'lc_collate': row.lc_collate,
                'lc_ctype': row.lc_ctype,
                'size': row.size
            }
        
        engine.dispose()
        return db_info
        
    except SQLAlchemyError as e:
        logger.error(f"Error getting database info: {e}")
        raise DatabaseUtilsError(f"Failed to get database info: {e}")


def verify_database_creation(
    host: str,
    port: int,
    user: str,
    password: str,
    admin_db: str,
    target_db: str,
    expected_encoding: str = 'UTF8',
    expected_collate: str = 'en_GB.UTF-8'
) -> Dict[str, Any]:
    """Verify database creation and return database details.
    
    Checks that database exists and optionally validates encoding and
    collation settings match expected values. Logs warnings for mismatches
    but does not raise errors.
    
    Args:
        host: Database server hostname
        port: Database server port
        user: Database username
        password: Database password
        admin_db: Admin database name (typically 'postgres')
        target_db: Target database name to verify
        expected_encoding: Expected character encoding (default 'UTF8')
        expected_collate: Expected collation (default 'en_GB.UTF-8')
        
    Returns:
        Dictionary containing database configuration details (see get_database_info)
        
    Raises:
        DatabaseUtilsError: If database doesn't exist or verification fails
        
    Example:
        >>> db_info = verify_database_creation(
        ...     host='localhost',
        ...     port=5432,
        ...     user='postgres',
        ...     password='password',
        ...     admin_db='postgres',
        ...     target_db='warehouse',
        ...     expected_encoding='UTF8',
        ...     expected_collate='en_GB.UTF-8'
        ... )
        >>> print(f"Database verified: {db_info['database']}")
    """
    try:
        # Check if database exists
        if not check_database_exists(host, port, user, password, admin_db, target_db):
            raise DatabaseUtilsError(f"Database {target_db} does not exist")
        
        # Get database info
        db_info = get_database_info(host, port, user, password, admin_db, target_db)
        
        # Verify encoding
        if db_info['encoding'] != expected_encoding:
            logger.warning(
                f"Encoding mismatch: expected {expected_encoding}, "
                f"got {db_info['encoding']}"
            )
        
        # Verify collation
        if db_info['lc_collate'] != expected_collate:
            logger.warning(
                f"Collation mismatch: expected {expected_collate}, "
                f"got {db_info['lc_collate']}"
            )
        
        logger.info(f"Database {target_db} verified successfully")
        return db_info
        
    except Exception as e:
        logger.error(f"Error verifying database creation: {e}")
        raise DatabaseUtilsError(f"Failed to verify database creation: {e}")
