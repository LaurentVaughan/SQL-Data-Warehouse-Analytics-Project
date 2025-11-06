"""
==================================================
Database connectivity utilities for PostgreSQL.
==================================================

Provides reusable connection helpers, health checks, and database
availability verification for the data warehouse infrastructure.

This module abstracts PostgreSQL connection logic from business logic,
enabling consistent connection handling across setup, ETL, and monitoring.

Key Features:
    - Connection string building from config
    - Database availability checking
    - Connection pooling setup
    - Health check utilities
    - Timeout and retry logic

Example:
    >>> from utils.database_utils import (
    ...     check_database_available,
    ...     wait_for_database,
    ...     get_connection_string
    ... )
    >>> 
    >>> # Check if database is available
    >>> if check_database_available('localhost', 5432, 'postgres', 'password'):
    ...     print("Database ready")
    >>> 
    >>> # Wait for database with retries
    >>> wait_for_database('localhost', 5432, max_retries=5)
    >>> 
    >>> # Get connection string
    >>> conn_str = get_connection_string(use_warehouse=True)
"""

import logging
import time
from typing import Optional, Tuple
from urllib.parse import quote_plus

import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError

from core.config import config

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Exception raised when database connection fails."""
    pass


def get_connection_string(
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    use_warehouse: bool = False
) -> str:
    """
    Build PostgreSQL connection string.
    
    Args:
        host: Database hostname (defaults to config.db_host)
        port: Database port (defaults to config.db_port)
        user: Database user (defaults to config.db_user)
        password: Database password (defaults to config.db_password)
        database: Database name (defaults to config.db_name or warehouse_db_name)
        use_warehouse: If True, use warehouse database name from config
        
    Returns:
        PostgreSQL connection string
        
    Example:
        >>> conn_str = get_connection_string(use_warehouse=True)
        >>> # Returns: postgresql://user:pass@localhost:5432/warehouse_db
    """
    host = host if host is not None else config.db_host
    port = port if port is not None else config.db_port
    user = user if user is not None else config.db_user
    password = password if password is not None else config.db_password
    
    if database is None:
        database = config.warehouse_db_name if use_warehouse else config.db_name
    
    return f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}"


def create_sqlalchemy_engine(
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    use_warehouse: bool = False,
    echo: bool = False,
    pool_size: int = 5,
    max_overflow: int = 10
) -> Engine:
    """
    Create SQLAlchemy engine with connection pooling.
    
    Args:
        host: Database hostname
        port: Database port
        user: Database user
        password: Database password
        database: Database name
        use_warehouse: If True, use warehouse database
        echo: Enable SQL statement logging
        pool_size: Connection pool size
        max_overflow: Maximum overflow connections
        
    Returns:
        Configured SQLAlchemy Engine
        
    Example:
        >>> engine = create_sqlalchemy_engine(use_warehouse=True)
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT 1"))
    """
    try:
        # Use modern URL.create() for robust connection string building
        connection_url = URL.create(
            drivername='postgresql',
            username=user or config.db_user,
            password=password or config.db_password,
            host=host or config.db_host,
            port=port or config.db_port,
            database=database or (
                config.warehouse_db_name if use_warehouse else config.db_name
            )
        )
        
        return create_engine(
            connection_url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True  # Verify connections before using
        )
    except AttributeError:
        # Fallback for older SQLAlchemy versions
        conn_str = get_connection_string(host, port, user, password, database, use_warehouse)
        return create_engine(
            conn_str,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True
        )


def check_database_available(
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    timeout: int = 5
) -> bool:
    """
    Check if PostgreSQL database is available.
    
    Args:
        host: Database hostname (defaults to config)
        port: Database port (defaults to config)
        user: Database user (defaults to config)
        password: Database password (defaults to config)
        database: Database name (defaults to config.db_name)
        timeout: Connection timeout in seconds
        
    Returns:
        True if database is available, False otherwise
        
    Example:
        >>> if check_database_available():
        ...     print("PostgreSQL is ready")
    """
    host = host or config.db_host
    port = port or config.db_port
    user = user or config.db_user
    password = password or config.db_password
    database = database or config.db_name
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=timeout
        )
        conn.close()
        return True
    except OperationalError as e:
        logger.debug(f"Database not available: {e}")
        return False


def wait_for_database(
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    max_retries: int = 10,
    retry_delay: int = 2,
    timeout: int = 5
) -> bool:
    """
    Wait for PostgreSQL database to become available with retries.
    
    Args:
        host: Database hostname (defaults to config)
        port: Database port (defaults to config)
        user: Database user (defaults to config)
        password: Database password (defaults to config)
        database: Database name (defaults to config.db_name)
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        timeout: Connection timeout per attempt in seconds
        
    Returns:
        True if database became available, False if all retries exhausted
        
    Raises:
        DatabaseConnectionError: If database never becomes available
        
    Example:
        >>> wait_for_database(max_retries=5, retry_delay=3)
        >>> # Waits up to 15 seconds for database
    """
    host = host or config.db_host
    port = port or config.db_port
    database = database or config.db_name
    
    logger.info(f"Waiting for PostgreSQL at {host}:{port}/{database}...")
    
    for attempt in range(1, max_retries + 1):
        if check_database_available(host, port, user, password, database, timeout):
            logger.info(f"✅ PostgreSQL is available (attempt {attempt}/{max_retries})")
            return True
        
        if attempt < max_retries:
            logger.warning(
                f"⏳ PostgreSQL not available yet (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)
    
    error_msg = (
        f"PostgreSQL at {host}:{port}/{database} did not become available "
        f"after {max_retries} attempts"
    )
    logger.error(f"❌ {error_msg}")
    raise DatabaseConnectionError(error_msg)


def verify_database_exists(
    database_name: str,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None
) -> bool:
    """
    Verify if a specific database exists in PostgreSQL.
    
    Args:
        database_name: Name of database to check
        host: Database hostname (defaults to config)
        port: Database port (defaults to config)
        user: Database user (defaults to config)
        password: Database password (defaults to config)
        
    Returns:
        True if database exists, False otherwise
        
    Example:
        >>> if verify_database_exists('sql_retail_analytics_warehouse'):
        ...     print("Warehouse database exists")
    """
    try:
        # Connect to default 'postgres' database to check if target exists
        engine = create_sqlalchemy_engine(
            host=host,
            port=port,
            user=user,
            password=password,
            database='postgres',  # Connect to default database
            use_warehouse=False
        )
        
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": database_name}
            )
            exists = result.fetchone() is not None
        
        engine.dispose()
        return exists
        
    except Exception as e:
        logger.error(f"Failed to verify database existence: {e}")
        return False


def get_database_connection_info() -> dict:
    """
    Get current database connection configuration.
    
    Returns:
        Dictionary with connection parameters
        
    Example:
        >>> info = get_database_connection_info()
        >>> print(f"Connecting to {info['host']}:{info['port']}")
    """
    return {
        'host': config.db_host,
        'port': config.db_port,
        'user': config.db_user,
        'admin_database': config.db_name,
        'warehouse_database': config.warehouse_db_name
    }


def verify_connection() -> Tuple[bool, Optional[str]]:
    """
    Verify database connection and return status with details.
    
    Returns:
        Tuple of (success: bool, message: str)
        
    Example:
        >>> success, message = verify_connection()
        >>> if success:
        ...     print(f"✅ {message}")
        ... else:
        ...     print(f"❌ {message}")
    """
    try:
        if not check_database_available():
            return False, "PostgreSQL server not available"
        
        # Test warehouse database if it exists
        warehouse_exists = verify_database_exists(config.warehouse_db_name)
        
        if warehouse_exists:
            message = (
                f"Connected to PostgreSQL at {config.db_host}:{config.db_port}. "
                f"Warehouse database '{config.warehouse_db_name}' exists."
            )
        else:
            message = (
                f"Connected to PostgreSQL at {config.db_host}:{config.db_port}. "
                f"Warehouse database '{config.warehouse_db_name}' does not exist yet."
            )
        
        return True, message
        
    except Exception as e:
        return False, f"Connection test failed: {str(e)}"