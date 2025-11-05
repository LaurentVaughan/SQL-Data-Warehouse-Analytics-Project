"""
==================================================
Database creation module for data warehouse setup.
==================================================

Provides core database creation functionality using SQL utilities from the
sql/ package. Focuses solely on database creation and dropping operations
without orchestration logic (orchestration handled by setup_orchestrator.py).

This module uses psycopg2 for low-level database operations that require
connection to the admin database (typically 'postgres') rather than the
target database.

Key Features:
    - Database creation with configurable encoding and collation
    - Connection termination for database dropping
    - Database existence checking
    - Uses SQL from sql.ddl for all DDL generation
    - No orchestration or verification (moved to utils/)

Prerequisites:
    - PostgreSQL 13+ installed and running
    - SQLAlchemy and psycopg2 dependencies
    - Superuser or CREATE DATABASE privileges
    - Connection to admin database (NOT target database)

Example:
    >>> from setup.create_database import DatabaseCreator
    >>> 
    >>> creator = DatabaseCreator(
    ...     host='localhost',
    ...     port=5432,
    ...     user='postgres',
    ...     password='password',
    ...     admin_db='postgres',
    ...     target_db='warehouse'
    ... )
    >>> 
    >>> # Create database
    >>> if creator.create_database():
    ...     print("Database created successfully")
    >>> 
    >>> # Drop database
    >>> creator.drop_database()
"""

import logging
import time
from typing import Optional
from urllib.parse import quote_plus

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

from sql.ddl import create_database_sql, drop_database_sql, terminate_connections_sql
from sql.query_builder import check_database_exists_sql, count_database_connections_sql

logger = logging.getLogger(__name__)


class DatabaseCreationError(Exception):
    """Exception raised for database creation operation errors.
    
    Raised when database creation, dropping, or connection operations fail.
    """
    pass


class DatabaseCreator:
    """Core database creation and management utility.
    
    Handles low-level PostgreSQL database creation and dropping operations
    using psycopg2 for admin-level database operations. All SQL generation
    delegated to sql.ddl module for consistency.
    
    Attributes:
        host: PostgreSQL server hostname
        port: PostgreSQL server port
        user: Database username with CREATE DATABASE privileges
        password: Database password
        admin_db: Admin database name (typically 'postgres')
        target_db: Name of database to create/drop
        template: Template database for creation
        encoding: Character encoding (default 'UTF8')
        lc_collate: Collation order
        lc_ctype: Character classification
    
    Example:
        >>> creator = DatabaseCreator(
        ...     host='localhost',
        ...     port=5432,
        ...     user='postgres',
        ...     password='pwd',
        ...     admin_db='postgres',
        ...     target_db='warehouse'
        ... )
        >>> creator.create_database()
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        admin_db: str,
        target_db: str,
        template: str = 'template0',
        encoding: str = 'UTF8',
        lc_collate: str = 'en_GB.UTF-8',
        lc_ctype: str = 'en_GB.UTF-8'
    ):
        """Initialize database creator with connection parameters.
        
        Args:
            host: PostgreSQL server hostname or IP address
            port: PostgreSQL server port number
            user: Database user with CREATE DATABASE privileges
            password: Database password
            admin_db: Admin database name (typically 'postgres')
            target_db: Name of database to create or manage
            template: Template database for creation (default 'template0')
            encoding: Character encoding (default 'UTF8')
            lc_collate: Collation order (default 'en_GB.UTF-8')
            lc_ctype: Character classification (default 'en_GB.UTF-8')
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.admin_db = admin_db
        self.target_db = target_db
        
        # Database configuration
        self.db_config = {
            'template': template,
            'encoding': encoding,
            'lc_collate': lc_collate,
            'lc_ctype': lc_ctype
        }
        
        self._admin_engine: Optional[Engine] = None
        
    def _get_admin_engine(self) -> Engine:
        """Get SQLAlchemy engine connected to admin database."""
        if self._admin_engine is None:
            # Use modern URL.create() for robust connection string building
            try:
                connection_url = URL.create(
                    drivername='postgresql',
                    username=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.admin_db
                )
                self._admin_engine = create_engine(
                    connection_url,
                    isolation_level='AUTOCOMMIT',
                    echo=False
                )
            except AttributeError:
                # Fallback for older SQLAlchemy versions
                connection_string = (
                    f"postgresql://{self.user}:{quote_plus(self.password)}"
                    f"@{self.host}:{self.port}/{self.admin_db}"
                )
                self._admin_engine = create_engine(
                    connection_string,
                    isolation_level='AUTOCOMMIT',
                    echo=False
                )
        return self._admin_engine
    
    def check_database_exists(self) -> bool:
        """
        Check if target database exists.
        
        Returns:
            True if database exists, False otherwise
        """
        try:
            engine = self._get_admin_engine()
            with engine.connect() as conn:
                # Use SQL from query_builder module
                sql = check_database_exists_sql(self.target_db)
                result = conn.execute(text(sql))
                return result.fetchone() is not None
                
        except SQLAlchemyError as e:
            logger.error(f"Error checking database existence: {e}")
            raise DatabaseCreationError(f"Failed to check database existence: {e}")
    
    def terminate_connections(self) -> int:
        """
        Terminate all active connections to target database.
        
        Returns:
            Number of connections terminated
        """
        if not self.check_database_exists():
            logger.info(f"Database {self.target_db} does not exist")
            return 0
            
        try:
            engine = self._get_admin_engine()
            with engine.connect() as conn:
                # Count connections using SQL from query_builder
                count_sql = count_database_connections_sql(self.target_db)
                count_result = conn.execute(text(count_sql))
                connection_count = count_result.scalar()
                
                if connection_count == 0:
                    logger.info(f"No active connections to {self.target_db}")
                    return 0
                
                # Terminate using SQL from ddl module
                logger.info(f"Terminating {connection_count} connections to {self.target_db}")
                terminate_sql = terminate_connections_sql(self.target_db)
                conn.execute(text(terminate_sql))
                conn.commit()
                
                return connection_count
                
        except SQLAlchemyError as e:
            logger.error(f"Error terminating connections: {e}")
            raise DatabaseCreationError(f"Failed to terminate connections: {e}")
    
    def drop_database(self, force: bool = True) -> bool:
        """
        Drop target database if it exists.
        
        Args:
            force: Use WITH (FORCE) for PostgreSQL 13+
            
        Returns:
            True if database was dropped, False if didn't exist
        """
        if not self.check_database_exists():
            logger.info(f"Database {self.target_db} does not exist")
            return False
            
        try:
            # Terminate connections first
            self.terminate_connections()
            
            # Get DROP SQL from ddl module
            drop_sql = drop_database_sql(
                database_name=self.target_db,
                if_exists=True,
                force=force
            )
            
            # Execute using raw psycopg2
            engine = self._get_admin_engine()
            with engine.connect() as conn:
                raw_conn = conn.connection.driver_connection
                raw_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                
                with raw_conn.cursor() as cursor:
                    logger.info(f"Dropping database {self.target_db}")
                    cursor.execute(drop_sql)
                    
            logger.info(f"Successfully dropped database {self.target_db}")
            return True
            
        except (SQLAlchemyError, psycopg2.Error) as e:
            logger.error(f"Error dropping database: {e}")
            raise DatabaseCreationError(f"Failed to drop database: {e}")
    
    def create_database(self) -> None:
        """
        Create target database with configured settings.
        
        Raises:
            DatabaseCreationError: If creation fails
        """
        try:
            # Get CREATE DATABASE SQL from ddl module
            create_sql = create_database_sql(
                database_name=self.target_db,
                template=self.db_config['template'],
                encoding=self.db_config['encoding'],
                lc_collate=self.db_config['lc_collate'],
                lc_ctype=self.db_config['lc_ctype']
            )
            
            # Execute using raw psycopg2
            engine = self._get_admin_engine()
            with engine.connect() as conn:
                raw_conn = conn.connection.driver_connection
                raw_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                
                with raw_conn.cursor() as cursor:
                    logger.info(f"Creating database {self.target_db}")
                    cursor.execute(create_sql)
                    
            logger.info(f"Successfully created database {self.target_db}")
            
            # Brief wait for catalog refresh
            time.sleep(1)
            
        except (SQLAlchemyError, psycopg2.Error) as e:
            logger.error(f"Error creating database: {e}")
            raise DatabaseCreationError(f"Failed to create database: {e}")
    
    def close_connections(self) -> None:
        """Close all database connections."""
        if self._admin_engine:
            self._admin_engine.dispose()
            self._admin_engine = None
