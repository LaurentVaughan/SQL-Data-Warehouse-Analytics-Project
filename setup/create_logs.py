"""
=============================================================
Logging infrastructure module for audit and process tracking.
=============================================================

Provides SQLAlchemy-based logging infrastructure for the medallion architecture.
Creates comprehensive logging tables for audit trails, process tracking, error
logging, performance monitoring, and data lineage.

This module replaces the original SQL script with object-oriented Python code
that provides better type safety, error handling, and integration with Python
logging frameworks.

Logging Infrastructure Components:
    - ProcessLog: Track ETL process execution and metrics
    - ConfigurationLog: Audit configuration changes
    - ErrorLog: Centralized error logging with context
    - PerformanceMetrics: Performance monitoring and optimization
    - DataLineage: Track data flow through medallion layers

Key Features:
    - Type-safe table definitions with SQLAlchemy ORM
    - Object-oriented approach to logging operations
    - Better error handling and validation
    - Integration with Python logging frameworks
    - Support for automated testing and mocking
    - Uses sql/query_builder for metadata queries

Prerequisites:
    - Warehouse database exists
    - logs schema exists (created by create_schemas.py)
    - Connection with table creation privileges
    - SQLAlchemy and psycopg2 dependencies

Example:
    >>> from setup.create_logs import LoggingInfrastructure
    >>> 
    >>> logs = LoggingInfrastructure(
    ...     host='localhost',
    ...     user='postgres',
    ...     password='password',
    ...     database='warehouse'
    ... )
    >>> 
    >>> # Create all logging tables
    >>> results = logs.create_all_tables()
    >>> 
    >>> # Log a process
    >>> process_id = logs.log_process_start('bronze_ingestion', 'Load CRM data')
    >>> logs.log_process_end(process_id, 'SUCCESS', rows_processed=1000)
"""

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

# Import ORM models from centralized models package
from models.logs_models import (
    Base,
    ConfigurationLog,
    DataLineage,
    ErrorLog,
    PerformanceMetrics,
    ProcessLog,
)

# Get logger for this module (don't configure root logger)
logger = logging.getLogger(__name__)


class LoggingInfrastructureError(Exception):
    """Exception raised for logging infrastructure operation errors.
    
    Raised when logging table creation, initialization, or operations fail.
    """
    pass


# Note: ORM models (ProcessLog, ErrorLog, DataLineage, PerformanceMetrics, ConfigurationLog)
# have been moved to models/logs_models.py to prevent circular imports.
# They are imported at the top of this file from models.logs_models.


class LoggingInfrastructure:
    """
    SQLAlchemy-based logging infrastructure utility.
    
    Replaces the functionality of setup/create_logs.sql with Python code
    that provides better integration with application logging frameworks.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """
        Initialize the logging infrastructure.
        
        Args:
            host: PostgreSQL server hostname
            port: PostgreSQL server port
            user: Database user with table creation privileges
            password: Database password
            database: Target database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine connected to target database."""
        if self._engine is None:
            # Use modern URL.create() for robust connection string building
            try:
                connection_url = URL.create(
                    drivername='postgresql',
                    username=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database
                )
                self._engine = create_engine(connection_url, echo=False)
            except AttributeError:
                # Fallback for older SQLAlchemy versions
                connection_string = (
                    f"postgresql://{self.user}:{quote_plus(self.password)}"
                    f"@{self.host}:{self.port}/{self.database}"
                )
                self._engine = create_engine(connection_string, echo=False)
        return self._engine
    
    @contextmanager
    def _get_session(self):
        """Get SQLAlchemy session with proper resource management."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self._get_engine())
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def create_all_tables(self) -> Dict[str, bool]:
        """
        Create all logging infrastructure tables.
        
        Returns:
            Dictionary mapping table names to creation status
        """
        logger.info("🚀 Creating logging infrastructure tables...")
        
        try:
            engine = self._get_engine()
            
            # Create all tables
            Base.metadata.create_all(engine, checkfirst=True)
            
            # Get list of created tables
            created_tables = [
                'process_log',
                'error_log', 
                'data_lineage',
                'performance_metrics',
                'configuration_log'
            ]
            
            results = {table: True for table in created_tables}
            
            logger.info(f"✅ Created {len(created_tables)} logging tables")
            logger.info("✅ Logging infrastructure setup completed successfully")
            
            return results
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create logging tables: {e}")
            raise LoggingInfrastructureError(f"Failed to create logging tables: {e}")
    
    def log_process_start(
        self,
        process_name: str,
        process_description: str = None,
        source_system: str = None,
        target_layer: str = None,
        created_by: str = 'system',
        metadata: Dict[str, Any] = None
    ) -> int:
        """
        Log the start of a process.
        
        Args:
            process_name: Name of the process
            process_description: Description of what the process does
            source_system: Source system identifier
            target_layer: Target medallion layer
            created_by: User or system initiating the process
            metadata: Additional metadata as dictionary (will be stored as JSONB)
            
        Returns:
            Process log ID for tracking
        """
        try:
            with self._get_session() as session:
                process_log = ProcessLog(
                    process_name=process_name,
                    process_description=process_description,
                    source_system=source_system,
                    target_layer=target_layer,
                    created_by=created_by,
                    process_metadata=metadata  # Pass dict directly to JSONB column
                )
                
                session.add(process_log)
                session.flush()  # Get the ID before commit
                
                log_id = process_log.log_id
                
                logger.info(f"Started process '{process_name}' with log_id {log_id}")
                return log_id
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to log process start: {e}")
            raise LoggingInfrastructureError(f"Failed to log process start: {e}")
    
    def log_process_end(
        self,
        log_id: int,
        status: str,
        rows_processed: int = None,
        rows_inserted: int = None,
        rows_updated: int = None,
        rows_deleted: int = None,
        error_message: str = None
    ) -> None:
        """
        Log the completion of a process.
        
        Args:
            log_id: Process log ID from log_process_start
            status: Final status (SUCCESS, FAILED, CANCELLED)
            rows_processed: Number of rows processed
            rows_inserted: Number of rows inserted
            rows_updated: Number of rows updated
            rows_deleted: Number of rows deleted
            error_message: Error message if status is FAILED
        """
        try:
            with self._get_session() as session:
                process_log = session.query(ProcessLog).filter_by(log_id=log_id).first()
                if not process_log:
                    raise LoggingInfrastructureError(f"Process log with ID {log_id} not found")
                
                # Use datetime.now() for Python attribute assignment
                process_log.end_time = datetime.now()
                process_log.status = status
                process_log.rows_processed = rows_processed
                process_log.rows_inserted = rows_inserted
                process_log.rows_updated = rows_updated
                process_log.rows_deleted = rows_deleted
                process_log.error_message = error_message
                
                logger.info(f"Completed process '{process_log.process_name}' with status {status}")
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to log process end: {e}")
            raise LoggingInfrastructureError(f"Failed to log process end: {e}")
    
    def log_error(
        self,
        process_log_id: int,
        error_message: str,
        error_level: str = 'ERROR',
        error_code: str = None,
        error_detail: str = None,
        table_name: str = None,
        column_name: str = None,
        row_context: str = None,
        recovery_suggestion: str = None
    ) -> int:
        """
        Log an error with detailed context.
        
        Args:
            process_log_id: Associated process log ID
            error_message: Human-readable error message
            error_level: Error severity level
            error_code: Application-specific error code
            error_detail: Detailed error information
            table_name: Table involved in the error
            column_name: Column involved in the error
            row_context: Row data context
            recovery_suggestion: Suggested resolution steps
            
        Returns:
            Error log ID
        """
        try:
            with self._get_session() as session:
                error_log = ErrorLog(
                    process_log_id=process_log_id,
                    error_level=error_level,
                    error_code=error_code,
                    error_message=error_message,
                    error_detail=error_detail,
                    table_name=table_name,
                    column_name=column_name,
                    row_context=row_context,
                    recovery_suggestion=recovery_suggestion
                )
                
                session.add(error_log)
                session.flush()
                
                error_id = error_log.error_id
                
                logger.error(f"Logged error {error_id}: {error_message}")
                return error_id
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to log error: {e}")
            raise LoggingInfrastructureError(f"Failed to log error: {e}")
    
    def log_data_lineage(
        self,
        process_log_id: int,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        source_column: str = None,
        target_column: str = None,
        transformation_logic: str = None,
        record_count: int = None
    ) -> int:
        """
        Log data lineage information.
        
        Args:
            process_log_id: Associated process log ID
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
            source_column: Source column name (optional)
            target_column: Target column name (optional)
            transformation_logic: Description of transformation
            record_count: Number of records involved
            
        Returns:
            Data lineage ID
        """
        try:
            with self._get_session() as session:
                lineage = DataLineage(
                    process_log_id=process_log_id,
                    source_schema=source_schema,
                    source_table=source_table,
                    source_column=source_column,
                    target_schema=target_schema,
                    target_table=target_table,
                    target_column=target_column,
                    transformation_logic=transformation_logic,
                    record_count=record_count
                )
                
                session.add(lineage)
                session.flush()
                
                lineage_id = lineage.lineage_id
                
                logger.info(f"Logged lineage {lineage_id}: {source_schema}.{source_table} -> {target_schema}.{target_table}")
                return lineage_id
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to log data lineage: {e}")
            raise LoggingInfrastructureError(f"Failed to log data lineage: {e}")
    
    def get_process_status(self, log_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the current status of a process.
        
        Args:
            log_id: Process log ID
            
        Returns:
            Dictionary with process status information
        """
        try:
            with self._get_session() as session:
                process_log = session.query(ProcessLog).filter_by(log_id=log_id).first()
                if not process_log:
                    return None
                
                result = {
                    'log_id': process_log.log_id,
                    'process_name': process_log.process_name,
                    'status': process_log.status,
                    'start_time': process_log.start_time,
                    'end_time': process_log.end_time,
                    'rows_processed': process_log.rows_processed,
                    'error_message': process_log.error_message
                }
                
                return result
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get process status: {e}")
            raise LoggingInfrastructureError(f"Failed to get process status: {e}")
    
    def close_connections(self) -> None:
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


def main():
    """
    Main function for command-line usage.
    Equivalent to running the original create_logs.sql script.
    """
    import os

    # Configure logging for CLI usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Get database credentials from environment or use defaults
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'sql_retail_analytics_warehouse')
    }
    
    try:
        logs = LoggingInfrastructure(**db_config)
        
        # Create all logging tables
        results = logs.create_all_tables()
        
        print("\n✅ Logging infrastructure created successfully!")
        
        print("\nCreated Tables:")
        for table_name, was_created in results.items():
            status = "Created" if was_created else "Already existed"
            print(f"  logs.{table_name}: {status}")
        
        print(f"\nNext steps:")
        print(f"1. Seed configuration: python -m setup.seed.seed_configuration")
        print(f"2. Continue with bronze layer DDL")
        print(f"3. Start using logging in your ETL processes")
        
        # Example usage
        print(f"\nExample usage in Python:")
        print(f"  process_id = logs.log_process_start('my_process', 'Process description')")
        print(f"  # ... do work ...")
        print(f"  logs.log_process_end(process_id, 'SUCCESS', rows_processed=1000)")
        
    except LoggingInfrastructureError as e:
        logger.error(f"Logging infrastructure creation failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    finally:
        if 'logs' in locals():
            logs.close_connections()
    
    return 0


if __name__ == '__main__':
    exit(main())