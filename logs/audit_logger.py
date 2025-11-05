"""
===========================================================
Process and configuration audit logging for ETL operations.
===========================================================

Provides process execution logging and configuration change tracking that
integrates with the logging infrastructure created by setup.create_logs.

This module enables comprehensive audit trails for all ETL processes across
the medallion architecture, tracking start/end times, metrics, and configuration
changes for compliance and debugging purposes.

Classes:
    ProcessLogger: Log ETL process execution with start/end times and metrics
    ConfigurationLogger: Track configuration changes for audit purposes
    BatchLogger: Specialized logging for batch operations

Key Features:
    - Process lifecycle tracking (start/end/status)
    - Custom metrics recording (rows processed, execution time, etc.)
    - Configuration change auditing
    - Integration with logs.process_log table
    - SQLAlchemy ORM for type-safe operations

Example:
    >>> from logs.audit_logger import ProcessLogger, ConfigurationLogger
    >>> 
    >>> # Process logging
    >>> process_logger = ProcessLogger(
    ...     host='localhost',
    ...     user='postgres',
    ...     password='pwd',
    ...     database='warehouse'
    ... )
    >>> 
    >>> process_id = process_logger.start_process(
    ...     process_name='bronze_ingestion',
    ...     process_description='Load CRM data',
    ...     source_system='CRM',
    ...     target_layer='bronze'
    ... )
    >>> 
    >>> # ... do ETL work ...
    >>> 
    >>> process_logger.end_process(
    ...     log_id=process_id,
    ...     status='SUCCESS',
    ...     rows_processed=1000,
    ...     error_message=None
    ... )
    >>> 
    >>> # Configuration logging
    >>> config_logger = ConfigurationLogger()
    >>> config_logger.log_config_change(
    ...     parameter_name='batch_size',
    ...     old_value='1000',
    ...     new_value='5000',
    ...     change_reason='Performance optimization'
    ... )
"""

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

# Import ORM models from centralized models package (prevents circular imports)
from models.logs_models import (
    ConfigurationLog,
    DataLineage,
    ErrorLog,
    PerformanceMetrics,
    ProcessLog,
)

logger = logging.getLogger(__name__)


class AuditLoggerError(Exception):
    """Exception raised for audit logging operation errors.
    
    Raised when process logging, configuration logging, or database
    operations fail.
    """
    pass


class ProcessLogger:
    """Process execution logging for ETL operations.
    
    Provides methods to log process start/end, track custom metrics,
    and maintain comprehensive audit trails throughout the medallion
    architecture. Integrates with logs.process_log table.
    
    Attributes:
        host: PostgreSQL server hostname
        port: PostgreSQL server port
        user: Database username
        password: Database password
        database: Database name
    
    Example:
        >>> logger = ProcessLogger(
        ...     host='localhost',
        ...     user='postgres',
        ...     password='pwd',
        ...     database='warehouse'
        ... )
        >>> 
        >>> # Start process
        >>> pid = logger.start_process('bronze_load', 'Load customer data')
        >>> 
        >>> # End process
        >>> logger.end_process(pid, 'SUCCESS', rows_processed=1000)
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the process logger with connection parameters.
        
        Args:
            host: PostgreSQL server hostname
            port: PostgreSQL server port number
            user: Database username
            password: Database password
            database: Database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine."""
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
    
    def start_process(
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
            raise AuditLoggerError(f"Failed to log process start: {e}")
    
    def end_process(
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
            log_id: Process log ID from start_process
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
                    raise AuditLoggerError(f"Process log with ID {log_id} not found")
                
                # Use UTC for audit logs
                process_log.end_time = datetime.now(timezone.utc)
                process_log.status = status
                process_log.rows_processed = rows_processed
                process_log.rows_inserted = rows_inserted
                process_log.rows_updated = rows_updated
                process_log.rows_deleted = rows_deleted
                process_log.error_message = error_message
                
                logger.info(f"Completed process '{process_log.process_name}' with status {status}")
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to log process end: {e}")
            raise AuditLoggerError(f"Failed to log process end: {e}")
    
    def update_process_metrics(
        self,
        log_id: int,
        rows_processed: int = None,
        rows_inserted: int = None,
        rows_updated: int = None,
        rows_deleted: int = None
    ) -> None:
        """
        Update process metrics during execution.
        
        Args:
            log_id: Process log ID
            rows_processed: Updated rows processed count
            rows_inserted: Updated rows inserted count
            rows_updated: Updated rows updated count
            rows_deleted: Updated rows deleted count
        """
        try:
            with self._get_session() as session:
                process_log = session.query(ProcessLog).filter_by(log_id=log_id).first()
                if not process_log:
                    raise AuditLoggerError(f"Process log with ID {log_id} not found")
                
                if rows_processed is not None:
                    process_log.rows_processed = rows_processed
                if rows_inserted is not None:
                    process_log.rows_inserted = rows_inserted
                if rows_updated is not None:
                    process_log.rows_updated = rows_updated
                if rows_deleted is not None:
                    process_log.rows_deleted = rows_deleted
                
                logger.debug(f"Updated metrics for process {log_id}")
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to update process metrics: {e}")
            raise AuditLoggerError(f"Failed to update process metrics: {e}")
    
    def get_active_processes(self) -> List[Dict[str, Any]]:
        """
        Get list of currently running processes.
        
        Returns:
            List of active process dictionaries
        """
        try:
            with self._get_session() as session:
                active_processes = session.query(ProcessLog).filter_by(status='RUNNING').all()
                
                results = []
                for process in active_processes:
                    results.append({
                        'log_id': process.log_id,
                        'process_name': process.process_name,
                        'start_time': process.start_time,
                        'source_system': process.source_system,
                        'target_layer': process.target_layer,
                        'created_by': process.created_by
                    })
                
                return results
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get active processes: {e}")
            raise AuditLoggerError(f"Failed to get active processes: {e}")
    
    def get_process_history(
        self,
        process_name: str = None,
        days: int = 7,
        status: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get process execution history.
        
        Args:
            process_name: Filter by process name
            days: Number of days to look back
            status: Filter by status
            
        Returns:
            List of process history records
        """
        try:
            with self._get_session() as session:
                query = session.query(ProcessLog)
                
                # Add filters - use UTC for consistent time filtering
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
                query = query.filter(ProcessLog.start_time >= cutoff_time)
                
                if process_name:
                    query = query.filter(ProcessLog.process_name == process_name)
                
                if status:
                    query = query.filter(ProcessLog.status == status)
                
                processes = query.order_by(ProcessLog.start_time.desc()).all()
                
                results = []
                for process in processes:
                    duration = None
                    if process.end_time:
                        duration = (process.end_time - process.start_time).total_seconds()
                    
                    results.append({
                        'log_id': process.log_id,
                        'process_name': process.process_name,
                        'status': process.status,
                        'start_time': process.start_time,
                        'end_time': process.end_time,
                        'duration_seconds': duration,
                        'rows_processed': process.rows_processed,
                        'source_system': process.source_system,
                        'target_layer': process.target_layer
                    })
                
                return results
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get process history: {e}")
            raise AuditLoggerError(f"Failed to get process history: {e}")
    
    def close_connections(self) -> None:
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


class ConfigurationLogger:
    """
    Handles configuration change logging for audit purposes.
    
    Tracks changes to system configuration parameters with
    before/after values and change reasons.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the configuration logger."""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine."""
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
    
    def log_config_change(
        self,
        config_key: str,
        old_value: str,
        new_value: str,
        change_reason: str,
        changed_by: str,
        environment: str = 'production'
    ) -> int:
        """
        Log a configuration change.
        
        Args:
            config_key: Configuration parameter name
            old_value: Previous value
            new_value: New value
            change_reason: Reason for the change
            changed_by: User who made the change
            environment: Environment where change was made
            
        Returns:
            Configuration log ID
        """
        try:
            with self._get_session() as session:
                config_log = ConfigurationLog(
                    config_key=config_key,
                    old_value=old_value,
                    new_value=new_value,
                    change_reason=change_reason,
                    changed_by=changed_by,
                    environment=environment
                )
                
                session.add(config_log)
                session.flush()
                
                log_id = config_log.config_log_id
                
                logger.info(f"Logged configuration change for '{config_key}': {old_value} -> {new_value}")
                return log_id
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to log configuration change: {e}")
            raise AuditLoggerError(f"Failed to log configuration change: {e}")
    
    def get_config_history(
        self,
        config_key: str = None,
        days: int = 30,
        environment: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get configuration change history.
        
        Args:
            config_key: Filter by configuration key
            days: Number of days to look back
            environment: Filter by environment
            
        Returns:
            List of configuration change records
        """
        try:
            with self._get_session() as session:
                query = session.query(ConfigurationLog)
                
                # Add filters - use UTC for consistent time filtering
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
                query = query.filter(ConfigurationLog.change_timestamp >= cutoff_time)
                
                if config_key:
                    query = query.filter(ConfigurationLog.config_key == config_key)
                
                if environment:
                    query = query.filter(ConfigurationLog.environment == environment)
                
                changes = query.order_by(ConfigurationLog.change_timestamp.desc()).all()
                
                results = []
                for change in changes:
                    results.append({
                        'config_log_id': change.config_log_id,
                        'config_key': change.config_key,
                        'old_value': change.old_value,
                        'new_value': change.new_value,
                        'change_reason': change.change_reason,
                        'changed_by': change.changed_by,
                        'change_timestamp': change.change_timestamp,
                        'environment': change.environment
                    })
                
                return results
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get configuration history: {e}")
            raise AuditLoggerError(f"Failed to get configuration history: {e}")
    
    def close_connections(self) -> None:
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


class BatchLogger:
    """
    Specialized logger for batch operations with progress tracking.
    
    Provides enhanced logging for long-running batch processes
    with checkpoint and recovery capabilities.
    """
    
    def __init__(self, process_logger: ProcessLogger):
        """
        Initialize batch logger with a process logger instance.
        
        Args:
            process_logger: ProcessLogger instance
        """
        self.process_logger = process_logger
        self.current_batch_id = None
        self.current_process_id = None
        
    def start_batch(
        self,
        batch_name: str,
        total_records: int,
        batch_size: int,
        source_system: str = None,
        target_layer: str = None
    ) -> str:
        """
        Start a batch operation.
        
        Args:
            batch_name: Name of the batch operation
            total_records: Total number of records to process
            batch_size: Size of each batch
            source_system: Source system identifier
            target_layer: Target medallion layer
            
        Returns:
            Batch ID for tracking
        """
        # Use UTC for batch timestamps
        self.current_batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        metadata = {
            'batch_id': self.current_batch_id,
            'total_records': total_records,
            'batch_size': batch_size,
            'estimated_batches': (total_records + batch_size - 1) // batch_size
        }
        
        self.current_process_id = self.process_logger.start_process(
            process_name=batch_name,
            process_description=f"Batch processing {total_records} records in batches of {batch_size}",
            source_system=source_system,
            target_layer=target_layer,
            metadata=metadata  # Will be stored as JSONB dict
        )
        
        logger.info(f"Started batch '{batch_name}' with ID {self.current_batch_id}")
        return self.current_batch_id
    
    def log_batch_progress(
        self,
        batch_number: int,
        records_processed: int,
        records_inserted: int = None,
        records_updated: int = None,
        records_deleted: int = None
    ) -> None:
        """
        Log progress for a batch operation.
        
        Args:
            batch_number: Current batch number
            records_processed: Cumulative records processed
            records_inserted: Cumulative records inserted
            records_updated: Cumulative records updated
            records_deleted: Cumulative records deleted
        """
        if self.current_process_id:
            self.process_logger.update_process_metrics(
                log_id=self.current_process_id,
                rows_processed=records_processed,
                rows_inserted=records_inserted,
                rows_updated=records_updated,
                rows_deleted=records_deleted
            )
            
            logger.info(f"Batch {batch_number} progress: {records_processed} records processed")
    
    def end_batch(
        self,
        status: str,
        final_record_count: int,
        error_message: str = None
    ) -> None:
        """
        End a batch operation.
        
        Args:
            status: Final status (SUCCESS, FAILED, CANCELLED)
            final_record_count: Final count of processed records
            error_message: Error message if status is FAILED
        """
        if self.current_process_id:
            self.process_logger.end_process(
                log_id=self.current_process_id,
                status=status,
                rows_processed=final_record_count,
                error_message=error_message
            )
            
            logger.info(f"Completed batch {self.current_batch_id} with status {status}")
            
        self.current_batch_id = None
        self.current_process_id = None