"""
===========================================================
ORM Models for Logging Infrastructure
===========================================================

SQLAlchemy ORM model definitions for the logging schema.

This module contains all database table models for audit trails, process
tracking, error logging, performance monitoring, and data lineage. These
models are separated from the setup logic to prevent circular imports and
improve maintainability.

Models:
    ProcessLog: Track ETL process execution and metrics
    ErrorLog: Centralized error logging with context
    DataLineage: Track data flow through medallion layers
    PerformanceMetrics: Performance monitoring and optimization
    ConfigurationLog: Audit configuration changes

Architecture:
    - Separated from setup/ package to break circular dependencies
    - Used by both logs/ (for logging operations) and setup/ (for table creation)
    - SQLAlchemy declarative base for ORM functionality

Example:
    >>> from models.logs_models import ProcessLog, ConfigurationLog
    >>> from sqlalchemy.orm import Session
    >>> 
    >>> # Create a new process log entry
    >>> process_log = ProcessLog(
    ...     process_name='bronze_ingestion',
    ...     process_description='Load CRM data',
    ...     source_system='CRM',
    ...     target_layer='bronze'
    ... )
    >>> session.add(process_log)
    >>> session.commit()
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

# SQLAlchemy declarative base
Base = declarative_base()


class ProcessLog(Base):
    """Process execution logging table for ETL audit trails.
    
    Tracks start/end times, status, and metadata for all ETL processes
    across the medallion architecture. Provides comprehensive audit trail
    for data processing operations.
    
    Attributes:
        log_id: Unique identifier for each process log entry
        process_name: Name of the ETL process
        process_description: Detailed description of process purpose
        start_time: Process start timestamp
        end_time: Process completion timestamp
        status: Process status (RUNNING/SUCCESS/FAILED)
        rows_processed: Number of rows processed
        rows_inserted: Number of rows inserted
        rows_updated: Number of rows updated
        rows_deleted: Number of rows deleted
        source_system: Source system identifier
        target_layer: Target medallion layer (bronze/silver/gold)
        error_message: Error message if process failed
        process_metadata: Additional JSON metadata (JSONB)
        created_by: User or system that initiated the process
    
    Relationships:
        error_logs: Related ErrorLog entries
        data_lineage: Related DataLineage entries
    """
    __tablename__ = 'process_log'
    __table_args__ = {'schema': 'logs'}
    
    log_id = Column(Integer, primary_key=True, autoincrement=True,
                   comment='Unique identifier for each process log entry')
    process_name = Column(String(100), nullable=False,
                         comment='Name of the process (e.g., bronze_ingestion, silver_transform)')
    process_description = Column(Text,
                               comment='Detailed description of what the process does')
    start_time = Column(DateTime, nullable=False, default=func.now(),
                       comment='Process start timestamp')
    end_time = Column(DateTime,
                     comment='Process end timestamp')
    status = Column(String(20), nullable=False, default='RUNNING',
                   comment='Process status: RUNNING, SUCCESS, FAILED, CANCELLED')
    rows_processed = Column(Integer,
                          comment='Number of rows processed')
    rows_inserted = Column(Integer,
                         comment='Number of rows inserted')
    rows_updated = Column(Integer,
                        comment='Number of rows updated')
    rows_deleted = Column(Integer,
                        comment='Number of rows deleted')
    source_system = Column(String(50),
                         comment='Source system identifier (CRM, ERP, etc.)')
    target_layer = Column(String(20),
                        comment='Target medallion layer (bronze, silver, gold)')
    error_message = Column(Text,
                         comment='Error message if process failed')
    process_metadata = Column(JSONB,
                            comment='Additional JSON metadata about the process')
    created_by = Column(String(50), nullable=False, default='system',
                       comment='User or system that initiated the process')
    
    # Relationships
    error_logs = relationship("ErrorLog", back_populates="process")
    data_lineage = relationship("DataLineage", back_populates="process")


class ErrorLog(Base):
    """Error logging table.
    
    Captures detailed error information including stack traces,
    error codes, and recovery suggestions for all ETL processes.
    
    Attributes:
        error_id: Unique identifier for each error log entry
        process_log_id: Reference to the process that generated the error
        error_timestamp: When the error occurred
        error_level: Error severity (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        error_code: Application-specific error code
        error_message: Human-readable error message
        error_detail: Detailed error information including stack trace
        table_name: Table involved in the error
        column_name: Column involved in the error
        row_context: Row data or context where error occurred
        recovery_suggestion: Suggested steps to resolve the error
        is_resolved: Whether the error has been resolved
        resolved_by: User who marked the error as resolved
        resolved_timestamp: When the error was marked as resolved
    
    Relationships:
        process: Related ProcessLog entry
    """
    __tablename__ = 'error_log'
    __table_args__ = {'schema': 'logs'}
    
    error_id = Column(Integer, primary_key=True, autoincrement=True,
                     comment='Unique identifier for each error log entry')
    process_log_id = Column(Integer, ForeignKey('logs.process_log.log_id'),
                          comment='Reference to the process that generated the error')
    error_timestamp = Column(DateTime, nullable=False, default=func.now(),
                           comment='When the error occurred')
    error_level = Column(String(10), nullable=False, default='ERROR',
                        comment='Error severity: DEBUG, INFO, WARNING, ERROR, CRITICAL')
    error_code = Column(String(20),
                       comment='Application-specific error code')
    error_message = Column(Text, nullable=False,
                         comment='Human-readable error message')
    error_detail = Column(Text,
                        comment='Detailed error information including stack trace')
    table_name = Column(String(100),
                       comment='Table involved in the error')
    column_name = Column(String(100),
                        comment='Column involved in the error')
    row_context = Column(Text,
                        comment='Row data or context where error occurred')
    recovery_suggestion = Column(Text,
                               comment='Suggested steps to resolve the error')
    is_resolved = Column(Boolean, default=False,
                        comment='Whether the error has been resolved')
    resolved_by = Column(String(50),
                        comment='User who marked the error as resolved')
    resolved_timestamp = Column(DateTime,
                              comment='When the error was marked as resolved')
    
    # Relationships
    process = relationship("ProcessLog", back_populates="error_logs")


class DataLineage(Base):
    """Data lineage tracking table.
    
    Tracks the flow of data through the medallion architecture,
    enabling impact analysis and data governance.
    
    Attributes:
        lineage_id: Unique identifier for each lineage entry
        process_log_id: Reference to the process that created this lineage
        source_schema: Source schema name
        source_table: Source table name
        source_column: Source column name (optional)
        target_schema: Target schema name
        target_table: Target table name
        target_column: Target column name (optional)
        transformation_logic: Description of transformation applied
        record_count: Number of records involved in this lineage
        created_timestamp: When this lineage entry was created
    
    Relationships:
        process: Related ProcessLog entry
    """
    __tablename__ = 'data_lineage'
    __table_args__ = {'schema': 'logs'}
    
    lineage_id = Column(Integer, primary_key=True, autoincrement=True,
                       comment='Unique identifier for each lineage entry')
    process_log_id = Column(Integer, ForeignKey('logs.process_log.log_id'),
                          comment='Reference to the process that created this lineage')
    source_schema = Column(String(50),
                         comment='Source schema name')
    source_table = Column(String(100),
                        comment='Source table name')
    source_column = Column(String(100),
                         comment='Source column name (optional)')
    target_schema = Column(String(50),
                         comment='Target schema name')
    target_table = Column(String(100),
                        comment='Target table name')
    target_column = Column(String(100),
                         comment='Target column name (optional)')
    transformation_logic = Column(Text,
                                comment='Description of transformation applied')
    record_count = Column(Integer,
                        comment='Number of records involved in this lineage')
    created_timestamp = Column(DateTime, nullable=False, default=func.now(),
                             comment='When this lineage entry was created')
    
    # Relationships
    process = relationship("ProcessLog", back_populates="data_lineage")


class PerformanceMetrics(Base):
    """Performance metrics table.
    
    Captures performance statistics for monitoring and optimization
    of ETL processes across the medallion architecture.
    
    Attributes:
        metric_id: Unique identifier for each metric entry
        process_log_id: Reference to the process being measured
        metric_name: Name of the performance metric
        metric_value: Numeric value of the metric
        metric_unit: Unit of measurement (seconds, MB, rows/sec, etc.)
        measurement_timestamp: When the metric was captured
        additional_context: Additional context about the measurement
    """
    __tablename__ = 'performance_metrics'
    __table_args__ = {'schema': 'logs'}
    
    metric_id = Column(Integer, primary_key=True, autoincrement=True,
                      comment='Unique identifier for each metric entry')
    process_log_id = Column(Integer, ForeignKey('logs.process_log.log_id'),
                          comment='Reference to the process being measured')
    metric_name = Column(String(100), nullable=False,
                        comment='Name of the performance metric')
    metric_value = Column(Numeric(15, 4),
                        comment='Numeric value of the metric')
    metric_unit = Column(String(20),
                        comment='Unit of measurement (seconds, MB, rows/sec, etc.)')
    measurement_timestamp = Column(DateTime, nullable=False, default=func.now(),
                                 comment='When the metric was captured')
    additional_context = Column(Text,
                              comment='Additional context about the measurement')


class ConfigurationLog(Base):
    """Configuration change log table.
    
    Tracks changes to system configuration for audit and rollback purposes.
    
    Attributes:
        config_log_id: Unique identifier for each configuration change
        config_key: Configuration parameter name
        old_value: Previous configuration value
        new_value: New configuration value
        change_reason: Reason for the configuration change
        changed_by: User who made the change
        change_timestamp: When the change was made
        environment: Environment where change was made (dev, test, prod)
    """
    __tablename__ = 'configuration_log'
    __table_args__ = {'schema': 'logs'}
    
    config_log_id = Column(Integer, primary_key=True, autoincrement=True,
                          comment='Unique identifier for each configuration change')
    config_key = Column(String(100), nullable=False,
                       comment='Configuration parameter name')
    old_value = Column(Text,
                      comment='Previous configuration value')
    new_value = Column(Text,
                      comment='New configuration value')
    change_reason = Column(Text,
                         comment='Reason for the configuration change')
    changed_by = Column(String(50), nullable=False,
                       comment='User who made the change')
    change_timestamp = Column(DateTime, nullable=False, default=func.now(),
                            comment='When the change was made')
    environment = Column(String(20), default='production',
                        comment='Environment where change was made (dev, test, prod)')
