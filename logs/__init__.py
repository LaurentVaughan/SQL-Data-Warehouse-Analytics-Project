"""
=============================================================
Logging and monitoring infrastructure for the data warehouse.
=============================================================

This package provides comprehensive logging capabilities for audit trails,
process tracking, error handling, performance monitoring, and data lineage
across the medallion architecture.

The logging package is designed to be reusable across all ETL processes and
provides centralized logging separate from the setup infrastructure.

Modules:
    audit_logger: Process execution logging and audit trails
    error_handler: Centralized error logging and recovery mechanisms
    performance_monitor: Performance metrics collection and analysis
    data_lineage: Data lineage tracking through medallion layers

Components:
    ProcessLogger: Log ETL process start/end and metrics
    ConfigurationLogger: Track configuration changes for audit
    ErrorLogger: Centralized error logging with context
    ErrorRecovery: Automated error recovery and retry mechanisms
    PerformanceMonitor: Track performance metrics for processes
    MetricsCollector: Collect and aggregate system metrics
    LineageTracker: Track data lineage through transformations
    LineageAnalyzer: Analyze and visualize data lineage

Example:
    >>> from logs.audit_logger import ProcessLogger
    >>> from logs.error_handler import ErrorLogger
    >>> from logs.performance_monitor import PerformanceMonitor
    >>> from logs.data_lineage import LineageTracker
    >>> 
    >>> # Process logging
    >>> process_logger = ProcessLogger()
    >>> process_id = process_logger.start_process('bronze_load', 'Load CRM data')
    >>> # ... do work ...
    >>> process_logger.end_process(process_id, 'SUCCESS', rows_processed=1000)
    >>> 
    >>> # Error logging
    >>> error_logger = ErrorLogger()
    >>> error_logger.log_error(
    ...     process_log_id=process_id,
    ...     error_message="Validation failed",
    ...     recovery_suggestion="Check source data format"
    ... )
    >>> 
    >>> # Performance monitoring
    >>> perf_monitor = PerformanceMonitor()
    >>> perf_monitor.record_metric(process_id, 'processing_time', 45.2)
    >>> 
    >>> # Data lineage
    >>> lineage = LineageTracker()
    >>> lineage.log_lineage(
    ...     process_log_id=process_id,
    ...     source_schema='bronze',
    ...     target_schema='silver'
    ... )
"""

__version__ = "0.1.0"
__all__ = [
    'ProcessLogger', 'ConfigurationLogger',
    'ErrorLogger', 'ErrorRecovery', 
    'PerformanceMonitor', 'MetricsCollector',
    'LineageTracker', 'LineageAnalyzer'
]

# Note: Eager imports removed to prevent circular dependencies.
# Import modules directly when needed:
#   from logs.audit_logger import ProcessLogger, ConfigurationLogger
#   from logs.error_handler import ErrorLogger, ErrorRecovery
#   from logs.performance_monitor import PerformanceMonitor, MetricsCollector
#   from logs.data_lineage import LineageTracker, LineageAnalyzer
