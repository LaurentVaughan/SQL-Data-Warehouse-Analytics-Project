"""
=========================================================
Main orchestrator for SQL Data Warehouse Analytics Project.
=========================================================

This is the top-level entry point for all data warehouse operations including:
    - Database infrastructure setup
    - Schema and logging infrastructure creation
    - Bronze layer data ingestion (future)
    - Silver layer transformations (future)
    - Gold layer analytics (future)
    - Monitoring and maintenance tasks

The main orchestrator coordinates all components with proper error handling,
logging, and recovery mechanisms. It uses the centralized configuration and
integrates with all logging infrastructure for comprehensive audit trails.

Architecture:
    1. Database Connectivity (utils.database_utils)
    2. Setup Orchestration (setup.setup_orchestrator)
    3. Application Logging (core.logger) - ALWAYS AVAILABLE
    4. Audit Logging (logs.audit_logger) - AFTER SETUP
    5. Error Handling (logs.error_handler) - AFTER SETUP
    6. Performance Monitoring (logs.performance_monitor) - OPTIONAL
    7. [Future] Medallion Layer Processing

Key Design Principles:
    - core.logger for console/file logging (ALWAYS available)
    - logs.* modules for database audit (ONLY after setup)
    - SetupOrchestrator handles ALL setup logic
    - main.py is a thin CLI wrapper

Usage:
    # Run complete setup
    python main.py --setup
    
    # Run setup with performance monitoring
    python main.py --setup --monitor
    
    # Run bronze layer ingestion (future)
    python main.py --bronze --source crm
    
    # Run full pipeline (future)
    python main.py --full-pipeline
    
Example:
    >>> from main import DataWarehouseOrchestrator
    >>> 
    >>> orchestrator = DataWarehouseOrchestrator()
    >>> orchestrator.run_setup(include_samples=True, monitor_performance=True)
    >>> 
    >>> # Future: Run medallion layers
    >>> orchestrator.run_bronze_ingestion(source='crm')
    >>> orchestrator.run_silver_transformation()
    >>> orchestrator.run_gold_analytics()
"""

import argparse
import sys
from datetime import datetime
from typing import Any, Dict, Optional

# Core infrastructure
from core.config import config
from core.logger import get_logger  # Application logging (ALWAYS available)

# Audit logging infrastructure (ONLY available AFTER setup)
from logs.audit_logger import AuditLoggerError, ProcessLogger
from logs.error_handler import ErrorHandlerError, ErrorLogger, ErrorRecovery
from logs.performance_monitor import PerformanceMonitor, PerformanceMonitorError

# Setup orchestration
from setup.setup_orchestrator import SetupError, SetupOrchestrator

# Database connectivity
from utils.database_utils import (
    get_database_connection_info,
    verify_connection,
    verify_database_exists,
    wait_for_database,
)

# Future imports (placeholders)
# from medallion.bronze import BronzeLayerOrchestrator
# from medallion.silver import SilverLayerOrchestrator
# from medallion.gold import GoldLayerOrchestrator

logger = get_logger(__name__)


class OrchestratorError(Exception):
    """Exception raised for orchestrator operation errors."""
    pass


class DataWarehouseOrchestrator:
    """
    Top-level orchestrator for all data warehouse operations.
    
    Coordinates infrastructure setup, ETL processing, and monitoring
    with comprehensive error handling and audit trails.
    
    Architecture Notes:
        - Uses core.logger for console/file logging (ALWAYS available)
        - Uses logs.* modules for database audit (ONLY after setup completes)
        - Delegates setup to SetupOrchestrator (NOT direct DDL)
        - Thin orchestration layer, not implementation
    
    Attributes:
        monitor_performance: Enable performance monitoring
        process_logger: ProcessLogger instance (available after setup)
        error_logger: ErrorLogger instance (available after setup)
        perf_monitor: PerformanceMonitor instance (available after setup)
        error_recovery: ErrorRecovery instance (available after setup)
        setup_orchestrator: SetupOrchestrator instance
        current_process_id: Current process log ID for tracking
        
    Example:
        >>> orchestrator = DataWarehouseOrchestrator(monitor_performance=True)
        >>> orchestrator.run_setup(include_samples=True)
        >>> # Future: orchestrator.run_bronze_ingestion(source='crm')
    """
    
    def __init__(self, monitor_performance: bool = False):
        """
        Initialize the data warehouse orchestrator.
        
        Args:
            monitor_performance: Enable performance monitoring for all operations
        """
        self.monitor_performance = monitor_performance
        
        # Audit logging infrastructure (initialized AFTER setup)
        self.process_logger: Optional[ProcessLogger] = None
        self.error_logger: Optional[ErrorLogger] = None
        self.perf_monitor: Optional[PerformanceMonitor] = None
        self.error_recovery: Optional[ErrorRecovery] = None
        
        # Setup orchestrator
        self.setup_orchestrator: Optional[SetupOrchestrator] = None
        
        # Current process tracking
        self.current_process_id: Optional[int] = None
        
        logger.info("=" * 70)
        logger.info("SQL Data Warehouse Analytics - Main Orchestrator")
        logger.info("=" * 70)
    
    def verify_prerequisites(self) -> bool:
        """
        Verify all prerequisites for warehouse operations.
        
        Checks:
            1. PostgreSQL server connectivity
            2. Database availability with retry logic
            3. Connection parameters validity
        
        Returns:
            True if all prerequisites met
            
        Raises:
            OrchestratorError: If critical prerequisites are missing
            
        Note:
            Uses utils.database_utils for all connectivity checks.
            Uses core.logger for output (NOT logs.audit_logger yet).
        """
        logger.info("\n🔍 Verifying prerequisites...")
        
        try:
            # 1. Display connection info
            conn_info = get_database_connection_info()
            logger.info(f"📍 PostgreSQL Server: {conn_info['host']}:{conn_info['port']}")
            logger.info(f"👤 User: {conn_info['user']}")
            logger.info(f"🗄️  Admin Database: {conn_info['admin_database']}")
            logger.info(f"🏢 Warehouse Database: {conn_info['warehouse_database']}")
            
            # 2. Test connection
            logger.info("\n⏳ Testing database connection...")
            success, message = verify_connection()
            
            if not success:
                logger.error(f"❌ {message}")
                raise OrchestratorError(
                    f"PostgreSQL connection failed: {message}\n"
                    f"Please ensure PostgreSQL is running at {conn_info['host']}:{conn_info['port']}"
                )
            
            logger.info(f"✅ {message}")
            
            # 3. Wait for database availability (with retries)
            logger.info("\n⏳ Waiting for database availability...")
            wait_for_database(max_retries=5, retry_delay=2)
            
            logger.info("✅ All prerequisites verified successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Prerequisite verification failed: {e}")
            raise OrchestratorError(f"Prerequisite verification failed: {e}")
    
    def verify_setup_complete(self) -> bool:
        """
        Verify if warehouse setup is already complete.
        
        Checks:
            1. Warehouse database exists
            2. Database is accessible
        
        Note: This is a lightweight check. Does not verify all schemas/tables,
        just confirms the database exists and is accessible.
        
        Returns:
            True if warehouse database exists and is accessible
            
        Example:
            >>> if orchestrator.verify_setup_complete():
            ...     print("Warehouse ready for operations")
        """
        try:
            # Check if warehouse database exists
            if verify_database_exists(config.warehouse_db_name):
                logger.debug(f"✅ Warehouse database '{config.warehouse_db_name}' exists")
                return True
            else:
                logger.debug(f"❌ Warehouse database '{config.warehouse_db_name}' does not exist")
                return False
                
        except Exception as e:
            logger.debug(f"Setup verification failed: {e}")
            return False
    
    def initialize_logging_infrastructure(self) -> None:
        """
        Initialize audit logging infrastructure after warehouse setup.
        
        CRITICAL: This method can ONLY be called AFTER:
            1. Database exists
            2. Schemas created (including logs schema)
            3. Logging tables created (process_log, error_log, etc.)
        
        Initializes:
            - ProcessLogger: For tracking ETL process execution
            - ErrorLogger: For database error logging
            - ErrorRecovery: For automated retry mechanisms
            - PerformanceMonitor: For performance metrics (if enabled)
        
        Raises:
            OrchestratorError: If warehouse database doesn't exist
            
        Note:
            This is separate from core.logger which is ALWAYS available.
            This provides DATABASE AUDIT LOGGING capabilities.
        """
        logger.info("\n📝 Initializing audit logging infrastructure...")
        
        try:
            # Verify warehouse database exists
            if not verify_database_exists(config.warehouse_db_name):
                raise OrchestratorError(
                    f"Warehouse database '{config.warehouse_db_name}' does not exist. "
                    "Run setup first."
                )
            
            # Initialize process logger (for database audit trails)
            self.process_logger = ProcessLogger(
                host=config.db_host,
                port=config.db_port,
                user=config.db_user,
                password=config.db_password,
                database=config.warehouse_db_name
            )
            logger.info("✅ ProcessLogger initialized (database audit trails)")
            
            # Initialize error logger (for database error logging)
            self.error_logger = ErrorLogger(
                host=config.db_host,
                port=config.db_port,
                user=config.db_user,
                password=config.db_password,
                database=config.warehouse_db_name
            )
            logger.info("✅ ErrorLogger initialized (database error tracking)")
            
            # Initialize error recovery (automated retry logic)
            self.error_recovery = ErrorRecovery(
                error_logger=self.error_logger,
                max_retries=3,
                base_delay=1.0,
                backoff_multiplier=2.0
            )
            logger.info("✅ ErrorRecovery initialized (automated retry)")
            
            # Initialize performance monitor if enabled
            if self.monitor_performance:
                self.perf_monitor = PerformanceMonitor(
                    host=config.db_host,
                    port=config.db_port,
                    user=config.db_user,
                    password=config.db_password,
                    database=config.warehouse_db_name
                )
                logger.info("✅ PerformanceMonitor initialized (metrics collection)")
            
            logger.info("✅ Audit logging infrastructure ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize audit logging infrastructure: {e}")
            raise OrchestratorError(f"Audit logging initialization failed: {e}")
    
    def run_setup(
        self,
        include_samples: bool = False,
        force_recreate: bool = False
    ) -> Dict[str, bool]:
        """
        Run complete data warehouse setup.
        
        This orchestrates (via SetupOrchestrator):
            1. Database creation (via DatabaseCreator → sql.ddl)
            2. Schema creation (via SchemaCreator → sql.ddl)
            3. Logging infrastructure tables (via LoggingInfrastructure)
            4. Sample medallion tables (optional, via sql.ddl)
        
        Then initializes audit logging infrastructure for future operations.
        
        Architecture Note:
            - DOES NOT call sql.ddl directly
            - DELEGATES to SetupOrchestrator which coordinates everything
            - Uses core.logger for console output
            - Initializes logs.* modules AFTER setup completes
            - SKIPS setup if already complete (unless force_recreate=True)
        
        Args:
            include_samples: Create sample medallion tables for demo
            force_recreate: Drop and recreate if exists (DANGEROUS!)
            
        Returns:
            Dictionary mapping setup steps to success status:
                - 'database': Database creation success
                - 'schemas': Schema creation success
                - 'logging': Logging infrastructure success
                - 'samples': Sample tables success (if include_samples=True)
                - 'already_complete': True if setup was skipped (already done)
            
        Raises:
            OrchestratorError: If any setup step fails
            
        Example:
            >>> orchestrator = DataWarehouseOrchestrator()
            >>> results = orchestrator.run_setup(include_samples=True)
            >>> if all(results.values()):
            ...     print("Setup completed successfully")
        """
        logger.info("\n" + "=" * 70)
        logger.info("🚀 DATA WAREHOUSE SETUP")
        logger.info("=" * 70)
        
        setup_start_time = datetime.now()
        
        try:
            # 1. Verify prerequisites (connectivity, availability)
            self.verify_prerequisites()
            
            # 2. Check if setup is already complete (unless force recreate)
            if not force_recreate and self.verify_setup_complete():
                logger.info("\n✅ Warehouse setup is already complete!")
                logger.info("   Database exists and is accessible")
                logger.info("\n💡 Options:")
                logger.info("   • To recreate: python main.py --setup --force-recreate")
                logger.info("   • To proceed: python main.py --bronze")
                
                # Initialize logging infrastructure for operations
                self.initialize_logging_infrastructure()
                
                # Set setup_orchestrator to None (not needed, already complete)
                self.setup_orchestrator = None
                
                logger.info("\n✅ Warehouse is ready for operations")
                logger.info("\n🎯 Next Steps:")
                logger.info("  1. Run bronze layer ingestion: python main.py --bronze")
                logger.info("  2. Run full pipeline: python main.py --full-pipeline")
                
                return {
                    'database': True,
                    'schemas': True,
                    'logging': True,
                    'already_complete': True
                }
            
            # 3. Handle force recreate warning
            if force_recreate:
                logger.warning("\n⚠️  Force recreate enabled - existing database will be dropped")
                logger.warning("⚠️  THIS IS A DESTRUCTIVE OPERATION")
                # Actual recreation handled by SetupOrchestrator
            
            # 4. Initialize setup orchestrator
            logger.info("\n🔧 Initializing setup orchestrator...")
            self.setup_orchestrator = SetupOrchestrator()
            
            # 5. Delegate to SetupOrchestrator (which handles EVERYTHING)
            logger.info("\n⚙️  Running complete warehouse setup...")
            logger.info("    (Database → Schemas → Logging Tables → Samples)")
            
            results = self.setup_orchestrator.run_complete_setup(
                include_samples=include_samples
            )
            
            # 6. Check results
            if not all(results.values()):
                failed_steps = [step for step, success in results.items() if not success]
                raise OrchestratorError(
                    f"Setup failed for steps: {', '.join(failed_steps)}"
                )
            
            # 7. NOW initialize audit logging infrastructure (database ready!)
            logger.info("\n🔧 Setup completed, initializing audit logging...")
            self.initialize_logging_infrastructure()
            
            # 8. Log setup completion
            setup_duration = (datetime.now() - setup_start_time).total_seconds()
            
            logger.info("\n" + "=" * 70)
            logger.info("✅ SETUP COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"⏱️  Total setup time: {setup_duration:.2f} seconds")
            logger.info("\n📊 Setup Results:")
            for step, success in results.items():
                status = "✅ SUCCESS" if success else "❌ FAILED"
                logger.info(f"  {step.ljust(20)}: {status}")
            
            logger.info("\n🎯 Next Steps:")
            logger.info("  1. Run bronze layer ingestion: python main.py --bronze")
            logger.info("  2. View process logs in database: logs.process_log")
            logger.info("  3. Check error logs: logs.error_log")
            
            return results
            
        except SetupError as e:
            logger.error(f"\n❌ Setup failed: {e}")
            raise OrchestratorError(f"Setup failed: {e}")
        except Exception as e:
            logger.error(f"\n❌ Unexpected error during setup: {e}", exc_info=True)
            raise OrchestratorError(f"Unexpected setup error: {e}")
    
    # ========================================================================
    # FUTURE: Bronze Layer Operations
    # ========================================================================
    
    def run_bronze_ingestion(
        self,
        source: str = 'all',
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Run bronze layer data ingestion (PLACEHOLDER FOR FUTURE).
        
        Architecture (Future):
            1. Initialize audit logging if needed
            2. Start process tracking (ProcessLogger)
            3. Optional performance monitoring (PerformanceMonitor)
            4. Delegate to BronzeLayerOrchestrator
            5. Log errors to database (ErrorLogger)
            6. End process tracking
        
        Args:
            source: Source system ('crm', 'erp', 'all')
            batch_size: Batch size for processing
            
        Returns:
            Ingestion results dictionary
            
        Raises:
            OrchestratorError: If ingestion fails
            
        Note:
            This is a PLACEHOLDER. Actual implementation will:
            - Use logs.audit_logger for process tracking
            - Use logs.error_handler for error management
            - Use logs.performance_monitor if enabled
            - Delegate to medallion.bronze module
            
        Example:
            >>> orchestrator.run_bronze_ingestion(source='crm')
        """
        logger.info("\n" + "=" * 70)
        logger.info(f"🥉 BRONZE LAYER INGESTION - Source: {source}")
        logger.info("=" * 70)
        
        # Ensure audit logging infrastructure is available
        if not self.process_logger:
            logger.info("Audit logging not initialized, initializing now...")
            self.initialize_logging_infrastructure()
        
        try:
            # Start process tracking (DATABASE AUDIT)
            self.current_process_id = self.process_logger.start_process(
                process_name=f'bronze_ingestion_{source}',
                process_description=f'Ingest raw data from {source} system',
                source_system=source,
                target_layer='bronze',
                metadata={'batch_size': batch_size}
            )
            logger.info(f"✅ Started process tracking (log_id={self.current_process_id})")
            
            # Performance monitoring context (OPTIONAL)
            if self.perf_monitor:
                with self.perf_monitor.monitor_process(self.current_process_id) as pm:
                    # TODO: Implement bronze layer ingestion
                    # from medallion.bronze import BronzeLayerOrchestrator
                    # bronze_orchestrator = BronzeLayerOrchestrator(...)
                    # results = bronze_orchestrator.ingest(source, batch_size)
                    
                    logger.warning("⚠️  Bronze layer ingestion not yet implemented")
                    results = {'status': 'NOT_IMPLEMENTED'}
                    
                    # Record metrics
                    pm.record_metric('rows_processed', 0, 'rows')
                    pm.record_metric('files_processed', 0, 'files')
            else:
                logger.warning("⚠️  Bronze layer ingestion not yet implemented")
                results = {'status': 'NOT_IMPLEMENTED'}
            
            # End process tracking
            self.process_logger.end_process(
                log_id=self.current_process_id,
                status='SUCCESS',
                rows_processed=0
            )
            logger.info("✅ Process completed and logged to database")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Bronze ingestion failed: {e}", exc_info=True)
            
            # Log error to DATABASE (not just console)
            if self.error_logger and self.current_process_id:
                self.error_logger.log_exception(
                    process_log_id=self.current_process_id,
                    exception=e,
                    context={'source': source, 'batch_size': batch_size},
                    recovery_suggestion="Check source data availability and format"
                )
            
            # End process as FAILED in database
            if self.process_logger and self.current_process_id:
                self.process_logger.end_process(
                    log_id=self.current_process_id,
                    status='FAILED',
                    error_message=str(e)
                )
            
            raise OrchestratorError(f"Bronze ingestion failed: {e}")
    
    # ========================================================================
    # FUTURE: Silver Layer Operations
    # ========================================================================
    
    def run_silver_transformation(self) -> Dict[str, Any]:
        """
        Run silver layer transformations (PLACEHOLDER FOR FUTURE).
        
        Architecture (Future):
            - Same pattern as run_bronze_ingestion()
            - Process tracking with ProcessLogger
            - Error logging with ErrorLogger
            - Performance monitoring with PerformanceMonitor
            - Delegate to SilverLayerOrchestrator
        
        Returns:
            Transformation results dictionary
        """
        logger.info("\n" + "=" * 70)
        logger.info("🥈 SILVER LAYER TRANSFORMATION")
        logger.info("=" * 70)
        
        logger.warning("⚠️  Silver layer transformation not yet implemented")
        logger.info("    Future: Will use logs.audit_logger for process tracking")
        logger.info("    Future: Will use logs.error_handler for error management")
        logger.info("    Future: Will delegate to medallion.silver module")
        
        return {'status': 'NOT_IMPLEMENTED'}
    
    # ========================================================================
    # FUTURE: Gold Layer Operations
    # ========================================================================
    
    def run_gold_analytics(self) -> Dict[str, Any]:
        """
        Run gold layer analytics (PLACEHOLDER FOR FUTURE).
        
        Architecture (Future):
            - Same pattern as run_bronze_ingestion()
            - Process tracking with ProcessLogger
            - Error logging with ErrorLogger
            - Performance monitoring with PerformanceMonitor
            - Delegate to GoldLayerOrchestrator
        
        Returns:
            Analytics results dictionary
        """
        logger.info("\n" + "=" * 70)
        logger.info("🥇 GOLD LAYER ANALYTICS")
        logger.info("=" * 70)
        
        logger.warning("⚠️  Gold layer analytics not yet implemented")
        logger.info("    Future: Will use logs.audit_logger for process tracking")
        logger.info("    Future: Will use logs.error_handler for error management")
        logger.info("    Future: Will delegate to medallion.gold module")
        
        return {'status': 'NOT_IMPLEMENTED'}
    
    # ========================================================================
    # FUTURE: Full Pipeline
    # ========================================================================
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Run complete medallion architecture pipeline (PLACEHOLDER FOR FUTURE).
        
        Executes: Bronze → Silver → Gold
        
        Architecture (Future):
            1. Initialize audit logging
            2. Start overall pipeline process tracking
            3. Run bronze ingestion (with error handling)
            4. Run silver transformation (with error handling)
            5. Run gold analytics (with error handling)
            6. Log pipeline completion
        
        Returns:
            Pipeline execution results for each layer
            
        Raises:
            OrchestratorError: If any layer fails
        """
        logger.info("\n" + "=" * 70)
        logger.info("🏗️  FULL MEDALLION PIPELINE")
        logger.info("=" * 70)
        
        results = {}
        
        try:
            # Future: Track entire pipeline as one process
            logger.info("Bronze → Silver → Gold")
            
            # Bronze layer
            logger.info("\n1️⃣  Running Bronze Layer...")
            results['bronze'] = self.run_bronze_ingestion(source='all')
            
            # Silver layer
            logger.info("\n2️⃣  Running Silver Layer...")
            results['silver'] = self.run_silver_transformation()
            
            # Gold layer
            logger.info("\n3️⃣  Running Gold Layer...")
            results['gold'] = self.run_gold_analytics()
            
            logger.info("\n✅ Pipeline completed")
            return results
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
            raise OrchestratorError(f"Pipeline execution failed: {e}")


def main():
    """
    Command-line interface for the data warehouse orchestrator.
    
    Provides CLI for all warehouse operations with comprehensive
    argument parsing and error handling.
    
    Usage:
        # Run setup
        python main.py --setup
        
        # Run setup with samples and monitoring
        python main.py --setup --samples --monitor
        
        # Run bronze ingestion (future)
        python main.py --bronze --source crm
        
        # Run full pipeline (future)
        python main.py --full-pipeline --monitor
    
    Exit Codes:
        0: Success
        1: Error
        130: User interrupt (Ctrl+C)
    """
    parser = argparse.ArgumentParser(
        description="SQL Data Warehouse Analytics - Main Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Complete setup with sample tables
  python main.py --setup --samples
  
  # Setup with performance monitoring
  python main.py --setup --monitor
  
  # Force recreate database (DANGEROUS!)
  python main.py --setup --force-recreate
  
  # Run bronze layer ingestion (future)
  python main.py --bronze --source crm
  
  # Run full pipeline with monitoring (future)
  python main.py --full-pipeline --monitor

Architecture:
  - Uses core.logger for console/file logging (ALWAYS available)
  - Uses logs.* modules for database audit (AFTER setup)
  - Delegates to setup.setup_orchestrator for setup
  - Thin CLI wrapper, not implementation
        """
    )
    
    # Setup operations
    parser.add_argument(
        '--setup',
        action='store_true',
        help='Run complete warehouse setup (database, schemas, logging)'
    )
    parser.add_argument(
        '--samples',
        action='store_true',
        help='Include sample medallion tables in setup'
    )
    parser.add_argument(
        '--force-recreate',
        action='store_true',
        help='Drop and recreate database if exists (DANGEROUS!)'
    )
    
    # Bronze layer operations (future)
    parser.add_argument(
        '--bronze',
        action='store_true',
        help='Run bronze layer ingestion (NOT YET IMPLEMENTED)'
    )
    parser.add_argument(
        '--source',
        type=str,
        choices=['crm', 'erp', 'all'],
        default='all',
        help='Source system for bronze ingestion'
    )
    
    # Silver layer operations (future)
    parser.add_argument(
        '--silver',
        action='store_true',
        help='Run silver layer transformation (NOT YET IMPLEMENTED)'
    )
    
    # Gold layer operations (future)
    parser.add_argument(
        '--gold',
        action='store_true',
        help='Run gold layer analytics (NOT YET IMPLEMENTED)'
    )
    
    # Full pipeline (future)
    parser.add_argument(
        '--full-pipeline',
        action='store_true',
        help='Run complete medallion pipeline (NOT YET IMPLEMENTED)'
    )
    
    # Monitoring options
    parser.add_argument(
        '--monitor',
        action='store_true',
        help='Enable performance monitoring (metrics collection)'
    )
    
    # Verbose logging
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize orchestrator
        orchestrator = DataWarehouseOrchestrator(
            monitor_performance=args.monitor
        )
        
        # Determine operation
        if args.setup:
            # Run setup
            results = orchestrator.run_setup(
                include_samples=args.samples,
                force_recreate=args.force_recreate
            )
            
            if all(results.values()):
                logger.info("\n🎉 Setup completed successfully!")
                return 0
            else:
                logger.error("\n❌ Setup completed with errors")
                return 1
        
        elif args.bronze:
            # Run bronze ingestion (future)
            orchestrator.initialize_logging_infrastructure()
            results = orchestrator.run_bronze_ingestion(source=args.source)
            return 0
        
        elif args.silver:
            # Run silver transformation (future)
            orchestrator.initialize_logging_infrastructure()
            results = orchestrator.run_silver_transformation()
            return 0
        
        elif args.gold:
            # Run gold analytics (future)
            orchestrator.initialize_logging_infrastructure()
            results = orchestrator.run_gold_analytics()
            return 0
        
        elif args.full_pipeline:
            # Run full pipeline (future)
            orchestrator.initialize_logging_infrastructure()
            results = orchestrator.run_full_pipeline()
            return 0
        
        else:
            # No operation specified
            parser.print_help()
            logger.warning("\n⚠️  No operation specified. Use --setup, --bronze, etc.")
            return 1
        
    except OrchestratorError as e:
        logger.error(f"\n❌ Orchestration failed: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Operation interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())