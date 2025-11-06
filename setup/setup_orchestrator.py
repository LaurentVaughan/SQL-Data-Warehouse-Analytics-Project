"""
=====================================================
Setup orchestrator for data warehouse initialization.
=====================================================

Coordinates the complete setup process for the data warehouse including
database creation, schema setup, and logging infrastructure. Uses modular
SQL utilities and centralized configuration for maintainability.

This orchestrator manages:
    - Database creation with proper encoding/collation
    - Medallion schema creation (bronze/silver/gold/logs)
    - Logging infrastructure and audit tables
    - Sample table creation for demonstration
    - Dependency management between setup steps
    - Rollback capabilities for cleanup

Key Features:
    - Modular: Uses sql/ package for reusable SQL construction
    - Centralized Config: All settings from .env via core/config
    - Error Handling: Proper exception handling and rollback
    - Process Tracking: Logs all setup steps for auditability
    - Dependency Management: Ensures correct setup order

Example:
    >>> # Command-line usage
    >>> # python -m setup.setup_orchestrator
    >>> 
    >>> # Programmatic usage
    >>> from setup.setup_orchestrator import SetupOrchestrator
    >>> 
    >>> orchestrator = SetupOrchestrator()
    >>> results = orchestrator.run_complete_setup()
    >>> 
    >>> if all(results.values()):
    ...     print("Setup completed successfully")
    >>> 
    >>> # Rollback
    >>> orchestrator.rollback_setup(keep_database=False)
"""

import time
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Import centralized configuration and logging
from core.config import config
from core.logger import get_logger

# Import logging infrastructure
from logs.audit_logger import ConfigurationLogger, ProcessLogger

# Import our modular components
from setup.create_database import DatabaseCreator
from setup.create_logs import LoggingInfrastructure
from setup.create_schemas import SchemaCreator

# Import modular SQL utilities
from sql.ddl import create_medallion_table_template, create_schema, create_table
from sql.dml import bulk_insert, upsert

# Get logger for this module
logger = get_logger(__name__)


class SetupError(Exception):
    """Exception raised for setup process errors.
    
    Raised when setup operations fail, including database creation,
    schema creation, or configuration validation errors.
    """
    pass


class SetupOrchestrator:
    """Orchestrate complete data warehouse setup process.
    
    Coordinates all setup steps with proper dependency management,
    error handling, and rollback capabilities. Uses modular components
    from setup/ package and SQL utilities from sql/ package.
    
    Attributes:
        host: Database server hostname
        port: Database server port
        user: Database username
        password: Database password
        admin_db: Admin database name (typically 'postgres')
        target_db: Target warehouse database name
        db_creator: DatabaseCreator instance
        schema_creator: SchemaCreator instance
        logging_infrastructure: LoggingInfrastructure instance
        process_logger: ProcessLogger instance for audit tracking
        setup_steps: List of completed setup steps
        current_step: Current setup step being executed
    
    Example:
        >>> orchestrator = SetupOrchestrator()
        >>> 
        >>> # Run full setup
        >>> results = orchestrator.run_complete_setup(include_samples=True)
        >>> 
        >>> # Check results
        >>> for step, success in results.items():
        ...     print(f"{step}: {'✓' if success else '✗'}")
        >>> 
        >>> # Rollback if needed
        >>> orchestrator.rollback_setup(keep_database=True)
    """
    
    def __init__(self):
        """Initialize the setup orchestrator.
        
        Loads configuration from .env file via core/config and validates
        that all required settings are present. Does not create any
        database connections until setup methods are called.
        
        Raises:
            SetupError: If required configuration is missing
        """
        # Validate configuration
        self._validate_config()
        
        # Use configuration from .env file
        self.host = config.db_host
        self.port = config.db_port
        self.user = config.db_user
        self.password = config.db_password
        self.admin_db = config.db_name
        self.target_db = config.warehouse_db_name
        
        # Initialize component instances
        self.db_creator = None
        self.schema_creator = None
        self.logging_infrastructure = None
        self.process_logger = None
        
        # Setup tracking
        self.setup_steps = []
        self.current_step = None
        
        logger.info(f"Initialized SetupOrchestrator for database: {self.target_db}")
    
    def _validate_config(self) -> None:
        """Validate required configuration exists.
        
        Checks that all required database connection settings are present
        in the configuration loaded from .env file.
        
        Raises:
            SetupError: If any required configuration is missing, with
                detailed message listing missing items
        """
        required_configs = [
            ('db_host', 'Database host'),
            ('db_port', 'Database port'),
            ('db_user', 'Database user'),
            ('db_password', 'Database password'),
            ('db_name', 'Admin database name'),
            ('warehouse_db_name', 'Warehouse database name')
        ]
        
        missing = []
        for attr, description in required_configs:
            if not hasattr(config, attr) or not getattr(config, attr):
                missing.append(description)
        
        if missing:
            raise SetupError(
                f"Missing required configuration: {', '.join(missing)}. "
                f"Please check your .env file."
            )
    
    def _initialize_components(self) -> None:
        """Initialize all component instances."""
        logger.debug("Initializing setup components...")
        
        # Database creator (connects to admin DB)
        self.db_creator = DatabaseCreator(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            admin_db=self.admin_db,
            target_db=self.target_db
        )
        
        # Schema creator (connects to target DB)
        self.schema_creator = SchemaCreator(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.target_db
        )
        
        # Logging infrastructure
        self.logging_infrastructure = LoggingInfrastructure(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.target_db
        )
        
        logger.debug("Components initialized successfully")
    
    def _start_setup_step(self, step_name: str, step_description: str) -> int:
        """Start a setup step with logging."""
        self.current_step = step_name
        
        if self.process_logger is None:
            # Create basic logger without full infrastructure if not available yet
            try:
                self.process_logger = ProcessLogger(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.target_db
                )
            except:
                # If logging infrastructure doesn't exist yet, use basic logging
                logger.info(f"Starting setup step: {step_name}")
                return 0
        
        try:
            process_id = self.process_logger.start_process(
                process_name=f"setup_{step_name}",
                process_description=step_description,
                target_layer="setup",
                created_by="setup_orchestrator"
            )
            
            self.setup_steps.append({
                'step_name': step_name,
                'process_id': process_id,
                'start_time': time.time(),
                'status': 'RUNNING'
            })
            
            logger.info(f"Starting: {step_description}")
            return process_id
            
        except Exception as e:
            logger.warning(f"Could not log setup step {step_name}: {e}")
            return 0
    
    def _end_setup_step(self, process_id: int, status: str, error_message: str = None) -> None:
        """End a setup step with logging."""
        if self.process_logger and process_id > 0:
            try:
                self.process_logger.end_process(
                    log_id=process_id,
                    status=status,
                    error_message=error_message
                )
                
                # Update setup steps tracking
                for step in self.setup_steps:
                    if step['process_id'] == process_id:
                        step['status'] = status
                        step['end_time'] = time.time()
                        step['duration'] = step['end_time'] - step['start_time']
                        break
                        
            except Exception as e:
                logger.warning(f"Could not log setup step completion: {e}")
        
        if status == 'SUCCESS':
            logger.info("Step completed successfully")
        else:
            logger.error(f"Step failed: {error_message}")
    
    def create_database(self) -> bool:
        """Create the target warehouse database.
        
        Creates the data warehouse database with proper encoding and
        collation settings. Checks if database already exists before
        attempting creation.
        
        Returns:
            True if database created successfully or already exists,
            False otherwise
            
        Raises:
            SetupError: If database creation or verification fails
            
        Example:
            >>> orchestrator = SetupOrchestrator()
            >>> if orchestrator.create_database():
            ...     print("Database ready")
        """
        process_id = self._start_setup_step(
            "create_database",
            f"Create database {self.target_db}"
        )
        
        try:
            if self.db_creator is None:
                self._initialize_components()
            
            # Check if database already exists
            if self.db_creator.database_exists():
                logger.info(f"Database {self.target_db} already exists")
                self._end_setup_step(process_id, 'SUCCESS')
                return True
            
            # Create the database
            result = self.db_creator.create_warehouse_database()
            
            if result:
                # Verify creation
                if self.db_creator.verify_database_creation():
                    self._end_setup_step(process_id, 'SUCCESS')
                    return True
                else:
                    self._end_setup_step(process_id, 'FAILED', 'Database verification failed')
                    return False
            else:
                self._end_setup_step(process_id, 'FAILED', 'Database creation failed')
                return False
                
        except Exception as e:
            error_msg = f"Database creation failed: {e}"
            self._end_setup_step(process_id, 'FAILED', error_msg)
            logger.exception("Database creation error")
            raise SetupError(error_msg)
    
    def create_schemas(self) -> bool:
        """Create medallion architecture schemas.
        
        Creates all schemas required for medallion architecture:
        - bronze: Raw data layer
        - silver: Cleaned and conformed data layer  
        - gold: Business-ready aggregated data layer
        - logs: Audit and process logging
        
        Returns:
            True if all schemas created successfully, False otherwise
            
        Raises:
            SetupError: If schema creation fails
            
        Example:
            >>> orchestrator = SetupOrchestrator()
            >>> orchestrator.create_database()
            >>> if orchestrator.create_schemas():
            ...     print("Schemas ready")
        """
        process_id = self._start_setup_step(
            "create_schemas", 
            "Create medallion architecture schemas"
        )
        
        try:
            if self.schema_creator is None:
                self._initialize_components()
            
            # Create all schemas
            results = self.schema_creator.create_all_schemas()
            
            # Check if all schemas were created successfully
            success = all(results.values())
            
            if success:
                self._end_setup_step(process_id, 'SUCCESS')
                return True
            else:
                failed_schemas = [name for name, success in results.items() if not success]
                error_msg = f"Failed to create schemas: {', '.join(failed_schemas)}"
                self._end_setup_step(process_id, 'FAILED', error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Schema creation failed: {e}"
            self._end_setup_step(process_id, 'FAILED', error_msg)
            logger.exception("Schema creation error")
            raise SetupError(error_msg)
    
    def create_logging_infrastructure(self) -> bool:
        """Create logging infrastructure.
        
        Creates all logging and audit tables in the logs schema including:
        - process_log: Process execution tracking
        - configuration_log: Configuration change tracking
        - data_lineage: Data transformation lineage
        - error_log: Error and exception tracking
        
        After successful creation, initializes ProcessLogger for tracking
        subsequent setup steps.
        
        Returns:
            True if all logging tables created successfully, False otherwise
            
        Raises:
            SetupError: If logging infrastructure creation fails
            
        Example:
            >>> orchestrator = SetupOrchestrator()
            >>> orchestrator.create_database()
            >>> orchestrator.create_schemas()
            >>> if orchestrator.create_logging_infrastructure():
            ...     print("Logging ready")
        """
        process_id = self._start_setup_step(
            "create_logging",
            "Create logging infrastructure tables"
        )
        
        try:
            if self.logging_infrastructure is None:
                self._initialize_components()
            
            # Create all logging tables
            results = self.logging_infrastructure.create_all_tables()
            
            # Check results
            success = all(results.values())
            
            if success:
                self._end_setup_step(process_id, 'SUCCESS')
                
                # Now that logging infrastructure exists, initialize process logger
                self.process_logger = ProcessLogger(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.target_db
                )
                
                return True
            else:
                failed_tables = [name for name, success in results.items() if not success]
                error_msg = f"Failed to create logging tables: {', '.join(failed_tables)}"
                self._end_setup_step(process_id, 'FAILED', error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Logging infrastructure creation failed: {e}"
            self._end_setup_step(process_id, 'FAILED', error_msg)
            logger.exception("Logging infrastructure error")
            raise SetupError(error_msg)
    
    def create_sample_medallion_tables(self) -> bool:
        """Create sample medallion architecture tables.
        
        Creates demonstration tables across bronze/silver/gold layers using
        the medallion table template from sql.ddl:
        - bronze.crm_customers: Raw CRM customer data
        - silver.customers: Cleansed and standardized customer data
        - gold.customer_analytics: Customer analytics and KPIs
        
        These tables demonstrate the medallion architecture pattern with
        proper metadata columns, partitioning, and constraints.
        
        Returns:
            True if all sample tables created successfully, False otherwise
            
        Raises:
            SetupError: If sample table creation fails
            
        Example:
            >>> orchestrator = SetupOrchestrator()
            >>> # After database, schemas, and logging setup
            >>> if orchestrator.create_sample_medallion_tables():
            ...     print("Sample tables created")
        """
        process_id = self._start_setup_step(
            "create_sample_tables",
            "Create sample medallion architecture tables"
        )
        
        try:
            engine = self.schema_creator._get_engine()
            
            # Bronze layer - raw CRM customer data
            bronze_customer_sql = create_medallion_table_template(
                schema='bronze',
                table='crm_customers',
                business_columns=[
                    {'name': 'customer_id', 'type': 'INTEGER', 'constraints': ['NOT NULL']},
                    {'name': 'customer_name', 'type': 'VARCHAR(255)', 'constraints': ['NOT NULL']},
                    {'name': 'email', 'type': 'VARCHAR(255)'},
                    {'name': 'phone', 'type': 'VARCHAR(50)'},
                    {'name': 'address', 'type': 'TEXT'},
                    {'name': 'registration_date', 'type': 'DATE'},
                    {'name': 'customer_status', 'type': 'VARCHAR(20)'}
                ],
                partition_by='created_at',
                comment='Bronze layer - Raw CRM customer data'
            )
            
            # Silver layer - cleansed customer data  
            silver_customer_sql = create_medallion_table_template(
                schema='silver',
                table='customers',
                business_columns=[
                    {'name': 'customer_key', 'type': 'SERIAL', 'constraints': ['PRIMARY KEY']},
                    {'name': 'customer_id', 'type': 'INTEGER', 'constraints': ['NOT NULL', 'UNIQUE']},
                    {'name': 'customer_name', 'type': 'VARCHAR(255)', 'constraints': ['NOT NULL']},
                    {'name': 'email_normalized', 'type': 'VARCHAR(255)'},
                    {'name': 'phone_formatted', 'type': 'VARCHAR(20)'},
                    {'name': 'address_standardized', 'type': 'TEXT'},
                    {'name': 'registration_date', 'type': 'DATE'},
                    {'name': 'customer_status', 'type': 'VARCHAR(20)'},
                    {'name': 'data_quality_score', 'type': 'DECIMAL(3,2)'}
                ],
                comment='Silver layer - Cleansed and standardized customer data'
            )
            
            # Gold layer - customer analytics
            gold_customer_sql = create_medallion_table_template(
                schema='gold',
                table='customer_analytics',
                business_columns=[
                    {'name': 'customer_key', 'type': 'INTEGER', 'constraints': ['NOT NULL']},
                    {'name': 'customer_segment', 'type': 'VARCHAR(50)'},
                    {'name': 'lifetime_value', 'type': 'DECIMAL(10,2)'},
                    {'name': 'total_orders', 'type': 'INTEGER'},
                    {'name': 'avg_order_value', 'type': 'DECIMAL(10,2)'},
                    {'name': 'last_order_date', 'type': 'DATE'},
                    {'name': 'churn_probability', 'type': 'DECIMAL(3,2)'},
                    {'name': 'preferred_category', 'type': 'VARCHAR(100)'}
                ],
                comment='Gold layer - Customer analytics and KPIs'
            )
            
            # Execute the table creation SQL
            with engine.connect() as conn:
                conn.execute(text(bronze_customer_sql))
                conn.execute(text(silver_customer_sql))
                conn.execute(text(gold_customer_sql))
                conn.commit()
            
            logger.info("Created sample medallion tables: bronze.crm_customers, silver.customers, gold.customer_analytics")
            
            self._end_setup_step(process_id, 'SUCCESS')
            return True
            
        except Exception as e:
            error_msg = f"Sample table creation failed: {e}"
            self._end_setup_step(process_id, 'FAILED', error_msg)
            logger.exception("Sample table creation error")
            raise SetupError(error_msg)
    
    def run_complete_setup(self, include_samples: bool = True) -> Dict[str, bool]:
        """Run the complete setup process.
        
        Executes all setup steps in correct dependency order:
        1. Create database
        2. Create schemas
        3. Create logging infrastructure
        4. Create sample tables (optional)
        
        Stops on first failure and returns results for all attempted steps.
        Logs detailed progress and timing information.
        
        Args:
            include_samples: If True, create sample medallion tables
            
        Returns:
            Dictionary mapping step names to success status:
                - 'database': Database creation success
                - 'schemas': Schema creation success
                - 'logging': Logging infrastructure success
                - 'samples': Sample tables success (if include_samples=True)
                
        Example:
            >>> orchestrator = SetupOrchestrator()
            >>> results = orchestrator.run_complete_setup(include_samples=True)
            >>> 
            >>> if all(results.values()):
            ...     print("✓ Complete setup successful")
            ... else:
            ...     for step, success in results.items():
            ...         print(f"{step}: {'✓' if success else '✗'}")
        """
        logger.info("Starting data warehouse setup...")
        
        results = {}
        
        # Setup steps in dependency order
        setup_sequence = [
            ('database', self.create_database, "Create target database"),
            ('schemas', self.create_schemas, "Create medallion schemas"),
            ('logging', self.create_logging_infrastructure, "Create logging infrastructure")
        ]
        
        if include_samples:
            setup_sequence.append(
                ('samples', self.create_sample_medallion_tables, "Create sample tables")
            )
        
        # Execute setup steps
        for step_name, step_function, step_description in setup_sequence:
            try:
                logger.info(f"\nStep: {step_description}")
                result = step_function()
                results[step_name] = result
                
                if not result:
                    logger.error(f"Setup step '{step_name}' failed, stopping setup")
                    break
                    
            except Exception as e:
                logger.error(f"Setup step '{step_name}' failed with exception: {e}")
                results[step_name] = False
                break
        
        # Print setup summary
        self._print_setup_summary(results)
        
        return results
    
    def _print_setup_summary(self, results: Dict[str, bool]) -> None:
        """Print a summary of the setup results.
        
        Displays formatted summary including:
        - Success/failure status for each step
        - Overall completion percentage
        - Next steps guidance if successful
        - Step timing information
        
        Args:
            results: Dictionary mapping step names to success status
        """
        logger.info("\n" + "="*60)
        logger.info("SETUP SUMMARY")
        logger.info("="*60)
        
        total_steps = len(results)
        successful_steps = sum(1 for success in results.values() if success)
        
        for step_name, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"{step_name.ljust(20)}: {status}")
        
        logger.info("-"*60)
        logger.info(f"Completed: {successful_steps}/{total_steps} steps")
        
        if successful_steps == total_steps:
            logger.info("Setup completed successfully!")
            logger.info("\nNext steps:")
            logger.info("1. Start loading data into bronze layer")
            logger.info("2. Implement data transformations for silver layer")
            logger.info("3. Create business logic for gold layer")
            logger.info("4. Set up monitoring and alerting")
        else:
            logger.error("Setup incomplete. Please check errors above.")
            
        # Print step timing if available
        if self.setup_steps:
            logger.info("\nStep timings:")
            for step in self.setup_steps:
                if 'duration' in step:
                    duration = f"{step['duration']:.2f}s"
                    logger.info(f"  {step['step_name']}: {duration}")
    
    def rollback_setup(self, keep_database: bool = False) -> bool:
        """Rollback setup by dropping created database objects.
        
        Provides cleanup capability for development/testing. Can either drop
        the entire database or just schemas while preserving the database.
        
        Args:
            keep_database: If True, keep database and drop only schemas;
                          if False, drop entire database
            
        Returns:
            True if rollback successful, False otherwise
            
        Warning:
            This is a destructive operation. All data will be lost.
            Use with caution, especially in production environments.
            
        Example:
            >>> orchestrator = SetupOrchestrator()
            >>> 
            >>> # Drop everything including database
            >>> orchestrator.rollback_setup(keep_database=False)
            >>> 
            >>> # Drop only schemas, keep database
            >>> orchestrator.rollback_setup(keep_database=True)
        """
        logger.info("Starting setup rollback...")
        
        try:
            if not keep_database and self.db_creator:
                # Drop the entire database
                success = self.db_creator.drop_database()
                if success:
                    logger.info("Database dropped successfully")
                    return True
                else:
                    logger.error("Failed to drop database")
                    return False
            else:
                # Drop schemas using schema_creator
                if self.schema_creator:
                    success = self.schema_creator.drop_all_schemas()
                    if success:
                        logger.info("Schemas dropped successfully")
                        return True
                    else:
                        logger.error("Failed to drop schemas")
                        return False
                return True
                    
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            logger.exception("Rollback error details")
            return False
        
        return True


def main():
    """Command-line interface for setup orchestrator.
    
    Provides CLI for running complete setup or rollback operations with
    optional flags for controlling behavior.
    
    Arguments:
        --no-samples: Skip creating sample medallion tables
        --rollback: Perform rollback instead of setup
        --keep-db: During rollback, keep database (drop schemas only)
        --verbose: Enable DEBUG level logging
    
    Returns:
        Exit code: 0 for success, 1 for failure
        
    Example:
        # Complete setup with samples
        python -m setup.setup_orchestrator
        
        # Setup without samples
        python -m setup.setup_orchestrator --no-samples
        
        # Rollback keeping database
        python -m setup.setup_orchestrator --rollback --keep-db
        
        # Verbose output
        python -m setup.setup_orchestrator --verbose
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Data Warehouse Setup Orchestrator",
        epilog="Example: python -m setup.setup_orchestrator --no-samples"
    )
    parser.add_argument(
        '--no-samples',
        action='store_true',
        help='Skip creating sample medallion tables'
    )
    parser.add_argument(
        '--rollback',
        action='store_true',
        help='Rollback setup (drop database/schemas)'
    )
    parser.add_argument(
        '--keep-db',
        action='store_true',
        help='Keep database during rollback (only drop schemas)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level if verbose
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        orchestrator = SetupOrchestrator()
        
        if args.rollback:
            success = orchestrator.rollback_setup(keep_database=args.keep_db)
            return 0 if success else 1
        else:
            results = orchestrator.run_complete_setup(
                include_samples=not args.no_samples
            )
            
            # Return 0 if all steps successful, 1 otherwise
            return 0 if all(results.values()) else 1
        
    except SetupError as e:
        logger.error(f"Setup configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Setup orchestrator failed: {e}")
        logger.exception("Fatal error details")
        return 1


if __name__ == '__main__':
    exit(main())
