"""
================================================
Configuration management for the data warehouse.
================================================

Loads all configuration from environment variables (.env file) and provides
a centralized Config singleton for application-wide access.

The configuration system ensures:
- Single source of truth for all settings
- Type conversion and validation
- Environment-specific configurations (dev/staging/prod)
- Secure handling of sensitive credentials

Example:
    >>> from core.config import config
    >>> 
    >>> # Database connection
    >>> engine_url = config.get_connection_string(use_warehouse=True)
    >>> 
    >>> # Access individual settings
    >>> print(f"Host: {config.db_host}, Port: {config.db_port}").
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


@dataclass
class DatabaseConfig:
    """Database configuration settings.
    
    Attributes:
        host: PostgreSQL server hostname or IP address
        port: PostgreSQL server port number
        user: Database username
        password: Database password
        database: Default/admin database name
        warehouse_db: Target data warehouse database name
    """
    
    host: str
    port: int
    user: str
    password: str
    database: str
    warehouse_db: str
    
    def get_connection_string(self, use_warehouse: bool = False) -> str:
        """Get PostgreSQL connection string.
        
        Args:
            use_warehouse: If True, use warehouse database; otherwise default database
            
        Returns:
            SQLAlchemy-compatible PostgreSQL connection string
        """
        db_name = self.warehouse_db if use_warehouse else self.database
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{db_name}"
    
    def get_connection_params(self, use_warehouse: bool = False) -> dict:
        """Get connection parameters as dictionary.
        
        Args:
            use_warehouse: If True, use warehouse database; otherwise default database
            
        Returns:
            Dictionary with keys: host, port, user, password, database
        """
        db_name = self.warehouse_db if use_warehouse else self.database
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': db_name
        }


@dataclass
class ProjectConfig:
    """Project-wide configuration settings.
    
    Attributes:
        project_root: Absolute path to project root directory
        data_dir: Path to datasets directory
        logs_dir: Path to logs directory
        medallion_dir: Path to medallion architecture directory
    """
    
    project_root: Path
    data_dir: Path
    logs_dir: Path
    medallion_dir: Path
    
    def ensure_directories(self) -> None:
        """Create project directories if they don't exist.
        
        Creates data_dir, logs_dir, and medallion_dir with parent directories
        as needed. Safe to call multiple times (idempotent).
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.medallion_dir.mkdir(parents=True, exist_ok=True)


class Config:
    """Centralized configuration manager.
    
    Provides access to all configuration settings loaded from environment
    variables (.env file) and manages project directory structure.
    
    Attributes:
        db: DatabaseConfig instance with database connection settings
        project: ProjectConfig instance with project directory paths
    
    Properties:
        db_host: Database server hostname
        db_port: Database server port
        db_user: Database username
        db_password: Database password
        db_name: Default/admin database name
        warehouse_db_name: Warehouse database name
    
    Example:
        >>> config = Config()
        >>> conn_str = config.get_connection_string(use_warehouse=True)
        >>> print(f"Connecting to {config.db_host}:{config.db_port}")
    """
    
    def __init__(self):
        """Initialize configuration from environment variables.
        
        Loads .env file and creates DatabaseConfig and ProjectConfig instances.
        Automatically determines project root from module location.
        """
        # Database configuration
        self.db = DatabaseConfig(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', ''),
            database=os.getenv('POSTGRES_DB', 'postgres'),
            warehouse_db=os.getenv('WAREHOUSE_DB', 'sql_retail_analytics_warehouse')
        )
        
        # Project structure
        project_root = Path(__file__).parent.parent
        self.project = ProjectConfig(
            project_root=project_root,
            data_dir=project_root / 'datasets',
            logs_dir=project_root / 'logs',
            medallion_dir=project_root / 'medallion'
        )
    
    @property
    def db_host(self) -> str:
        """Get database server hostname."""
        return self.db.host
    
    @property
    def db_port(self) -> int:
        """Get database server port number."""
        return self.db.port
    
    @property
    def db_user(self) -> str:
        """Get database username."""
        return self.db.user
    
    @property
    def db_password(self) -> str:
        """Get database password."""
        return self.db.password
    
    @property
    def db_name(self) -> str:
        """Get default/admin database name."""
        return self.db.database
    
    @property
    def warehouse_db_name(self) -> str:
        """Get data warehouse database name."""
        return self.db.warehouse_db
    
    def get_connection_string(self, use_warehouse: bool = False) -> str:
        """Get database connection string.
        
        Args:
            use_warehouse: If True, connect to warehouse database; otherwise default
            
        Returns:
            SQLAlchemy-compatible PostgreSQL connection string
            
        Example:
            >>> config = Config()
            >>> warehouse_url = config.get_connection_string(use_warehouse=True)
            >>> admin_url = config.get_connection_string(use_warehouse=False)
        """
        return self.db.get_connection_string(use_warehouse=use_warehouse)
    
    def get_connection_params(self, use_warehouse: bool = False) -> dict:
        """Get database connection parameters.
        
        Args:
            use_warehouse: If True, use warehouse database; otherwise default
            
        Returns:
            Dictionary with keys: host, port, user, password, database
            
        Example:
            >>> config = Config()
            >>> params = config.get_connection_params(use_warehouse=True)
            >>> engine = create_engine(**params)
        """
        return self.db.get_connection_params(use_warehouse=use_warehouse)


# Global configuration instance
config = Config()
