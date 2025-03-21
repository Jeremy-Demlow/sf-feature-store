"""This module provides Snowflake connection management for a feature store framework, featuring configuration from environment variables or YAML files. It implements a robust SnowflakeConnection class with retry logic, connection testing, and smart session acquisition through the get_connection() function. The module integrates with the logging and exception systems to ensure reliable database connectivity with appropriate error handling."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/05_connection.ipynb.

# %% ../nbs/05_connection.ipynb 2
from __future__ import annotations
from typing import Optional, Dict, Any, Tuple, Union
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.context import get_active_session
import os
from dataclasses import dataclass
from pathlib import Path
import yaml
from tenacity import retry, stop_after_attempt, wait_exponential
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import warnings

# Import our new modules
from .exceptions import ConnectionError, ConfigurationError
from .logging import logger
from .config import BaseModel, Field

# Suppress the specific Pydantic warning about schema
warnings.filterwarnings("ignore", message="Field name \"schema\" .* shadows an attribute in parent \"BaseModel\"")

# %% auto 0
__all__ = ['ConnectionConfig', 'SnowflakeConnection', 'get_connection']

# %% ../nbs/05_connection.ipynb 3
class ConnectionConfig(BaseModel):
    """Configuration for Snowflake connection"""
    user: str
    password: Optional[str] = None
    account: str
    role: str = Field("DATA_SCIENTIST", description="Snowflake role")
    warehouse: str = Field("DS_WH_XS", description="Default warehouse")
    database: Optional[str] = Field(None, description="Default database")
    schema: Optional[str] = Field(None, description="Default schema")
    private_key_path: Optional[Path] = None
    private_key_pem: Optional[str] = None
    authenticator: Optional[str] = None
    query_tag: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_env(cls) -> ConnectionConfig:
        """Create connection config from environment variables"""
        try:
            config = {}
            env_vars = {
                'SNOWFLAKE_ACCOUNT': 'account',
                'SNOWFLAKE_USER': 'user',
                'SNOWFLAKE_PASSWORD': 'password',
                'SNOWFLAKE_ROLE': 'role',
                'SNOWFLAKE_WAREHOUSE': 'warehouse',
                'SNOWFLAKE_DATABASE': 'database',
                'SNOWFLAKE_SCHEMA': 'schema',
                'SNOWFLAKE_PRIVATE_KEY_PATH': 'private_key_path',
                'SNOWFLAKE_AUTHENTICATOR': 'authenticator'
            }

            for env_var, config_key in env_vars.items():
                if value := os.getenv(env_var):
                    config[config_key] = value
                    
            # Ensure required fields are present
            if 'account' not in config:
                raise ConfigurationError("Missing required environment variable: SNOWFLAKE_ACCOUNT")
            if 'user' not in config:
                raise ConfigurationError("Missing required environment variable: SNOWFLAKE_USER")
            
            # Check that at least one authentication method is provided
            if not any(k in config for k in ['password', 'private_key_path', 'authenticator']):
                raise ConfigurationError("No authentication method provided via environment variables")
                
            return cls(**config)
            
        except Exception as e:
            if not isinstance(e, ConfigurationError):
                raise ConfigurationError(f"Error creating connection config from environment: {str(e)}")
            raise
    
    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> ConnectionConfig:
        """Create connection config from YAML file"""
        try:
            with open(path) as f:
                yaml_config = yaml.safe_load(f)
                
            # Support both top-level config and nested under 'snowflake' key
            config = yaml_config.get('snowflake', yaml_config)
            
            # Environment variables override YAML
            env_vars = {
                'SNOWFLAKE_ACCOUNT': 'account',
                'SNOWFLAKE_USER': 'user',
                'SNOWFLAKE_PASSWORD': 'password',
                'SNOWFLAKE_ROLE': 'role',
                'SNOWFLAKE_WAREHOUSE': 'warehouse',
                'SNOWFLAKE_DATABASE': 'database',
                'SNOWFLAKE_SCHEMA': 'schema',
                'SNOWFLAKE_PRIVATE_KEY_PATH': 'private_key_path',
                'SNOWFLAKE_AUTHENTICATOR': 'authenticator'
            }

            for env_var, config_key in env_vars.items():
                if value := os.getenv(env_var):
                    config[config_key] = value
                    
            return cls(**config)
        except Exception as e:
            raise ConfigurationError(f"Error loading config from {path}: {str(e)}")


# %% ../nbs/05_connection.ipynb 4
class SnowflakeConnection:
    """Manages Snowflake connection and configuration"""
    def __init__(self, 
                 session: Session,
                 warehouse: Optional[str] = None,
                 database: Optional[str] = None,
                 schema: Optional[str] = None,
                 config: Optional[ConnectionConfig] = None):
        """Initialize Snowflake connection
        
        Args:
            session: Active Snowflake session
            warehouse: Override default warehouse
            database: Override default database
            schema: Override default schema
            config: Original connection configuration (for caching)
        """
        self.session = session
        self.warehouse = warehouse or session.get_current_warehouse()
        self.database = database or session.get_current_database()
        self.schema = schema or session.get_current_schema()
        self._config = config
        self._session_cache: Dict[Tuple[str, str, str], Session] = {}
        
        # Add the initial session to the cache if config is provided
        if config and self.database:
            cache_key = (config.role, self.warehouse, self.database)
            self._session_cache[cache_key] = session
            
        logger.info(f"Initialized connection to {self.database}.{self.schema}")
        
    @classmethod
    def from_config(cls, config: ConnectionConfig) -> SnowflakeConnection:
        """Create connection from config object"""
        try:
            # Prepare connection parameters
            params = {
                "account": config.account,
                "user": config.user,
                "role": config.role,
                "warehouse": config.warehouse,
            }

            if config.database:
                params["database"] = config.database
            if config.schema:
                params["schema"] = config.schema
            
            # Select authentication method
            if config.authenticator:
                params["authenticator"] = config.authenticator
            elif config.private_key_path or config.private_key_pem:
                # Load and format private key
                if config.private_key_pem:
                    key_data = config.private_key_pem.encode()
                elif config.private_key_path:
                    with open(config.private_key_path, "rb") as key_file:
                        key_data = key_file.read()
                        
                p_key = serialization.load_pem_private_key(
                    key_data,
                    password=None,
                    backend=default_backend()
                )
                params["private_key"] = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            elif config.password:
                params["password"] = config.password
            else:
                raise ConnectionError(
                    "No authentication method provided. Please provide either "
                    "authenticator, private_key, or password."
                )

            # Create session
            session = Session.builder.configs(params).create()
            
            # Set query tag if provided
            if config.query_tag:
                session.query_tag = config.query_tag
                
            return cls(session, config=config)
        except Exception as e:
            raise ConnectionError(f"Failed to create session: {str(e)}")
    
    @classmethod
    def from_env(cls) -> SnowflakeConnection:
        """Create connection from environment variables"""
        return cls.from_config(ConnectionConfig.from_env())
    
    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> SnowflakeConnection:
        """Create connection from YAML config file"""
        return cls.from_config(ConnectionConfig.from_yaml(path))
    
    def get_session(
        self,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        use_cache: bool = True
    ) -> Session:
        """Get or create a Snowflake session with specified parameters
        
        Args:
            role: Override default role
            warehouse: Override default warehouse
            database: Override default database
            schema: Override default schema
            use_cache: Whether to use session caching
            
        Returns:
            A Snowflake session
        """
        # Use existing config or create minimal one based on current connection
        config = self._config or ConnectionConfig(
            user=self.session.get_current_user(),
            account=self.session.get_current_account(),
            role=role or self.session.get_current_role(),
            warehouse=warehouse or self.warehouse,
            database=database or self.database,
            schema=schema or self.schema
        )
        
        final_role = role or config.role
        final_warehouse = warehouse or self.warehouse
        final_database = database or self.database
        
        if use_cache:
            cache_key = (final_role, final_warehouse, final_database)
            if cache_key in self._session_cache:
                session = self._session_cache[cache_key]
                if schema:
                    session.use_schema(schema)
                return session
        
        # Create new session with the updated parameters
        new_config = config.model_copy(update={
            "role": final_role,
            "warehouse": final_warehouse,
            "database": final_database,
            "schema": schema
        })
        
        # Create a new connection with the desired parameters
        new_conn = self.from_config(new_config)
        
        # Cache the session if requested
        if use_cache:
            cache_key = (final_role, final_warehouse, final_database)
            self._session_cache[cache_key] = new_conn.session
            logger.info(f"Cached new session for {cache_key}")
            
        return new_conn.session
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda retry_state: logger.error(
            f"Failed after {retry_state.attempt_number} attempts: {retry_state.outcome.exception()}"
        )
    )
    def execute_query(self, query: str) -> Any:
        """Execute query with retry logic"""
        try:
            return self.session.sql(query).collect()
        except Exception as e:
            raise ConnectionError(f"Query execution failed: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test if connection is working"""
        try:
            self.execute_query('SELECT 1')
            logger.info("Connection test successful")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
            
    def close(self, close_all: bool = False):
        """Close the Snowflake session(s)
        
        Args:
            close_all: Whether to close all cached sessions
        """
        try:
            if close_all:
                # Close all cached sessions
                for session in self._session_cache.values():
                    try:
                        session.close()
                    except Exception as e:
                        logger.warning(f"Error closing cached session: {str(e)}")
                self._session_cache.clear()
                logger.info("All sessions closed successfully")
            else:
                # Close only the main session
                if self.session:
                    self.session.close()
                    logger.info("Main session closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(close_all=True)


# %% ../nbs/05_connection.ipynb 5
def get_connection(
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    create_objects: bool = True  # Add parameter to control object creation
) -> SnowflakeConnection:
    """Get Snowflake connection from active session or environment
    
    First tries to get active session, falls back to environment variables.
    Optional parameters override both active session and environment variables.
    
    Args:
        database: Optional database to use
        schema: Optional schema to use
        warehouse: Optional warehouse to use
        role: Optional role to use
        create_objects: Whether to create database/schema if they don't exist
    
    Returns:
        SnowflakeConnection object
    
    Raises:
        ConnectionError: If connection cannot be established
    """
    try:
        # Try to get active session (e.g., in Snowflake worksheet)
        session = get_active_session()
        logger.info("Using active Snowflake session")
        conn = SnowflakeConnection(session)
        
        # Override with provided parameters if any
        if any([database, schema, warehouse, role]):
            # Use the existing session but update parameters
            if role:
                conn.session.sql(f"USE ROLE {role}").collect()
                
            if warehouse:
                conn.session.sql(f"USE WAREHOUSE {warehouse}").collect()
                conn.warehouse = warehouse
                
            if database:
                if create_objects:
                    # Create database if it doesn't exist
                    conn.session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
                
                # Now use the database
                conn.session.sql(f"USE DATABASE {database}").collect()
                conn.database = database
                
            if schema and conn.database:
                if create_objects:
                    # Create schema if it doesn't exist
                    conn.session.sql(f"CREATE SCHEMA IF NOT EXISTS {conn.database}.{schema}").collect()
                
                # Now use the schema
                conn.session.sql(f"USE SCHEMA {conn.database}.{schema}").collect()
                conn.schema = schema
            
            # Log updated connection info
            logger.info(f"Using role: {conn.session.get_current_role()}, "
                       f"warehouse: {conn.session.get_current_warehouse()}, "
                       f"database: {conn.database}, schema: {conn.schema}")
            
        return conn
    
    except SnowparkSessionException:
        # Fall back to environment variables
        logger.info("No active session found, creating new connection from environment")
        
        # Create a config with environment variables
        config = ConnectionConfig.from_env()
        
        # Override with provided parameters
        if role:
            config.role = role
        if warehouse:
            config.warehouse = warehouse
        if database:
            config.database = database
        if schema:
            config.schema = schema
            
        # Create connection with the config
        conn = SnowflakeConnection.from_config(config)
        
        # Now handle database and schema creation if requested
        if create_objects and database:
            try:
                # Create database if it doesn't exist
                conn.session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
                
                # Use the database
                conn.session.sql(f"USE DATABASE {database}").collect()
                conn.database = database
                
                if schema:
                    # Create schema if it doesn't exist
                    conn.session.sql(f"CREATE SCHEMA IF NOT EXISTS {conn.database}.{schema}").collect()
                    
                    # Use the schema
                    conn.session.sql(f"USE SCHEMA {conn.database}.{schema}").collect()
                    conn.schema = schema
            except Exception as e:
                logger.warning(f"Error creating database/schema: {str(e)}")
                # Continue anyway, might be permissions issue but DB/schema might already exist
        
        logger.info(f"Using role: {conn.session.get_current_role()}, "
                   f"warehouse: {conn.session.get_current_warehouse()}, "
                   f"database: {conn.database}, schema: {conn.schema}")
        return conn
