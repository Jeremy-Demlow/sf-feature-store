"""Fill in a module description here"""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/01_connection.ipynb.

# %% ../nbs/01_connection.ipynb 2
from __future__ import annotations
from typing import Optional, Dict, Any
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.context import get_active_session
import os
from dataclasses import dataclass
from pathlib import Path
import yaml


# %% auto 0
__all__ = ['ConnectionConfig', 'SnowflakeConnection', 'get_connection']

# %% ../nbs/01_connection.ipynb 3
@dataclass
class ConnectionConfig:
    "Configuration for Snowflake connection"
    user: str
    password: str
    account: str
    role: str = 'DATA_SCIENTIST'
    warehouse: str = 'DS_WH_XS'
    database: str = 'DATASCIENCE'
    schema: str = 'SIMPLE_ML_SCHEMA_2'
    
    @classmethod
    def from_env(cls) -> ConnectionConfig:
        "Create connection config from environment variables"
        return cls(
            user=os.getenv('SNOWFLAKE_USER', ''),
            password=os.getenv('SNOWFLAKE_PASSWORD', ''),
            account=os.getenv('SNOWFLAKE_ACCOUNT', ''),
            role=os.getenv('SNOWFLAKE_ROLE', 'DATA_SCIENTIST'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'DS_WH_XS'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'DATASCIENCE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'SIMPLE_ML_SCHEMA_2')
        )
    
    @classmethod
    def from_yaml(cls, path: str|Path) -> ConnectionConfig:
        "Create connection config from YAML file"
        with open(path) as f:
            config = yaml.safe_load(f)
        return cls(**config)
    
    def to_dict(self) -> Dict[str, str]:
        "Convert config to dictionary for Snowflake session"
        return {
            'user': self.user,
            'password': self.password,
            'account': self.account,
            'role': self.role,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema
        }


# %% ../nbs/01_connection.ipynb 4
class SnowflakeConnection:
    "Manages Snowflake connection and configuration"
    def __init__(self, 
                 session: Session,
                 warehouse: Optional[str] = None,
                 database: Optional[str] = None,
                 schema: Optional[str] = None):
        """Initialize Snowflake connection
        
        Args:
            session: Active Snowflake session
            warehouse: Override default warehouse
            database: Override default database
            schema: Override default schema
        """
        self.session = session
        self.warehouse = warehouse or session.get_current_warehouse()
        self.database = database or session.get_current_database()
        self.schema = schema or session.get_current_schema()
        
    @classmethod
    def from_config(cls, config: ConnectionConfig) -> SnowflakeConnection:
        "Create connection from config object"
        session = Session.builder.configs(config.to_dict()).create()
        return cls(session)
    
    @classmethod
    def from_env(cls) -> SnowflakeConnection:
        "Create connection from environment variables"
        return cls.from_config(ConnectionConfig.from_env())
    
    @classmethod
    def from_yaml(cls, path: str|Path) -> SnowflakeConnection:
        "Create connection from YAML config file"
        return cls.from_config(ConnectionConfig.from_yaml(path))
    
    def test_connection(self) -> bool:
        "Test if connection is working"
        try:
            self.session.sql('SELECT 1').collect()
            return True
        except Exception:
            return False
            
    def close(self):
        "Close the Snowflake session"
        if self.session:
            self.session.close()


# %% ../nbs/01_connection.ipynb 5
def get_connection() -> SnowflakeConnection:
    """Get Snowflake connection from active session or environment
    
    First tries to get active session, falls back to environment variables
    
    Returns:
        SnowflakeConnection object
    
    Example:
        >>> conn = get_connection()
        >>> df = conn.session.table('MY_TABLE')
    """
    try:
        # Try to get active session (e.g., in Snowflake worksheet)
        session = get_active_session()
        return SnowflakeConnection(session)
    except SnowparkSessionException:
        # Fall back to environment variables
        return SnowflakeConnection.from_env()

