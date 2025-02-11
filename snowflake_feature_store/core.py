"""Fill in a module description here"""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/00_core.ipynb.

# %% ../nbs/00_core.ipynb 4
from __future__ import annotations
from typing import Union, List, Dict, Optional, Protocol, Callable
from dataclasses import dataclass
from fastcore.basics import listify
import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame, Session


# %% auto 0
__all__ = ['defaults', 'FeatureStoreDefaults', 'create_version', 'create_feature_view_name', 'SQLFormatter']

# %% ../nbs/00_core.ipynb 5
@dataclass
class FeatureStoreDefaults:
    "Default settings for feature store operations"
    version_prefix: str = 'V'
    refresh_frequency: str = '1 day'
    feature_view_prefix: str = 'FV'
    creation_mode: str = 'CREATE_IF_NOT_EXIST'
    refresh_mode: str = 'FULL'


# %% ../nbs/00_core.ipynb 6
defaults = FeatureStoreDefaults()


# %% ../nbs/00_core.ipynb 8
def create_version(major:int, minor:int=0) -> str:
    "Create a standardized version string (e.g., 'V1_0' for major=1, minor=0)"
    return f"{defaults.version_prefix}{major}_{minor}"

# %% ../nbs/00_core.ipynb 9
def create_feature_view_name(
    domain:str,
    entity:str,
    feature_type:str
) -> str:
    """Create standardized feature view name
    
    Args:
        domain: Business domain (e.g., 'RETAIL', 'FINANCE')
        entity: Main entity name (e.g., 'CUSTOMER', 'PRODUCT')
        feature_type: Type of features (e.g., 'BEHAVIOR', 'PROFILE')
        
    Returns:
        Formatted name like 'FV_RETAIL_CUSTOMER_BEHAVIOR'
    
    Example:
        >>> create_feature_view_name('RETAIL', 'CUSTOMER', 'BEHAVIOR')
        'FV_RETAIL_CUSTOMER_BEHAVIOR'
    """
    parts = [defaults.feature_view_prefix, entity, feature_type]
    if domain: parts.insert(1, domain)
    return "_".join(part.upper() for part in parts)


# %% ../nbs/00_core.ipynb 11
class SQLFormatter:
    "Utilities for SQL query formatting and analysis"
    
    @staticmethod
    def format_sql(query:str, subq_to_cte:bool=False) -> str:
        """Format SQL query with proper indentation
        
        Args:
            query: SQL query string to format
            subq_to_cte: If True, convert subqueries to CTEs
            
        Returns:
            Formatted SQL string
            
        Example:
            >>> sql = "SELECT a, b FROM (SELECT * FROM table)"
            >>> print(SQLFormatter.format_sql(sql, True))
            WITH _q1 AS (
              SELECT *
              FROM table
            )
            SELECT a, b
            FROM _q1
        """
        import sqlglot
        expression = sqlglot.parse_one(query)
        if subq_to_cte:
            query = sqlglot.optimizer.optimizer.eliminate_subqueries(expression).sql()
        return sqlglot.transpile(query, read='snowflake', pretty=True)[0]
    
    @staticmethod
    def extract_table_names(query:str) -> List[str]:
        """Extract all table names from a SQL query
        
        Args:
            query: SQL query to analyze
            
        Returns:
            List of table names found in query
            
        Example:
            >>> sql = "SELECT * FROM table1 JOIN table2"
            >>> SQLFormatter.extract_table_names(sql)
            ['table1', 'table2']
        """
        import sqlglot
        from sqlglot.expressions import Table
        tables = set()
        for expr in sqlglot.parse(query):
            for table in expr.find_all(Table):
                tables.add(table.name)
        return list(tables)

