"""This module provides utilities for demonstrating and testing the Snowflake feature store framework. It includes functions for generating synthetic customer data, creating example feature configurations, and running an end-to-end demonstration of the feature store workflow. The generate_demo_data function creates realistic customer behavior datasets with configurable parameters, while create_feature_configs establishes properly structured feature definitions. The comprehensive run_end_to_end_example function demonstrates the complete feature engineering pipeline from data generation through entity creation, feature transformation, and training dataset generation."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/05_example_functions.ipynb.

# %% ../nbs/05_example_functions.ipynb 2
from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import *
from datetime import datetime, timedelta
import random

from .connection import get_connection
from .manager import feature_store_session
from snowflake_feature_store.config import (
    FeatureViewConfig, FeatureConfig, RefreshConfig, 
    FeatureValidationConfig
)
from snowflake.snowpark.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, LongType, TimestampType
)

from snowflake_feature_store.transforms import (
    Transform, TransformConfig, moving_agg, 
    fill_na, date_diff
)
from .logging import logger

# %% auto 0
__all__ = ['generate_demo_data', 'get_example_data', 'create_feature_configs', 'run_end_to_end_example']

# %% ../nbs/05_example_functions.ipynb 3
def generate_demo_data(
    session: Session, 
    schema: str,
    num_customers: int = 1000, 
    ltv_multiplier: float = 1.0, 
    session_length_multiplier: float = 1.0, 
    start_date: Optional[datetime] = None,
    num_days: int = 30,
    table_type: str = ''
) -> None:
    """Generate synthetic customer data for demonstration
    
    Args:
        session: Snowflake session
        schema: Schema to create tables in
        num_customers: Number of customers to generate
        ltv_multiplier: Multiplier for lifetime value
        session_length_multiplier: Multiplier for session length
        start_date: Start date for data (default: 30 days ago)
        num_days: Number of days of data to generate
    """
    try:
        start_date = start_date or (datetime.now() - timedelta(days=num_days))
        
        # Create functions for generating data
        def generate_customer_row(customer_id: int, date: datetime) -> List:
            """Generate a single customer data row"""
            # Basic metrics
            ltv = random.uniform(100, 75000) / 100 * ltv_multiplier
            session_length = (ltv / 100 + random.uniform(0, 5)) * session_length_multiplier
            
            # Derived metrics
            time_on_app = ltv / 100 + random.uniform(1, 7)
            time_on_website = ltv / 100 + random.uniform(3, 7)
            transactions = max(1, int(ltv / 100))
            
            return [
                f'C{customer_id}',
                date.strftime('%Y-%m-%d'),
                ltv,
                session_length if random.random() > 0.2 else None,  # 20% null
                time_on_app,
                time_on_website,
                transactions
            ]
        
        # Generate data for each day
        data = []
        for day in range(num_days):
            date = start_date + timedelta(days=day)
            # Generate data for active customers (80% active each day)
            active_customers = random.sample(
                range(num_customers), 
                k=int(num_customers * 0.8)
            )
            for cust_id in active_customers:
                data.append(generate_customer_row(cust_id, date))
        
       # Define schema
        schema_struct = StructType([
            StructField('CUSTOMER_ID', StringType()),
            StructField('DATE', DateType()),
            StructField('LIFE_TIME_VALUE', DoubleType()),
            StructField('SESSION_LENGTH', DoubleType()),
            StructField('TIME_ON_APP', DoubleType()),
            StructField('TIME_ON_WEBSITE', DoubleType()),
            StructField('TRANSACTIONS', LongType())
        ])
        
        # Create DataFrame
        df = session.create_dataframe(
            data,
            schema=schema_struct  # Pass the StructType directly
        )
        
        # Save to table
        table_name = f"{session.get_current_database()}.{schema}.CUSTOMER_ACTIVITY{'' if table_type == '' else '_' + table_type.upper()}"
        df.write.mode('overwrite').save_as_table(table_name)
        
        logger.info(f"Generated {len(data)} rows of demo data in {table_name}")
        logger.debug("Schema:")
        for field in df.schema.fields:
            logger.debug(f"{field.name}: {field.datatype}")
        
    except Exception as e:
        logger.error(f"Error generating demo data: {str(e)}")
        raise

# %% ../nbs/05_example_functions.ipynb 4
def get_example_data(
    session: Session,
    schema: str,
    num_customers: int = 100,
    ltv_multiplier: float = 1.0, 
    session_length_multiplier: float = 1.0, 
    start_date: Optional[datetime] = None,
    num_days: int = 30,
    table_type: str = ''
) -> DataFrame:
    """Load or generate example customer data
    
    Args:
        session: Active Snowflake session
        schema: Schema where tables are located
        num_customers: Number of customers if generating new data
        
    Returns:
        DataFrame with customer activity data
    """
    try:
        # Generate fresh data
        generate_demo_data(
            session, 
            schema,
            num_customers = num_customers,
            ltv_multiplier = ltv_multiplier, 
            session_length_multiplier = session_length_multiplier, 
            start_date = start_date,
            num_days = num_days,
            table_type = table_type
        )
        
        # Load the data
        df = session.table(
            f"{session.get_current_database()}.{schema}.CUSTOMER_ACTIVITY"
        )
        
        # Show sample and schema
        logger.info("\nSample Data:")
        df.show(5)
        
        logger.info("\nSchema:")
        for field in df.schema.fields:
            logger.info(f"{field.name}: {field.datatype}")
            
        return df
        
    except Exception as e:
        logger.error(f"Error getting example data: {str(e)}")
        raise



# %% ../nbs/05_example_functions.ipynb 5
def create_feature_configs() -> Dict[str, FeatureConfig]:
    """Create example feature configurations"""
    try:
        # Basic features
        configs = {
            "LIFE_TIME_VALUE": FeatureConfig(
                name="LIFE_TIME_VALUE",
                description="Customer lifetime value",
                validation=FeatureValidationConfig(
                    null_threshold=0.1,
                    range_check=True,
                    min_value=0
                )
            ),
            "SESSION_LENGTH": FeatureConfig(
                name="SESSION_LENGTH",
                description="Session length in minutes",
                validation=FeatureValidationConfig(
                    null_threshold=0.3,
                    range_check=True,
                    min_value=0
                )
            ),
            "TRANSACTIONS": FeatureConfig(
                name="TRANSACTIONS",
                description="Number of transactions",
                validation=FeatureValidationConfig(
                    null_threshold=0.1,
                    range_check=True,
                    min_value=0
                )
            )
        }
        
        # Derived features - match names with transform output
        configs.update({
            "AVG_SESSION_LENGTH_7": FeatureConfig(  # Match transform output name
                name="AVG_SESSION_LENGTH_7",
                description="7-day average session length",
                validation=FeatureValidationConfig(
                    null_threshold=0.1,
                    range_check=True,
                    min_value=0
                ),
                dependencies=["SESSION_LENGTH"]
            ),
            "SUM_TRANSACTIONS_7": FeatureConfig(  # Match transform output name
                name="SUM_TRANSACTIONS_7",
                description="7-day total transactions",
                validation=FeatureValidationConfig(
                    null_threshold=0.1,
                    range_check=True,
                    min_value=0
                ),
                dependencies=["TRANSACTIONS"]
            )
        })
        
        return configs
        
    except Exception as e:
        logger.error(f"Error creating feature configs: {str(e)}")
        raise


# %% ../nbs/05_example_functions.ipynb 6
def run_end_to_end_example(
    metrics_path: Optional[str] = None,
    num_customers: int = 100
) -> None:
    """Run end-to-end feature store example
    
    Args:
        metrics_path: Optional path to save metrics
        num_customers: Number of customers to generate
    """
    try:
        # Get connection
        conn = get_connection()
        
        # Use feature store session
        with feature_store_session(
            conn, metrics_path=metrics_path
        ) as manager:
            # Get example data
            df = get_example_data(
                conn.session,
                manager.connection.schema,
                num_customers
            )
            df = df.with_column(
                'DATE',
                F.to_date(F.col('DATE'))
            )

            # 1. Add Customer Entity
            manager.add_entity(
                name="CUSTOMER",
                join_keys=["CUSTOMER_ID"],
                description="Customer entity for retail domain"
            )
            
            # 2. Create Feature Configurations
            feature_configs = create_feature_configs()
            
            # 3. Create Feature View Config
            config = FeatureViewConfig(
                name="customer_behavior",
                domain="RETAIL",
                entity="CUSTOMER",
                feature_type="BEHAVIOR",
                refresh=RefreshConfig(frequency="1 day"),
                features=feature_configs,
                description="Customer behavior features",
                timestamp_col="DATE"
            )
            
            # 4. Create transforms
            transform_config = TransformConfig(
                name="customer_metrics",
                null_threshold=0.1,
                expected_types=['DECIMAL', 'DOUBLE', 'NUMBER']
            )
            
            transforms = [
                # Fill nulls in session length
                fill_na(['SESSION_LENGTH'], fill_value=0),
                
                # Calculate rolling metrics
                moving_agg(
                    cols=['SESSION_LENGTH', 'TRANSACTIONS'],
                    window_sizes=[7],
                    agg_funcs=['AVG', 'SUM'],
                    partition_by=['CUSTOMER_ID'],
                    order_by=['DATE'],
                    config=transform_config
                )
            ]
            
            # 5. Create Feature View
            feature_view = manager.add_feature_view(
                config=config,
                df=df,
                entity_name="CUSTOMER",
                transforms=transforms,
                collect_stats=True
            )
            
            # Show feature statistics
            logger.info("\nFeature Statistics:")
            for feature_name, stats in manager.feature_stats[config.name].items():
                logger.info(f"\n{feature_name}:")
                logger.info(str(stats))
            
        
            # 6. Generate Training Dataset
            spine_df = df.select(
                F.col('CUSTOMER_ID'),
                F.col('DATE')
            )
            training_data = manager.get_features(
                spine_df=spine_df,
                feature_views=[config],
                label_cols=['LIFE_TIME_VALUE'],
                spine_timestamp_col='DATE'
            )
            
            logger.info("\nTraining Data Sample:")
            training_data.show(5)
            
            # 7. Check Feature Dependencies
            deps = manager.get_feature_dependencies(config.name)
            logger.info(f"\nFeature Dependencies: {deps}")
            
    except Exception as e:
        logger.error(f"Error in end-to-end example: {str(e)}")
        raise

