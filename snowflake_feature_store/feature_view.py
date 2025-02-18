"""This module implements a robust feature view creation system with monitoring capabilities for Snowflake ML Feature Store. It provides comprehensive feature validation and statistical monitoring through the FeatureMonitor class, which tracks metrics like null ratios, cardinality, and distribution shifts. The FeatureViewBuilder handles the end-to-end process of creating properly configured feature views with validation checks and metadata attachment, while the create_feature_view function offers a simplified API for this complex process."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/03_feature_view.ipynb.

# %% ../nbs/03_feature_view.ipynb 2
from __future__ import annotations
from typing import Dict, List, Optional, Union, Set
from datetime import datetime
from snowflake.snowpark import DataFrame
from snowflake.ml.feature_store import FeatureView, Entity
import snowflake.snowpark.functions as F
import json

# Import our modules
from .exceptions import FeatureViewError, ValidationError
from .logging import logger
from snowflake_feature_store.config import (
    BaseModel, Field, FeatureViewConfig, 
    FeatureConfig, RefreshConfig
)
from snowflake.snowpark.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, LongType, TimestampType
)
from .transforms import Transform, TransformConfig


# %% auto 0
__all__ = ['FeatureStats', 'FeatureMonitor', 'FeatureViewBuilder', 'create_feature_view']

# %% ../nbs/03_feature_view.ipynb 3
class FeatureStats(BaseModel):
    """Statistics for feature monitoring"""
    timestamp: datetime
    row_count: int
    null_count: int
    null_ratio: float
    unique_count: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None
    
    def __str__(self) -> str:
        """Pretty print statistics"""
        stats = [
            f"Timestamp: {self.timestamp.isoformat()}",
            f"Row count: {self.row_count}",
            f"Null count: {self.null_count} ({self.null_ratio:.1%})",
            f"Unique values: {self.unique_count}"
        ]
        
        if all(v is not None for v in [self.min_value, self.max_value, self.mean_value]):
            stats.extend([
                f"Min value: {self.min_value:.2f}",
                f"Max value: {self.max_value:.2f}",
                f"Mean value: {self.mean_value:.2f}",
                f"Std dev: {self.std_value:.2f}" if self.std_value else "Std dev: N/A"
            ])
            
        return "\n".join(stats)
    
    def model_dump(self, **kwargs) -> Dict:
        """Convert stats to dictionary for storage/display"""
        data = super().model_dump(**kwargs)
        data['timestamp'] = self.timestamp.isoformat()
        return data


# %% ../nbs/03_feature_view.ipynb 4
class FeatureMonitor:
    """Monitor feature statistics and detect drift"""
    
    def __init__(self, 
                 feature_config: FeatureConfig,
                 collect_detailed_stats: bool = True):
        self.config = feature_config
        self._baseline_stats: Optional[FeatureStats] = None
        self.collect_detailed_stats = collect_detailed_stats

    def _verify_column_names(self, df: DataFrame, column: str) -> None:
        """Verify column names in DataFrame"""
        logger.debug(f"All columns: {df.columns}")
        logger.debug(f"Schema: {df.schema}")
        logger.debug(f"Looking for column: {column}")
        if column not in df.columns:
            matches = [c for c in df.columns if c.upper() == column.upper()]
            if matches:
                logger.warning(f"Column case mismatch. Found {matches[0]} instead of {column}")
    
    def set_baseline(self, stats: FeatureStats) -> None:
        """Set baseline statistics for drift detection"""
        self._baseline_stats = stats
        
    def detect_drift(self, current_stats: FeatureStats) -> Dict[str, float]:
        """Detect drift from baseline statistics
        
        Returns dictionary of drift metrics
        """
        if not self._baseline_stats:
            raise FeatureViewError("No baseline statistics set")
            
        drift_metrics = {}
        
        # Check null ratio drift
        drift_metrics['null_ratio_change'] = (
            current_stats.null_ratio - self._baseline_stats.null_ratio
        )
        
        # Check numeric drift if stats available
        if (current_stats.mean_value is not None and 
            self._baseline_stats.mean_value is not None):
            
            # Mean shift
            drift_metrics['mean_shift'] = (
                current_stats.mean_value - self._baseline_stats.mean_value
            )
            
            # Distribution shift (using std dev)
            if current_stats.std_value and self._baseline_stats.std_value:
                drift_metrics['std_ratio'] = (
                    current_stats.std_value / self._baseline_stats.std_value
                )
        
        return drift_metrics
        
    def compute_stats(self, df: DataFrame, column: str) -> FeatureStats:
        """Compute statistics for a feature column"""
        try:
            # Verify column names first
            self._verify_column_names(df, column)
            total_count = df.count()
            null_count = df.filter(F.col(column).is_null()).count()
            
            # Initialize stats
            stats = {
                'timestamp': datetime.utcnow(),
                'row_count': total_count,
                'null_count': null_count,
                'null_ratio': null_count / total_count if total_count > 0 else 1.0
            }
            
            if self.collect_detailed_stats:
                # Get column type and schema field
                schema_field = next(field for field in df.schema.fields if field.name.upper() == column.upper())
                col_type = str(schema_field.datatype)
                logger.debug(f"Computing stats for {column} (type: {col_type})")
                
                # Always compute unique count
                unique_count = df.select(column).distinct().count()
                stats['unique_count'] = unique_count
                
                # Check if column is numeric - improved type checking
                is_numeric = any(
                    col_type.upper().startswith(t) 
                    for t in ['DOUBLE', 'FLOAT', 'INT', 'LONG', 'DECIMAL', 'NUMBER']
                ) or hasattr(schema_field.datatype, 'scale')
                
                logger.debug(f"Column {column} is_numeric: {is_numeric} (type: {col_type})")
                
                if is_numeric:
                    # Compute numeric stats on non-null values
                    numeric_df = df.filter(F.col(column).is_not_null())
                    if numeric_df.count() > 0:
                        # Use agg with explicit column names
                        agg_df = numeric_df.agg([
                            F.min(column).alias("MIN_VAL"),
                            F.max(column).alias("MAX_VAL"),
                            F.avg(column).alias("AVG_VAL"),
                            F.stddev(column).alias("STD_VAL")
                        ])
                        
                        # Debug logging
                        logger.debug(f"Aggregation columns: {agg_df.schema.names}")
                        
                        # Get result row as dictionary
                        result_dict = agg_df.collect()[0].asDict()
                        
                        # Debug logging
                        logger.debug(f"Result dict: {result_dict}")
                        
                        # Update stats using the correct column names
                        stats.update({
                            'min_value': float(result_dict["MIN_VAL"]) if result_dict["MIN_VAL"] is not None else None,
                            'max_value': float(result_dict["MAX_VAL"]) if result_dict["MAX_VAL"] is not None else None,
                            'mean_value': float(result_dict["AVG_VAL"]) if result_dict["AVG_VAL"] is not None else None,
                            'std_value': float(result_dict["STD_VAL"]) if result_dict["STD_VAL"] is not None else None
                        })
                        
                        logger.debug(f"Numeric stats computed for {column}: {stats}")
                    else:
                        # No non-null numeric values
                        stats.update({
                            'min_value': None,
                            'max_value': None,
                            'mean_value': None,
                            'std_value': None
                        })
                else:
                    # For non-numeric columns, set numeric stats to None
                    stats.update({
                        'min_value': None,
                        'max_value': None,
                        'mean_value': None,
                        'std_value': None
                    })
            
            logger.debug(f"Final stats for {column}: {stats}")
            return FeatureStats(**stats)
            
        except Exception as e:
            logger.error(f"Error computing stats for {column}: {str(e)}")
            logger.error(f"Exception type: {type(e)}")
            logger.error(f"Exception args: {e.args}")
            raise FeatureViewError(f"Stats computation failed: {str(e)}")


# %% ../nbs/03_feature_view.ipynb 5
class FeatureViewBuilder:
    """Builder for creating feature views with monitoring"""
    
    def __init__(
        self,
        config: FeatureViewConfig,
        feature_df: DataFrame,
        entities: Union[Entity, List[Entity]],
        collect_stats: bool = True
    ):
        self.config = config
        self.feature_df = feature_df
        self.entities = [entities] if isinstance(entities, Entity) else entities
        self.monitors: Dict[str, FeatureMonitor] = {}
        self.collect_stats = collect_stats
        
        # Initialize monitors only for features that exist in the DataFrame
        available_columns = set(feature_df.columns)
        for name, feature_config in config.features.items():
            if name in available_columns:  # Only monitor existing columns
                self.monitors[name] = FeatureMonitor(
                    feature_config,
                    collect_detailed_stats=collect_stats
                )
    
    def _validate_features(self) -> None:
        """Validate features against their configurations"""
        for name, monitor in self.monitors.items():
            try:
                # Compute current stats
                stats = monitor.compute_stats(self.feature_df, name)
                
                # Validate against config
                if stats.null_ratio > monitor.config.validation.null_threshold:
                    raise ValidationError(
                        f"Feature {name} has {stats.null_ratio:.1%} null values, "
                        f"exceeding threshold of {monitor.config.validation.null_threshold:.1%}"
                    )
                
                # Set as baseline for future monitoring
                monitor.set_baseline(stats)
                
                logger.info(f"Validated feature {name} (stats: {stats.model_dump()})")
                
            except Exception as e:
                raise FeatureViewError(f"Validation failed for {name}: {str(e)}")

    def _validate_timestamp_col(self, df: DataFrame) -> None:
        """Validate timestamp column type"""
        if self.config.timestamp_col:
            col_type = df.schema[self.config.timestamp_col].datatype
            if not isinstance(col_type, (DateType, TimestampType)):
                # Try to cast the column
                logger.warning(
                    f"Timestamp column {self.config.timestamp_col} has type {col_type}. "
                    "Attempting to cast to DATE."
                )
                df = df.with_column(
                    self.config.timestamp_col,
                    F.to_date(F.col(self.config.timestamp_col))
                )
                self.feature_df = df
    
    def build(self) -> FeatureView:
        """Build the feature view with validation and monitoring"""
        try:
            # Validate features first
            self._validate_features()
            
            # Validate timestamp column
            self._validate_timestamp_col(self.feature_df)
            
            # Create feature view
            feature_view = FeatureView(
                name=self.config.name,
                entities=self.entities,
                feature_df=self.feature_df,
                refresh_freq=self.config.refresh.frequency,
                timestamp_col=self.config.timestamp_col,
                desc=self.config.description or f"Feature view {self.config.name}"
            )
            
            # Add feature descriptions
            feature_descriptions = {
                name: config.description
                for name, config in self.config.features.items()
            }
            feature_view = feature_view.attach_feature_desc(feature_descriptions)
            
            return feature_view
            
        except Exception as e:
            logger.error(f"Error building feature view: {str(e)}")
            raise FeatureViewError(f"Feature view creation failed: {str(e)}")


# %% ../nbs/03_feature_view.ipynb 6
def create_feature_view(
    config: FeatureViewConfig,
    feature_df: DataFrame,
    entities: Union[Entity, List[Entity]],
    collect_stats: bool = True
) -> FeatureView:
    """Create a feature view with validation and monitoring
    
    Args:
        config: Feature view configuration
        feature_df: DataFrame containing feature transformations
        entities: Entity or list of entities
        collect_stats: Whether to collect detailed statistics
        
    Returns:
        Configured FeatureView object
        
    Example:
        >>> config = FeatureViewConfig(
        ...     name="customer_behavior",
        ...     domain="RETAIL",
        ...     features={
        ...         "session_length": FeatureConfig(
        ...             name="session_length",
        ...             description="Session length in minutes",
        ...             validation=FeatureValidationConfig(
        ...                 null_threshold=0.1,
        ...                 range_check=True,
        ...                 min_value=0
        ...             )
        ...         )
        ...     }
        ... )
        >>> entity = Entity("CUSTOMER", ["customer_id"])
        >>> feature_view = create_feature_view(config, df, entity)
    """
    return FeatureViewBuilder(config, feature_df, entities, collect_stats).build()

