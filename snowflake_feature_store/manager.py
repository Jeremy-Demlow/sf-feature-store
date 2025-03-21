"""This module provides a comprehensive manager for Snowflake Feature Store operations with monitoring, drift detection and dependency tracking capabilities. It implements FeatureStoreManager as the central class for entity and feature view management, with robust callback mechanisms for monitoring through the MetricsCallback implementation. The feature_store_session context manager offers a convenient way to create temporary or persistent feature store environments with automatic cleanup, while the dependency tracking system uses a directed graph to maintain relationships between features for impact analysis."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/07_manager.ipynb.

# %% ../nbs/07_manager.ipynb 2
from __future__ import annotations
from typing import List, Optional, Dict, Union, Set
from dataclasses import dataclass
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
import networkx as nx
from pathlib import Path
import json

from snowflake.ml.feature_store import (
    FeatureStore, Entity, FeatureView, CreationMode
)
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F

# Import our modules
from .connection import SnowflakeConnection
from snowflake_feature_store.feature_view import (
    FeatureViewBuilder, create_feature_view, 
    FeatureStats, FeatureMonitor
)
from snowflake_feature_store.transforms import (
    Transform, apply_transforms, TransformConfig,
    moving_agg, fill_na
)
from snowflake_feature_store.config import (
    FeatureViewConfig, FeatureConfig, 
    RefreshConfig, FeatureValidationConfig
)
from snowflake_feature_store.exceptions import (
    FeatureStoreException, EntityError, 
    FeatureViewError, ValidationError
)
from .logging import logger


# %% auto 0
__all__ = ['FeatureStoreCallback', 'MetricsCallback', 'FeatureStoreManager', 'feature_store_session']

# %% ../nbs/07_manager.ipynb 3
class FeatureStoreCallback:
    """Protocol for feature store callbacks"""
    def on_feature_view_create(
        self, name: str, df: DataFrame, stats: Dict[str, FeatureStats]
    ) -> None: ...
    
    def on_entity_create(self, name: str, keys: List[str]) -> None: ...
    def on_error(self, error: str) -> None: ...
    def on_drift_detected(
        self, feature_view: str, feature: str, metrics: Dict[str, float]
    ) -> None: ...

# %% ../nbs/07_manager.ipynb 4
class MetricsCallback(FeatureStoreCallback):
    """Callback that logs metrics and statistics"""
    
    def __init__(self, metrics_path: Optional[Path] = None):
        self.metrics_path = metrics_path
        if metrics_path:
            metrics_path.mkdir(parents=True, exist_ok=True)
    
    def _save_metrics(self, name: str, data: Dict) -> None:
        """Save metrics to JSON file if path specified"""
        if self.metrics_path:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            file_path = self.metrics_path / f"{name}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
    
    def on_feature_view_create(
        self, name: str, df: DataFrame, stats: Dict[str, FeatureStats]
    ) -> None:
        """Log feature view creation with statistics"""
        logger.info(f"Created feature view: {name} with {len(df.columns)} features")
        
        # Log detailed stats
        stats_data = {
            'name': name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'feature_stats': {
                fname: fstats.model_dump()
                for fname, fstats in stats.items()
            }
        }
        self._save_metrics(f"{name}_creation", stats_data)
        
    def on_entity_create(self, name: str, keys: List[str]) -> None:
        """Log entity creation"""
        logger.info(f"Created entity: {name} with keys: {keys}")
        
    def on_error(self, error: str) -> None:
        """Log errors"""
        logger.error(f"Error: {error}")
        
    def on_drift_detected(
        self, feature_view: str, feature: str, metrics: Dict[str, float]
    ) -> None:
        """Log feature drift detection"""
        logger.warning(
            f"Drift detected in {feature_view}.{feature}: {metrics}"
        )
        self._save_metrics(
            f"{feature_view}_{feature}_drift",
            {
                'feature_view': feature_view,
                'feature': feature,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'metrics': metrics
            }
        )

# %% ../nbs/07_manager.ipynb 5
class FeatureStoreManager:
    """Manages feature store operations with monitoring and dependency tracking"""
    
    def __init__(
        self,
        connection: SnowflakeConnection,
        callbacks: Optional[List[FeatureStoreCallback]] = None,
        metrics_path: Optional[Union[str, Path]] = None,
        overwrite: bool = False
    ):
        """Initialize feature store manager
        
        Args:
            connection: Snowflake connection
            callbacks: Optional callbacks for monitoring
            metrics_path: Optional path to save metrics
            overwrite: Whether to overwrite existing features
        """
        self.connection = connection
        self.feature_store = FeatureStore(
            session=self.connection.session,
            database=self.connection.database,
            name=self.connection.schema,
            default_warehouse=self.connection.warehouse,
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST
        )
        
        # Initialize storage
        self.entities: Dict[str, Entity] = {}
        self.feature_views: Dict[str, FeatureView] = {}
        self.feature_configs: Dict[str, FeatureViewConfig] = {}
        self.feature_stats: Dict[str, Dict[str, FeatureStats]] = {}
        self.feature_transforms: Dict[str, List[Transform]] = {}  # Add this line
        self.dependencies = nx.DiGraph()
        
        # Setup callbacks
        self.callbacks = callbacks or []
        if metrics_path:
            self.callbacks.append(
                MetricsCallback(Path(metrics_path))
            )
        
        self.overwrite = overwrite
        logger.info("FeatureStoreManager initialized")
            
    def add_entity(
        self, 
        name: str, 
        join_keys: List[str], 
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> FeatureStoreManager:
        """Add entity to feature store
        
        Args:
            name: Entity name
            join_keys: Keys used for joining
            description: Optional description
            tags: Optional metadata tags
            
        Returns:
            Self for method chaining
        """
        try:
            entity = Entity(
                name=name,
                join_keys=join_keys,
                desc=description or f"Entity {name}"
            )
            
            # Register entity
            self.feature_store.register_entity(entity)
            self.entities[name] = entity
            
            # Add tags if provided
            if tags:
                for key, value in tags.items():
                    self.feature_store.set_tag(entity, key, value)
            
            for cb in self.callbacks:
                cb.on_entity_create(name, join_keys)
                
        except Exception as e:
            error_msg = f"Error creating entity {name}: {str(e)}"
            for cb in self.callbacks:
                cb.on_error(error_msg)
            raise EntityError(error_msg)
            
        return self

    
    def add_feature_view(
        self,
        config: FeatureViewConfig,
        df: DataFrame,
        entity_name: str,
        transforms: Optional[List[Transform]] = None,
        collect_stats: bool = True
    ) -> FeatureView:
        """Add feature view to feature store with monitoring
        
        Args:
            config: Feature view configuration
            df: Source DataFrame
            entity_name: Entity name
            transforms: Optional transformations to apply
            collect_stats: Whether to collect feature statistics
        """
        try:
            # Validate schema only (no execution)
            self._validate_schema(df)
            
            # Apply transforms if provided
            if transforms:
                self.feature_transforms[config.name] = transforms
                df = apply_transforms(df, transforms)
                
            # Get entity
            entity = self.entities.get(entity_name)
            if not entity:
                raise EntityError(f"Entity {entity_name} not found")
                
            # Create feature view
            feature_view = create_feature_view(
                config=config,
                feature_df=df,
                entities=entity,
                collect_stats=collect_stats
            )
            
            # Register feature view
            registered_view = self.feature_store.register_feature_view(
                feature_view=feature_view,
                version=config.version,
                block=True,
                overwrite=self.overwrite
            )
            
            # Store view, config, and compute initial stats
            self.feature_views[config.name] = registered_view
            self.feature_configs[config.name] = config

            # Update dependency graph
            self._update_dependencies(config)
            
            # Compute and store statistics
            builder = FeatureViewBuilder(config, df, entity)
            stats = {
                name: monitor.compute_stats(df, name)
                for name, monitor in builder.monitors.items()
            }
            self.feature_stats[config.name] = stats
            
            # Notify callbacks
            for cb in self.callbacks:
                cb.on_feature_view_create(config.name, df, stats)
                
            return registered_view
            
        except Exception as e:
            error_msg = f"Error creating feature view {config.name}: {str(e)}"
            for cb in self.callbacks:
                cb.on_error(error_msg)
            raise FeatureViewError(error_msg)
    
    def check_feature_drift(
        self,
        feature_view_name: str,
        new_data: DataFrame
    ) -> Dict[str, Dict[str, float]]:
        """Check for feature drift in new data"""
        drift_results = {}
        
        try:
            # Get stored stats
            stored_stats = self.feature_stats.get(feature_view_name)
            if not stored_stats:
                raise FeatureViewError(
                    f"No baseline stats for feature view {feature_view_name}"
                )
            
            # Get feature view configuration
            config = self.feature_configs.get(feature_view_name)
            if not config:
                raise FeatureViewError(f"Feature view config {feature_view_name} not found")
                
            # Apply stored transforms to new data
            transforms = self.feature_transforms.get(feature_view_name, [])
            if transforms:
                logger.info(f"Applying {len(transforms)} transforms to new data")
                new_data_with_features = apply_transforms(new_data, transforms)
            else:
                logger.info("No transforms to apply")
                new_data_with_features = new_data
            
            # Check drift for each feature
            for feature_name, baseline_stats in stored_stats.items():
                try:
                    # Get the original feature config if it exists
                    feature_config = config.features.get(feature_name)
                    
                    # Create monitor with existing config or default
                    monitor = FeatureMonitor(
                        feature_config or FeatureConfig(
                            name=feature_name,
                            description=f"Temporary monitor for {feature_name}"
                        ),
                        collect_detailed_stats=True
                    )
                    
                    # Compute current stats and detect drift
                    current_stats = monitor.compute_stats(new_data_with_features, feature_name)
                    monitor.set_baseline(baseline_stats)
                    drift_metrics = monitor.detect_drift(current_stats)
                    
                    # Check if drift is significant
                    if any(abs(v) > 0.1 for v in drift_metrics.values()):
                        drift_results[feature_name] = drift_metrics
                        # Notify callbacks
                        for cb in self.callbacks:
                            cb.on_drift_detected(
                                feature_view_name, feature_name, drift_metrics
                            )
                            
                except Exception as e:
                    logger.warning(f"Skipping drift detection for {feature_name}: {str(e)}")
                    continue
            
            return drift_results
            
        except Exception as e:
            error_msg = f"Error checking drift for {feature_view_name}: {str(e)}"
            for cb in self.callbacks:
                cb.on_error(error_msg)
            raise FeatureViewError(error_msg)
        
    def get_feature_dependencies(self, feature_view_name: str) -> Set[str]:
        """Get dependencies for a feature view"""
        try:
            return nx.descendants(self.dependencies, feature_view_name)
        except Exception as e:
            raise FeatureViewError(
                f"Error getting dependencies for {feature_view_name}: {str(e)}"
            )
    
    def _update_dependencies(self, config: FeatureViewConfig) -> None:
        """Update dependency graph with new feature view"""
        try:
            # Add the feature view as a node
            self.dependencies.add_node(config.name)
            
            # Track dependencies from transforms
            for feature_name, feature_config in config.features.items():
                # Add each feature as a node
                feature_node = f"{config.name}.{feature_name}"
                self.dependencies.add_node(feature_node)
                
                # Add edge from feature view to feature
                self.dependencies.add_edge(config.name, feature_node)
                
                # Add dependencies between features
                if feature_config.dependencies:
                    for dep in feature_config.dependencies:
                        self.dependencies.add_edge(feature_node, dep)
                        
            logger.debug(
                f"Updated dependencies for {config.name}: "
                f"{list(self.dependencies.edges)}"
            )
        except Exception as e:
            logger.error(f"Error updating dependencies: {str(e)}")

    def get_feature_dependencies(self, feature_view_name: str) -> Set[str]:
        """Get dependencies for a feature view
        
        Args:
            feature_view_name: Name of the feature view
            
        Returns:
            Set of dependent feature names
        """
        try:
            # Get all descendants (dependencies) from the graph
            deps = nx.descendants(self.dependencies, feature_view_name)
            
            # Filter out internal feature nodes
            feature_deps = {
                dep.split('.')[0] for dep in deps 
                if '.' in dep  # Only include actual feature views
            }
            
            logger.info(
                f"Dependencies for {feature_view_name}: {feature_deps}"
            )
            return feature_deps
            
        except Exception as e:
            raise FeatureViewError(
                f"Error getting dependencies for {feature_view_name}: {str(e)}"
            )

    
    def _validate_schema(self, df: DataFrame) -> None:
        """Validate DataFrame schema without execution"""
        if not df.schema:
            raise ValidationError("DataFrame has no schema")
            
    def get_features(
        self,
        spine_df: DataFrame,
        feature_views: List[Union[str, FeatureView, FeatureViewConfig]],
        label_cols: Optional[List[str]] = None,
        dataset_name: Optional[str] = None,
        spine_timestamp_col: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """Get features for training or inference"""
        try:
            # Debug information
            logger.info(f"Spine DataFrame columns: {spine_df.columns}")
            logger.info(f"Spine DataFrame schema: {spine_df.schema}")

            views = []
            for fv in feature_views:
                if isinstance(fv, FeatureView):
                    # Direct FeatureView object
                    views.append(fv)
                elif isinstance(fv, FeatureViewConfig):
                    # Config object - get view by name/version
                    view = self.feature_store.get_feature_view(
                        fv.name, version=fv.version
                    )
                    views.append(view)
                elif isinstance(fv, str):
                    # String reference "name/version"
                    name, version = fv.split('/')
                    view = self.feature_store.get_feature_view(name, version)
                    views.append(view)
                else:
                    raise ValueError(f"Unsupported feature view type: {type(fv)}")
            
            if dataset_name is None:
                timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
                unique_id = str(uuid.uuid4())[:8]
                dataset_name = f"DATASET_{timestamp}_{unique_id}"

            # If label_cols are provided, ensure they're properly quoted
            if label_cols:
                label_cols = [f'"{col}"' for col in label_cols]
                
            # Ensure timestamp col is quoted
            if spine_timestamp_col:
                spine_timestamp_col = f'"{spine_timestamp_col}"'

            logger.info(f"Generating dataset with name: {dataset_name}")
            logger.info(f"Label columns: {label_cols}")
            logger.info(f"Timestamp column: {spine_timestamp_col}")
                
            dataset = self.feature_store.generate_dataset(
                name=dataset_name,
                spine_df=spine_df,
                features=views,
                spine_label_cols=label_cols,
                spine_timestamp_col=spine_timestamp_col,
                **kwargs
            )
            
            return dataset.read.to_snowpark_dataframe()
            
        except Exception as e:
            error_msg = f"Error generating dataset: {str(e)}"
            for cb in self.callbacks:
                cb.on_error(error_msg)
            raise FeatureStoreException(error_msg)



# %% ../nbs/07_manager.ipynb 6
@contextmanager
def feature_store_session(
    connection: SnowflakeConnection, 
    *,  # Force keyword arguments
    schema_name: Optional[str] = None,
    metrics_path: Optional[Union[str, Path]] = None,  # Changed type hint
    cleanup: bool = True
):
    """Context manager for feature store operations
    
    Args:
        connection: Snowflake connection
        schema_name: Optional schema name (keyword only)
        metrics_path: Optional path to save metrics (keyword only)
        cleanup: Whether to cleanup schema after use (keyword only)
    """
    schema = schema_name or (
        f"FEATURE_STORE_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        f"_{uuid.uuid4().hex[:8]}"
    )
    original_schema = connection.schema
    
    try:
        # Create schema
        connection.session.sql(
            f"CREATE SCHEMA IF NOT EXISTS {connection.database}.{schema}"
        ).collect()
        
        # Set schema as current
        connection.session.sql(
            f"USE SCHEMA {connection.database}.{schema}"
        ).collect()
        connection.schema = schema
        
        # Create and yield manager with metrics path
        manager = FeatureStoreManager(
            connection=connection,
            metrics_path=metrics_path,
            overwrite=True
        )
        yield manager
        
    finally:
        if cleanup:
            try:
                # Cleanup schema and all objects
                connection.session.sql(
                    f"DROP SCHEMA IF EXISTS {connection.database}.{schema} CASCADE"
                ).collect()
                logger.info(f"Cleaned up schema {schema}")
            except Exception as e:
                logger.error(f"Cleanup failed: {str(e)}")
            
            # Restore original schema
            try:
                connection.session.sql(
                    f"USE SCHEMA {connection.database}.{original_schema}"
                ).collect()
                connection.schema = original_schema
            except Exception as e:
                logger.error(f"Failed to restore original schema: {str(e)}")

