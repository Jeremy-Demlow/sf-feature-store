"""This module provides configuration classes for a feature store system. It defines a comprehensive configuration structure using Pydantic models to support feature view management with validation capabilities. The configuration includes settings for feature views, data sources, and other related components."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/00_config.ipynb.

# %% ../nbs/00_config.ipynb 2
from __future__ import annotations
from typing import Optional, Dict, List, Union
from pydantic import BaseModel, Field, validator
from datetime import timedelta
import yaml
from pathlib import Path

# Import our custom exceptions
from .exceptions import ConfigurationError


# %% auto 0
__all__ = ['RefreshConfig', 'FeatureValidationConfig', 'FeatureConfig', 'FeatureViewConfig']

# %% ../nbs/00_config.ipynb 3
class RefreshConfig(BaseModel):
    """Configuration for feature refresh settings"""
    frequency: str = Field("1 day", description="Refresh frequency (e.g., '1 day', '30 minutes')")
    mode: str = Field("FULL", description="Refresh mode (FULL or INCREMENTAL)")
    
    @validator('frequency')
    def validate_frequency(cls, v):
        """Validate refresh frequency format"""
        try:
            # Check if it's a cron expression
            if ' ' in v and len(v.split()) == 5:
                return v
            
            # Parse as time duration
            parts = v.split()
            if len(parts) != 2:
                raise ValueError
            
            num = int(parts[0])
            unit = parts[1].lower()
            
            valid_units = ['minute', 'minutes', 'hour', 'hours', 'day', 'days']
            if unit not in valid_units:
                raise ValueError
                
            return v
        except ValueError:
            raise ConfigurationError(
                f"Invalid refresh frequency: {v}. "
                "Use either cron expression or duration (e.g., '1 day', '30 minutes')"
            )


# %% ../nbs/00_config.ipynb 4
class FeatureValidationConfig(BaseModel):
    """Configuration for feature validation rules"""
    null_check: bool = Field(True, description="Check for null values")
    null_threshold: float = Field(0.1, description="Maximum allowed null ratio")
    range_check: bool = Field(False, description="Check value ranges")
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unique_check: bool = Field(False, description="Check for uniqueness")
    unique_threshold: float = Field(0.9, description="Minimum unique ratio")


# %% ../nbs/00_config.ipynb 5
class FeatureConfig(BaseModel):
    """Configuration for individual features"""
    name: str
    description: str
    validation: Optional[FeatureValidationConfig] = Field(
        default_factory=FeatureValidationConfig,
        description="Validation rules for this feature"
    )
    dependencies: List[str] = Field(
        default_factory=list,
        description="List of features this feature depends on"
    )


# %% ../nbs/00_config.ipynb 6
class FeatureViewConfig(BaseModel):
    """Enhanced configuration for feature views"""
    name: str
    domain: str = ""
    entity: str = "CUSTOMER"
    feature_type: str = "BASE"
    major_version: int = Field(1, ge=1)
    minor_version: int = Field(0, ge=0)
    refresh: RefreshConfig = Field(default_factory=RefreshConfig)
    timestamp_col: Optional[str] = None
    description: Optional[str] = None
    features: Dict[str, FeatureConfig] = Field(
        default_factory=dict,
        description="Configuration for each feature"
    )
    tags: Dict[str, str] = Field(default_factory=dict)

    @property
    def version(self) -> str:
        """Get formatted version string"""
        return f"V{self.major_version}_{self.minor_version}"

    @property
    def full_name(self) -> str:
        """Get formatted full name for the feature view"""
        parts = ["FV"]
        if self.domain:
            parts.append(self.domain)
        parts.extend([self.entity, self.feature_type])
        return "_".join(part.upper() for part in parts)
    
    @property
    def refresh_frequency(self) -> str:
        """Get refresh frequency from RefreshConfig"""
        return self.refresh.frequency

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> FeatureViewConfig:
        """Load configuration from YAML file"""
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            return cls(**data)
        except Exception as e:
            raise ConfigurationError(f"Error loading config from {path}: {str(e)}")

    def to_yaml(self, path: Union[str, Path]) -> None:
        """Save configuration to YAML file"""
        try:
            with open(path, 'w') as f:
                yaml.dump(self.dict(), f)
        except Exception as e:
            raise ConfigurationError(f"Error saving config to {path}: {str(e)}")

