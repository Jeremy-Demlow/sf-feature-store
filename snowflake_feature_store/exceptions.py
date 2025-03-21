"""This module defines a hierarchical exception system for error handling in a feature store implementation. It establishes a base FeatureStoreException class with specialized subclasses for different error categories including configuration, connection, entity, feature view, and validation issues. These exceptions provide targeted error reporting to help troubleshoot specific problems in the feature store workflow."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/02_expections.ipynb.

# %% ../nbs/02_expections.ipynb 2
from __future__ import annotations


# %% auto 0
__all__ = ['FeatureStoreException', 'ConfigurationError', 'ConnectionError', 'EntityError', 'FeatureViewError', 'ValidationError']

# %% ../nbs/02_expections.ipynb 3
class FeatureStoreException(Exception):
    """Base exception for feature store errors"""
    pass


# %% ../nbs/02_expections.ipynb 4
class ConfigurationError(FeatureStoreException):
    """Raised when there's an error in configuration"""
    pass


# %% ../nbs/02_expections.ipynb 5
class ConnectionError(FeatureStoreException):
    """Raised when there's an error connecting to Snowflake"""
    pass


# %% ../nbs/02_expections.ipynb 6
class EntityError(FeatureStoreException):
    """Raised when there's an error with entities"""
    pass


# %% ../nbs/02_expections.ipynb 7
class FeatureViewError(FeatureStoreException):
    """Raised when there's an error with feature views"""
    pass


# %% ../nbs/02_expections.ipynb 8
class ValidationError(FeatureStoreException):
    """Raised when there's a validation error"""
    pass

