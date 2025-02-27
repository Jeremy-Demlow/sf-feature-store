"""This module implements a standardized logging system for a Snowflake feature store framework. It provides a preconfigured logger with consistent formatting and flexible output options including console and file handlers. The setup_logger function supports customizable log levels and multiple output destinations while maintaining a unified format across the application."""

# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/03_logging.ipynb.

# %% ../nbs/03_logging.ipynb 2
from __future__ import annotations
import logging
from typing import Optional
import sys

# %% auto 0
__all__ = ['logger', 'setup_logger']

# %% ../nbs/03_logging.ipynb 3
def setup_logger(
    name: str = "snowflake_feature_store",
    level: int = logging.INFO,
    log_file: Optional[str] = None
) -> logging.Logger:
    """Set up logger with consistent formatting
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Optional file path for logging
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# %% ../nbs/03_logging.ipynb 4
logger = setup_logger()
