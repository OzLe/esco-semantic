"""
Logging configuration for the ESCO semantic search system.

This module provides centralized logging configuration and utilities
for consistent logging across the application.
"""

import logging
import os
from pathlib import Path
import atexit
from typing import Optional, Dict, Any
import yaml
import sys
import json
from datetime import datetime

class ErrorContextFormatter(logging.Formatter):
    """Custom formatter that includes error context in log messages"""
    def format(self, record):
        if record.exc_info:
            # Add error context to the message
            record.msg = f"{record.msg} [Error: {record.exc_info[1]}]"
        return super().format(record)

def load_config(config_path: str = "config/weaviate_config.yaml", profile: str = "default") -> dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        if profile not in config:
            raise ValueError(f"Profile '{profile}' not found in config file")
        return config[profile]
    except Exception as e:
        raise ValueError(f"Failed to load config file: {str(e)}")

def setup_logging(
    log_level: str = "INFO",
    log_dir: Optional[str] = None,
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """
    Set up logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        log_format: Log message format
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('esco')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Check if handlers already exist to prevent duplicates
    if not logger.handlers:
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # Create file handler if log directory is specified
        if log_dir:
            log_path = Path(log_dir)
            log_path.mkdir(parents=True, exist_ok=True)
            
            # Create handlers for different log levels
            for level in ['info', 'error']:
                file_handler = logging.FileHandler(
                    log_path / f'esco_{level}.log',
                    encoding='utf-8'
                )
                file_handler.setLevel(getattr(logging, level.upper()))
                file_formatter = logging.Formatter(log_format)
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
    
    return logger

def log_ingestion_progress(
    logger: logging.Logger,
    step: str,
    progress: float,
    eta: Optional[str] = None,
    items_processed: Optional[int] = None,
    total_items: Optional[int] = None,
    level: str = "INFO"
) -> None:
    """
    Log structured ingestion progress information.
    
    Args:
        logger: Logger instance
        step: Current ingestion step
        progress: Progress percentage (0-100)
        eta: Estimated time of completion
        items_processed: Number of items processed
        total_items: Total number of items
        level: Log level
    """
    extra = {
        'step': step,
        'progress': f"{progress:.1f}%",
        'eta': eta or 'unknown',
        'items_processed': items_processed or 0,
        'total_items': total_items or 0
    }
    
    log_func = getattr(logger, level.lower())
    log_func(
        f"Ingestion progress - Step: {step}, Progress: {progress:.1f}%, "
        f"Items: {items_processed}/{total_items}, ETA: {eta or 'unknown'}",
        extra=extra
    )

def log_ingestion_wait(
    logger: logging.Logger,
    timeout_minutes: int,
    poll_interval: int,
    current_status: str
) -> None:
    """
    Log ingestion wait status.
    
    Args:
        logger: Logger instance
        timeout_minutes: Maximum wait time in minutes
        poll_interval: Polling interval in seconds
        current_status: Current ingestion status
    """
    logger.info(
        f"Waiting for ingestion completion - "
        f"Timeout: {timeout_minutes} minutes, "
        f"Poll interval: {poll_interval} seconds, "
        f"Current status: {current_status}"
    )

def log_ingestion_error(
    logger: logging.Logger,
    error: Exception,
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log ingestion error with context.
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        context: Additional context information
    """
    error_context = {
        'error_type': error.__class__.__name__,
        'error_message': str(error),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if context:
        error_context.update(context)
    
    logger.error(
        f"Ingestion error: {error.__class__.__name__} - {str(error)}",
        extra=error_context
    )

def log_error(
    logger: logging.Logger,
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    level: int = logging.ERROR
) -> None:
    """
    Log error with context.
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        context: Additional context information
        level: Logging level to use (defaults to ERROR)
    """
    error_context = {
        'error_type': error.__class__.__name__,
        'error_message': str(error),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if hasattr(error, 'details'):
        error_context.update(error.details)
    
    if context:
        error_context.update(context)
    
    logger.log(level, str(error), extra={'error_context': error_context}) 