import logging
import sys
from pathlib import Path
from typing import Optional
from esco.core.config import get_config

def setup_logging(name: Optional[str] = None) -> logging.Logger:
    """Setup logging for CLI commands"""
    logger = logging.getLogger(name or 'esco.cli')
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    )
    logger.addHandler(console_handler)
    
    # Set level from config
    config = get_config()
    logger.setLevel(config.get('logging.level', 'INFO'))
    
    return logger

def get_data_dir() -> Path:
    """Get data directory path"""
    config = get_config()
    data_dir = Path(config.get('data.directory', 'data'))
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

def get_log_dir() -> Path:
    """Get log directory path"""
    config = get_config()
    log_dir = Path(config.get('logging.directory', 'logs'))
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir

def get_config_dir() -> Path:
    """Get config directory path"""
    config = get_config()
    config_dir = Path(config.get('config.directory', 'config'))
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir

def format_error(error: Exception) -> str:
    """Format error message for display"""
    return f"Error: {str(error)}"

def format_success(message: str) -> str:
    """Format success message for display"""
    return f"Success: {message}"

def format_warning(message: str) -> str:
    """Format warning message for display"""
    return f"Warning: {message}"

def format_info(message: str) -> str:
    """Format info message for display"""
    return f"Info: {message}" 