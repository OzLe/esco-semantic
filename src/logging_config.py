import logging
import os
from pathlib import Path
import atexit
from typing import Optional
import yaml

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

def setup_logging(config_path: str = "config/weaviate_config.yaml", profile: str = "default", log_level: Optional[str] = None):
    """Setup logging configuration for the application
    
    Args:
        config_path: Path to configuration file
        profile: Configuration profile to use
        log_level: Optional log level override. If not provided, uses config or defaults to INFO.
    """
    # Load configuration
    config = load_config(config_path, profile)
    
    # Get log settings from config
    app_config = config.get('app', {})
    log_level = log_level or app_config.get('log_level', 'INFO').upper()
    log_dir = app_config.get('log_dir', 'logs')
    
    # Create logs directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Create handlers
    file_handler = logging.FileHandler(os.path.join(log_dir, 'esco.log'))
    console_handler = logging.StreamHandler()
    
    # Configure handlers with custom formatter
    formatter = ErrorContextFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Register cleanup function
    def cleanup():
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)
    
    atexit.register(cleanup)
    
    # Create and return module logger
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, log_level))
    
    return logger

def log_error(logger: logging.Logger, error: Exception, context: dict = None, level: int = logging.ERROR):
    """Helper function to log errors with context
    
    Args:
        logger: Logger instance to use
        error: Exception to log
        context: Optional dictionary of context information
        level: Logging level to use (defaults to ERROR)
    """
    error_context = {
        'error_type': error.__class__.__name__,
        'error_message': str(error)
    }
    
    if hasattr(error, 'details'):
        error_context.update(error.details)
    
    if context:
        error_context.update(context)
    
    logger.log(level, str(error), extra={'error_context': error_context}) 