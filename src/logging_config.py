import logging
import os
from pathlib import Path
import atexit
import traceback
from typing import Optional

class ErrorContextFormatter(logging.Formatter):
    """Custom formatter that includes error context when available."""
    
    def format(self, record: logging.LogRecord) -> str:
        # Add error context if available
        if hasattr(record, 'error_context'):
            record.msg = f"{record.msg} [Context: {record.error_context}]"
        
        # Add traceback for errors if available
        if record.exc_info:
            record.msg = f"{record.msg}\n{traceback.format_exception(*record.exc_info)}"
        
        return super().format(record)

def setup_logging(log_level: Optional[str] = None):
    """Setup logging configuration for the application
    
    Args:
        log_level: Optional log level override. If not provided, uses LOG_LEVEL env var or defaults to INFO.
    """
    # Get log level from environment or default to INFO
    log_level = log_level or os.getenv('LOG_LEVEL', 'INFO').upper()
    log_dir = os.getenv('LOG_DIR', 'logs')
    
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