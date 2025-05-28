import logging
import os
from pathlib import Path
import atexit

def setup_logging():
    """Setup logging configuration for the application"""
    # Get log level from environment or default to INFO
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_dir = os.getenv('LOG_DIR', 'logs')
    
    # Create logs directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Create handlers
    file_handler = logging.FileHandler(os.path.join(log_dir, 'esco.log'))
    console_handler = logging.StreamHandler()
    
    # Configure handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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