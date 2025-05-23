import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(level=logging.INFO):
    """Setup logging configuration for all modules
    
    Args:
        level (int): Logging level (default: logging.INFO)
    """
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    file_handler = RotatingFileHandler(
        'logs/esco.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Set specific logger levels
    logging.getLogger('urllib3').setLevel(logging.WARNING)  # Reduce urllib3 logging
    logging.getLogger('tqdm').setLevel(logging.WARNING)  # Reduce tqdm logging
    logging.getLogger('sentence_transformers').setLevel(logging.WARNING)  # Reduce sentence-transformers logging
    logging.getLogger('transformers').setLevel(logging.WARNING)  # Reduce transformers logging
    
    # Disable tqdm progress bars for specific modules
    import tqdm
    tqdm.tqdm.monitor_interval = 0  # Disable tqdm monitoring
    
    return logging.getLogger(__name__) 