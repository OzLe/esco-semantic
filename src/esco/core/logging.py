import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from .config import get_config

class ColoredFormatter(logging.Formatter):
    """Colored output for terminal"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        if sys.stdout.isatty():
            levelname = record.levelname
            record.levelname = f"{self.COLORS.get(levelname, '')}{levelname}{self.RESET}"
        return super().format(record)

def setup_logging(name: str = None) -> logging.Logger:
    """Setup logging with proper configuration"""
    config = get_config()
    
    # Create logger
    logger = logging.getLogger(name or 'esco')
    logger.setLevel(config.get('logging.level', 'INFO'))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    )
    logger.addHandler(console_handler)
    
    # File handler
    log_dir = Path(config.get('logging.directory', 'logs'))
    log_dir.mkdir(exist_ok=True)
    
    file_handler = RotatingFileHandler(
        log_dir / 'esco.log',
        maxBytes=config.get('logging.max_bytes', 10 * 1024 * 1024),
        backupCount=config.get('logging.backup_count', 5),
        encoding='utf-8'
    )
    file_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    )
    logger.addHandler(file_handler)
    
    # Configure third-party loggers
    for lib in ['urllib3', 'transformers', 'sentence_transformers']:
        logging.getLogger(lib).setLevel(logging.WARNING)
    
    return logger

# Create default logger
logger = setup_logging() 