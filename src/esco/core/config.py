import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from .exceptions import ConfigurationError

class Config:
    """Centralized configuration management with singleton pattern"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.env = os.getenv('ESCO_ENV', 'development')
        self._config = self._load_config()
        self._initialized = True
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration based on environment"""
        config_dir = Path(__file__).parent.parent.parent.parent / 'config'
        
        # Load base config
        base_config = self._load_yaml(config_dir / 'default.yaml')
        
        # Load environment-specific config
        env_config_file = config_dir / f'{self.env}.yaml'
        if env_config_file.exists():
            env_config = self._load_yaml(env_config_file)
            # Deep merge configurations
            return self._deep_merge(base_config, env_config)
        
        return base_config
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """Load YAML configuration file"""
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            raise ConfigurationError(f"Failed to load config from {path}: {e}")
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value

# Singleton accessor
def get_config() -> Config:
    """Get the singleton configuration instance"""
    return Config() 