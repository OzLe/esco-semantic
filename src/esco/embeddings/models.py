from typing import Dict, Optional
from sentence_transformers import SentenceTransformer
from ..core.logging import setup_logging
from ..core.config import get_config

logger = setup_logging(__name__)

class ModelManager:
    """Manages loading and caching of embedding models"""
    
    def __init__(self):
        self.config = get_config()
        self._models: Dict[str, SentenceTransformer] = {}
        self._model_configs = {
            'all-MiniLM-L6-v2': {
                'dimensions': 384,
                'max_sequence_length': 256
            },
            'all-mpnet-base-v2': {
                'dimensions': 768,
                'max_sequence_length': 384
            },
            'multi-qa-mpnet-base-dot-v1': {
                'dimensions': 768,
                'max_sequence_length': 384
            }
        }
    
    def get_model(self, model_name: str) -> SentenceTransformer:
        """Get a model by name, loading it if necessary"""
        if model_name not in self._models:
            try:
                logger.info(f"Loading model: {model_name}")
                self._models[model_name] = SentenceTransformer(model_name)
            except Exception as e:
                logger.error(f"Failed to load model {model_name}: {e}")
                raise
        
        return self._models[model_name]
    
    def get_model_config(self, model_name: str) -> Optional[Dict]:
        """Get configuration for a specific model"""
        return self._model_configs.get(model_name)
    
    def list_available_models(self) -> Dict[str, Dict]:
        """List all available models and their configurations"""
        return self._model_configs.copy()
    
    def clear_cache(self) -> None:
        """Clear the model cache"""
        self._models.clear() 