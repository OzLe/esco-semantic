from typing import Dict, List, Tuple, Optional
import torch
from transformers import MarianMTModel, MarianTokenizer
from ..core.logging import setup_logging
from ..core.config import get_config

logger = setup_logging(__name__)

class TranslationModelManager:
    """Manages loading and caching of translation models"""
    
    def __init__(self):
        self.config = get_config()
        self._models: Dict[str, Tuple[MarianMTModel, MarianTokenizer]] = {}
        
        # Define supported language pairs
        self._supported_languages = {
            'en': ['es', 'fr', 'de', 'it', 'pt', 'nl', 'pl', 'ru'],
            'es': ['en', 'fr', 'de', 'it', 'pt'],
            'fr': ['en', 'es', 'de', 'it', 'pt'],
            'de': ['en', 'es', 'fr', 'it', 'pt'],
            'it': ['en', 'es', 'fr', 'de', 'pt'],
            'pt': ['en', 'es', 'fr', 'de', 'it'],
            'nl': ['en'],
            'pl': ['en'],
            'ru': ['en']
        }
    
    def get_model(
        self,
        model_name: str,
        device: Optional[str] = None
    ) -> Tuple[MarianMTModel, MarianTokenizer]:
        """Get a model by name, loading it if necessary"""
        if model_name not in self._models:
            try:
                logger.info(f"Loading translation model: {model_name}")
                model = MarianMTModel.from_pretrained(model_name)
                tokenizer = MarianTokenizer.from_pretrained(model_name)
                
                if device:
                    model = model.to(device)
                
                self._models[model_name] = (model, tokenizer)
            except Exception as e:
                logger.error(f"Failed to load model {model_name}: {e}")
                raise
        
        return self._models[model_name]
    
    def get_supported_languages(self) -> Dict[str, List[str]]:
        """Get supported language pairs"""
        return self._supported_languages.copy()
    
    def is_supported(self, source_lang: str, target_lang: str) -> bool:
        """Check if language pair is supported"""
        return (
            source_lang in self._supported_languages and
            target_lang in self._supported_languages[source_lang]
        )
    
    def clear_cache(self) -> None:
        """Clear the model cache"""
        self._models.clear() 