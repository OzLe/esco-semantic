from typing import List, Dict, Any, Optional
import torch
from transformers import MarianMTModel, MarianTokenizer
from ..core.logging import setup_logging
from ..core.config import get_config
from .models import TranslationModelManager

logger = setup_logging(__name__)

class TranslationEngine:
    """Handles text translation using various models"""
    
    def __init__(
        self,
        source_lang: str = "en",
        target_lang: str = "es",
        model_name: Optional[str] = None,
        device: Optional[str] = None
    ):
        self.config = get_config()
        self.source_lang = source_lang
        self.target_lang = target_lang
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        
        # Initialize model manager
        self.model_manager = TranslationModelManager()
        
        # Load model
        self.model_name = model_name or self._get_default_model()
        self.model = None
        self.tokenizer = None
        self._load_model()
    
    def _get_default_model(self) -> str:
        """Get default model for language pair"""
        return f"Helsinki-NLP/opus-mt-{self.source_lang}-{self.target_lang}"
    
    def _load_model(self) -> None:
        """Load translation model"""
        try:
            self.model, self.tokenizer = self.model_manager.get_model(
                self.model_name,
                device=self.device
            )
            logger.info(f"Loaded translation model: {self.model_name}")
        except Exception as e:
            logger.error(f"Failed to load model {self.model_name}: {e}")
            raise
    
    def translate(self, text: str) -> str:
        """Translate single text"""
        try:
            # Tokenize
            inputs = self.tokenizer(
                text,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512
            ).to(self.device)
            
            # Translate
            with torch.no_grad():
                outputs = self.model.generate(**inputs)
            
            # Decode
            return self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        except Exception as e:
            logger.error(f"Translation failed: {e}")
            return text
    
    def translate_batch(
        self,
        texts: List[str],
        batch_size: int = 32,
        show_progress: bool = True
    ) -> List[str]:
        """Translate batch of texts"""
        results = []
        
        # Process in batches
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            try:
                # Tokenize batch
                inputs = self.tokenizer(
                    batch,
                    return_tensors="pt",
                    padding=True,
                    truncation=True,
                    max_length=512
                ).to(self.device)
                
                # Translate batch
                with torch.no_grad():
                    outputs = self.model.generate(**inputs)
                
                # Decode batch
                translations = [
                    self.tokenizer.decode(output, skip_special_tokens=True)
                    for output in outputs
                ]
                results.extend(translations)
                
            except Exception as e:
                logger.error(f"Batch translation failed: {e}")
                # Add original texts for failed translations
                results.extend(batch)
        
        return results
    
    def get_supported_languages(self) -> Dict[str, List[str]]:
        """Get supported language pairs"""
        return self.model_manager.get_supported_languages() 