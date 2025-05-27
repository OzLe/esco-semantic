from typing import List, Optional, Dict, Any
import torch
from sentence_transformers import SentenceTransformer
from ..core.config import get_config
from ..core.logging import setup_logging
from .cache import EmbeddingCache
from .models import ModelManager

logger = setup_logging(__name__)

class EmbeddingGenerator:
    """Handles generation of embeddings for text using various models"""
    
    def __init__(
        self,
        model_name: Optional[str] = None,
        cache_size: int = 10000,
        device: Optional[str] = None
    ):
        self.config = get_config()
        self.model_name = model_name or self.config.get('embeddings.default_model', 'all-MiniLM-L6-v2')
        self.device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
        
        # Initialize components
        self.model_manager = ModelManager()
        self.cache = EmbeddingCache(max_size=cache_size)
        self.model = None
        
        # Load model
        self._load_model()
    
    def _load_model(self) -> None:
        """Load the embedding model"""
        try:
            self.model = self.model_manager.get_model(self.model_name)
            self.model.to(self.device)
            logger.info(f"Loaded embedding model: {self.model_name} on {self.device}")
        except Exception as e:
            logger.error(f"Failed to load model {self.model_name}: {e}")
            raise
    
    def generate(self, text: str) -> List[float]:
        """Generate embedding for a single text"""
        # Check cache first
        if text in self.cache:
            return self.cache[text]
        
        # Generate embedding
        with torch.no_grad():
            embedding = self.model.encode(
                text,
                convert_to_tensor=True,
                show_progress_bar=False
            )
            embedding = embedding.cpu().numpy().tolist()
        
        # Cache result
        self.cache[text] = embedding
        return embedding
    
    def generate_batch(
        self,
        texts: List[str],
        batch_size: int = 32,
        show_progress: bool = True
    ) -> List[List[float]]:
        """Generate embeddings for a batch of texts"""
        # Check cache for each text
        uncached_texts = []
        cached_embeddings = []
        indices = []
        
        for i, text in enumerate(texts):
            if text in self.cache:
                cached_embeddings.append(self.cache[text])
            else:
                uncached_texts.append(text)
                indices.append(i)
        
        # Generate embeddings for uncached texts
        if uncached_texts:
            with torch.no_grad():
                new_embeddings = self.model.encode(
                    uncached_texts,
                    batch_size=batch_size,
                    show_progress_bar=show_progress,
                    convert_to_tensor=True
                )
                new_embeddings = new_embeddings.cpu().numpy().tolist()
            
            # Cache new embeddings
            for text, embedding in zip(uncached_texts, new_embeddings):
                self.cache[text] = embedding
        
        # Combine results in original order
        result = [None] * len(texts)
        for i, embedding in zip(indices, new_embeddings):
            result[i] = embedding
        for i, embedding in enumerate(cached_embeddings):
            result[i] = embedding
        
        return result
    
    def clear_cache(self) -> None:
        """Clear the embedding cache"""
        self.cache.clear() 