from typing import List, Dict, Optional
from collections import OrderedDict
import numpy as np
from ..core.logging import setup_logging

logger = setup_logging(__name__)

class EmbeddingCache:
    """LRU cache for embeddings"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._cache: OrderedDict[str, List[float]] = OrderedDict()
    
    def __getitem__(self, key: str) -> List[float]:
        """Get embedding from cache"""
        if key not in self._cache:
            raise KeyError(f"Key not found in cache: {key}")
        
        # Move to end (most recently used)
        value = self._cache.pop(key)
        self._cache[key] = value
        return value
    
    def __setitem__(self, key: str, value: List[float]) -> None:
        """Add embedding to cache"""
        # Remove if exists
        if key in self._cache:
            self._cache.pop(key)
        
        # Add new value
        self._cache[key] = value
        
        # Remove oldest if over size limit
        if len(self._cache) > self.max_size:
            self._cache.popitem(last=False)
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in cache"""
        return key in self._cache
    
    def get(self, key: str, default: Optional[List[float]] = None) -> Optional[List[float]]:
        """Get embedding from cache with default value"""
        try:
            return self[key]
        except KeyError:
            return default
    
    def clear(self) -> None:
        """Clear the cache"""
        self._cache.clear()
    
    def __len__(self) -> int:
        """Get cache size"""
        return len(self._cache)
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'size': len(self._cache),
            'max_size': self.max_size,
            'memory_usage': sum(
                len(value) * 8  # 8 bytes per float
                for value in self._cache.values()
            )
        } 