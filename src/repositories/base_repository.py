from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import numpy as np

class BaseRepository(ABC):
    """Base repository interface defining common operations for all repositories."""
    
    @abstractmethod
    def create(self, data: Dict[str, Any], vector: Optional[List[float]] = None) -> str:
        """Create a new entity in the database."""
        pass
    
    @abstractmethod
    def get_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get an entity by its URI."""
        pass
    
    @abstractmethod
    def update(self, uri: str, data: Dict[str, Any]) -> bool:
        """Update an existing entity."""
        pass
    
    @abstractmethod
    def delete(self, uri: str) -> bool:
        """Delete an entity by its URI."""
        pass
    
    @abstractmethod
    def search(self, query_vector: np.ndarray, limit: int = 10, certainty: float = 0.75) -> List[Dict[str, Any]]:
        """Perform semantic search using a query vector."""
        pass
    
    @abstractmethod
    def batch_create(self, data_list: List[Dict[str, Any]], vectors: List[np.ndarray]) -> List[str]:
        """Create multiple entities in a batch."""
        pass
    
    @abstractmethod
    def exists(self, uri: str) -> bool:
        """Check if an entity exists by its URI."""
        pass 