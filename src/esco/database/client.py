from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class DatabaseClient(ABC):
    """Abstract database client interface"""
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection"""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if database is healthy"""
        pass
    
    @abstractmethod
    def create_collection(self, name: str, schema: Dict[str, Any]) -> None:
        """Create a collection/class"""
        pass
    
    @abstractmethod
    def delete_collection(self, name: str) -> None:
        """Delete a collection/class"""
        pass
    
    @abstractmethod
    def insert_batch(
        self, 
        collection: str, 
        objects: List[Dict[str, Any]], 
        vectors: Optional[List[List[float]]] = None
    ) -> None:
        """Insert batch of objects"""
        pass
    
    @abstractmethod
    def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Vector search"""
        pass
    
    @abstractmethod
    def get_by_id(self, collection: str, object_id: str) -> Optional[Dict[str, Any]]:
        """Get object by ID"""
        pass
    
    @abstractmethod
    def create_reference(
        self,
        from_collection: str,
        from_id: str,
        to_collection: str,
        to_id: str,
        property_name: str
    ) -> None:
        """Create reference between objects"""
        pass
    
    @abstractmethod
    def delete_by_id(self, collection: str, object_id: str) -> None:
        """Delete object by ID"""
        pass
    
    @abstractmethod
    def update_by_id(
        self,
        collection: str,
        object_id: str,
        data: Dict[str, Any]
    ) -> None:
        """Update object by ID"""
        pass
    
    @abstractmethod
    def get_schema(self, collection: str) -> Dict[str, Any]:
        """Get collection schema"""
        pass
    
    @abstractmethod
    def update_schema(
        self,
        collection: str,
        schema: Dict[str, Any]
    ) -> None:
        """Update collection schema"""
        pass 