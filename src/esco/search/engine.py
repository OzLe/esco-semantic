from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from ..core.logging import setup_logging
from ..embeddings.generator import EmbeddingGenerator

logger = setup_logging(__name__)

@dataclass
class SearchResult:
    """Container for search results"""
    uri: str
    label: str
    description: Optional[str] = None
    score: float = 0.0
    type: str = "unknown"
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            'uri': self.uri,
            'label': self.label,
            'description': self.description,
            'score': self.score,
            'type': self.type,
            'metadata': self.metadata or {}
        }

class SearchEngine(ABC):
    """Abstract base class for search engines"""
    
    def __init__(self, embedding_generator: EmbeddingGenerator):
        self.embedding_generator = embedding_generator
    
    @abstractmethod
    def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchResult]:
        """Perform search"""
        pass
    
    @abstractmethod
    def search_by_vector(
        self,
        vector: List[float],
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchResult]:
        """Search using pre-computed vector"""
        pass
    
    @abstractmethod
    def get_by_id(self, id: str) -> Optional[SearchResult]:
        """Get item by ID"""
        pass
    
    @abstractmethod
    def get_similar(
        self,
        id: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """Get similar items"""
        pass

    @abstractmethod
    def semantic_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        min_score: float = 0.7
    ) -> List[SearchResult]:
        """Perform semantic search"""
        pass
    
    @abstractmethod
    def hybrid_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        min_score: float = 0.7,
        semantic_weight: float = 0.7
    ) -> List[SearchResult]:
        """Perform hybrid search combining keyword and semantic search"""
        pass 