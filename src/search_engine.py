from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class SearchResult:
    """Represents a single search result with metadata."""
    id: str
    content: str
    score: float
    metadata: Dict[str, Any]
    source: str
    timestamp: datetime = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert the search result to a dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "score": self.score,
            "metadata": self.metadata,
            "source": self.source,
            "timestamp": self.timestamp.isoformat()
        }

class SearchEngine(ABC):
    """Base class for all search engine implementations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the search engine with configuration.
        
        Args:
            config: Dictionary containing search engine configuration
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[SearchResult]:
        """Perform a search operation.
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            **kwargs: Additional search parameters
            
        Returns:
            List of SearchResult objects
        """
        pass

    @abstractmethod
    async def semantic_search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[SearchResult]:
        """Perform a semantic search operation.
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            **kwargs: Additional search parameters
            
        Returns:
            List of SearchResult objects
        """
        pass

    @abstractmethod
    async def hybrid_search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        semantic_weight: float = 0.5,
        **kwargs
    ) -> List[SearchResult]:
        """Perform a hybrid search combining text and semantic search.
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            semantic_weight: Weight to give to semantic search results (0-1)
            **kwargs: Additional search parameters
            
        Returns:
            List of SearchResult objects
        """
        pass

    async def batch_search(
        self,
        queries: List[str],
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, List[SearchResult]]:
        """Perform batch search operations.
        
        Args:
            queries: List of search query strings
            limit: Maximum number of results per query
            filters: Optional filters to apply to the search
            **kwargs: Additional search parameters
            
        Returns:
            Dictionary mapping queries to their search results
        """
        results = {}
        for query in queries:
            try:
                results[query] = await self.search(query, limit, filters, **kwargs)
            except Exception as e:
                self.logger.error(f"Error processing query '{query}': {str(e)}")
                results[query] = []
        return results

    def format_results(
        self,
        results: List[SearchResult],
        format: str = "json"
    ) -> Union[str, Dict[str, Any]]:
        """Format search results in the specified format.
        
        Args:
            results: List of SearchResult objects
            format: Output format ("json" or "dict")
            
        Returns:
            Formatted results in the specified format
        """
        if format == "json":
            return [result.to_dict() for result in results]
        elif format == "dict":
            return {result.id: result.to_dict() for result in results}
        else:
            raise ValueError(f"Unsupported format: {format}")

    async def close(self):
        """Clean up resources used by the search engine."""
        pass 