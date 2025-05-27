from typing import List, Dict, Any, Optional
import numpy as np
from .engine import SearchEngine, SearchResult
from ..database.client import DatabaseClient
from ..core.logging import setup_logging
from ..core.config import get_config
from ..embeddings.generator import EmbeddingGenerator

logger = setup_logging(__name__)

class SemanticSearchEngine(SearchEngine):
    """Semantic search implementation using vector similarity"""
    
    def __init__(
        self,
        db_client: DatabaseClient,
        embedding_generator: EmbeddingGenerator
    ):
        super().__init__(embedding_generator)
        self.db_client = db_client
        self.config = get_config()
    
    def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchResult]:
        """Perform semantic search"""
        # Generate query embedding
        query_vector = self.embedding_generator.generate(query)
        
        # Perform vector search
        return self.search_by_vector(
            vector=query_vector,
            filters=filters,
            limit=limit,
            offset=offset
        )
    
    def search_by_vector(
        self,
        vector: List[float],
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchResult]:
        """Search using pre-computed vector"""
        try:
            # Perform vector search in database
            results = self.db_client.search(
                collection="entities",
                query_vector=vector,
                limit=limit,
                offset=offset,
                filters=filters
            )
            
            # Convert to SearchResult objects
            return [
                SearchResult(
                    uri=result['uri'],
                    label=result['preferredLabel'],
                    description=result.get('description'),
                    score=result['score'],
                    type=result.get('type', 'unknown'),
                    metadata=result.get('metadata', {})
                )
                for result in results
            ]
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    def get_by_id(self, id: str) -> Optional[SearchResult]:
        """Get item by ID"""
        try:
            result = self.db_client.get_by_id("entities", id)
            if not result:
                return None
            
            return SearchResult(
                uri=result['uri'],
                label=result['preferredLabel'],
                description=result.get('description'),
                type=result.get('type', 'unknown'),
                metadata=result.get('metadata', {})
            )
        except Exception as e:
            logger.error(f"Failed to get item {id}: {e}")
            return None
    
    def get_similar(
        self,
        id: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """Get similar items"""
        # Get the item first
        item = self.get_by_id(id)
        if not item:
            return []
        
        # Get its vector
        try:
            vector = self.db_client.get_vector("entities", id)
            if not vector:
                return []
            
            # Search for similar items
            return self.search_by_vector(
                vector=vector,
                filters=filters,
                limit=limit
            )
        except Exception as e:
            logger.error(f"Failed to get similar items for {id}: {e}")
            return [] 