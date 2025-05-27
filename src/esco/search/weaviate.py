from typing import List, Dict, Any, Optional
import weaviate
from .engine import SearchEngine, SearchResult
from ..core.exceptions import SearchError
from ..core.config import Config
from ..database.weaviate.client import WeaviateClient

class WeaviateSearchEngine(SearchEngine):
    """Weaviate-specific search engine implementation"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = WeaviateClient(config)
    
    def search(
        self, 
        query: str, 
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        min_score: float = 0.7
    ) -> List[SearchResult]:
        """Perform keyword search"""
        try:
            results = self.client.search(
                query=query,
                filters=filters,
                limit=limit
            )
            return self._format_results(results, min_score)
        except Exception as e:
            raise SearchError(f"Failed to perform search: {str(e)}")
    
    def semantic_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        min_score: float = 0.7
    ) -> List[SearchResult]:
        """Perform semantic search"""
        try:
            # Use Weaviate's nearText search
            results = self.client.search(
                query=query,
                filters=filters,
                limit=limit
            )
            return self._format_results(results, min_score)
        except Exception as e:
            raise SearchError(f"Failed to perform semantic search: {str(e)}")
    
    def hybrid_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        min_score: float = 0.7,
        semantic_weight: float = 0.7
    ) -> List[SearchResult]:
        """Perform hybrid search"""
        try:
            # Get both keyword and semantic results
            keyword_results = self.search(query, filters, limit, min_score)
            semantic_results = self.semantic_search(query, filters, limit, min_score)
            
            # Combine and rank results
            combined_results = self._combine_results(
                keyword_results,
                semantic_results,
                semantic_weight
            )
            
            # Sort by score and limit
            return sorted(combined_results, key=lambda x: x.score, reverse=True)[:limit]
        except Exception as e:
            raise SearchError(f"Failed to perform hybrid search: {str(e)}")
    
    def _format_results(self, results: List[Dict[str, Any]], min_score: float) -> List[SearchResult]:
        """Format search results"""
        formatted_results = []
        for result in results:
            if result.get('score', 0) >= min_score:
                formatted_results.append(
                    SearchResult(
                        uri=result.get('uri', ''),
                        label=result.get('label', ''),
                        description=result.get('description'),
                        score=result.get('score', 0),
                        type=result.get('type', ''),
                        metadata=result.get('metadata', {})
                    )
                )
        return formatted_results
    
    def _combine_results(
        self,
        keyword_results: List[SearchResult],
        semantic_results: List[SearchResult],
        semantic_weight: float
    ) -> List[SearchResult]:
        """Combine and rank results from different search methods"""
        # Create a dictionary to store combined results
        combined = {}
        
        # Process keyword results
        for result in keyword_results:
            combined[result.uri] = {
                'result': result,
                'score': result.score * (1 - semantic_weight)
            }
        
        # Process semantic results
        for result in semantic_results:
            if result.uri in combined:
                # Update existing result
                combined[result.uri]['score'] += result.score * semantic_weight
            else:
                # Add new result
                combined[result.uri] = {
                    'result': result,
                    'score': result.score * semantic_weight
                }
        
        # Convert back to list of SearchResult objects
        return [
            SearchResult(
                uri=item['result'].uri,
                label=item['result'].label,
                description=item['result'].description,
                score=item['score'],
                type=item['result'].type,
                metadata=item['result'].metadata
            )
            for item in combined.values()
        ] 