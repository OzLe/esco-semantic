from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from .engine import SearchResult
from .filters import SearchFilter
from ..core.exceptions import SearchError
from ..core.logging import setup_logging

logger = setup_logging(__name__)

@dataclass
class BatchSearchResult:
    """Result of batch search operation"""
    query: str
    results: List[SearchResult]
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            'query': self.query,
            'results': [r.to_dict() for r in self.results],
            'error': self.error
        }

class BatchSearch:
    """Handles batch search operations"""
    
    def __init__(self, search_engine, max_workers: int = 4):
        self.search_engine = search_engine
        self.max_workers = max_workers
    
    def search(
        self,
        queries: List[str],
        filters: Optional[SearchFilter] = None,
        limit: int = 10,
        min_score: float = 0.7
    ) -> List[BatchSearchResult]:
        """Perform batch search"""
        try:
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all queries
                future_to_query = {
                    executor.submit(
                        self.search_engine.search,
                        query,
                        filters.to_dict() if filters else None,
                        limit,
                        min_score
                    ): query
                    for query in queries
                }
                
                # Process results as they complete
                for future in as_completed(future_to_query):
                    query = future_to_query[future]
                    try:
                        search_results = future.result()
                        results.append(
                            BatchSearchResult(
                                query=query,
                                results=search_results
                            )
                        )
                    except Exception as e:
                        logger.error(f"Failed to process query '{query}': {str(e)}")
                        results.append(
                            BatchSearchResult(
                                query=query,
                                results=[],
                                error=str(e)
                            )
                        )
            
            return results
        except Exception as e:
            raise SearchError(f"Failed to perform batch search: {str(e)}")
    
    def semantic_search(
        self,
        queries: List[str],
        filters: Optional[SearchFilter] = None,
        limit: int = 10,
        min_score: float = 0.7
    ) -> List[BatchSearchResult]:
        """Perform batch semantic search"""
        try:
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all queries
                future_to_query = {
                    executor.submit(
                        self.search_engine.semantic_search,
                        query,
                        filters.to_dict() if filters else None,
                        limit,
                        min_score
                    ): query
                    for query in queries
                }
                
                # Process results as they complete
                for future in as_completed(future_to_query):
                    query = future_to_query[future]
                    try:
                        search_results = future.result()
                        results.append(
                            BatchSearchResult(
                                query=query,
                                results=search_results
                            )
                        )
                    except Exception as e:
                        logger.error(f"Failed to process query '{query}': {str(e)}")
                        results.append(
                            BatchSearchResult(
                                query=query,
                                results=[],
                                error=str(e)
                            )
                        )
            
            return results
        except Exception as e:
            raise SearchError(f"Failed to perform batch semantic search: {str(e)}")
    
    def hybrid_search(
        self,
        queries: List[str],
        filters: Optional[SearchFilter] = None,
        limit: int = 10,
        min_score: float = 0.7,
        semantic_weight: float = 0.7
    ) -> List[BatchSearchResult]:
        """Perform batch hybrid search"""
        try:
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all queries
                future_to_query = {
                    executor.submit(
                        self.search_engine.hybrid_search,
                        query,
                        filters.to_dict() if filters else None,
                        limit,
                        min_score,
                        semantic_weight
                    ): query
                    for query in queries
                }
                
                # Process results as they complete
                for future in as_completed(future_to_query):
                    query = future_to_query[future]
                    try:
                        search_results = future.result()
                        results.append(
                            BatchSearchResult(
                                query=query,
                                results=search_results
                            )
                        )
                    except Exception as e:
                        logger.error(f"Failed to process query '{query}': {str(e)}")
                        results.append(
                            BatchSearchResult(
                                query=query,
                                results=[],
                                error=str(e)
                            )
                        )
            
            return results
        except Exception as e:
            raise SearchError(f"Failed to perform batch hybrid search: {str(e)}") 