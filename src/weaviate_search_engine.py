from typing import List, Dict, Any, Optional
import weaviate
from weaviate.util import generate_uuid
from .search_engine import SearchEngine, SearchResult

class WeaviateSearchEngine(SearchEngine):
    """Search engine implementation using Weaviate vector database."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Weaviate search engine.
        
        Args:
            config: Dictionary containing search engine configuration
                Required keys:
                - url: Weaviate server URL
                - auth_client_secret: Authentication credentials
                - class_name: Name of the Weaviate class to search
                - properties: List of properties to search and return
        """
        super().__init__(config)
        self.url = config["url"]
        self.auth_client_secret = config.get("auth_client_secret")
        self.class_name = config["class_name"]
        self.properties = config.get("properties", ["content"])
        self._initialize_client()

    def _initialize_client(self):
        """Initialize the Weaviate client."""
        try:
            self.client = weaviate.Client(
                url=self.url,
                auth_client_secret=self.auth_client_secret
            )
            self.logger.info(f"Initialized Weaviate client with URL: {self.url}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Weaviate client: {str(e)}")
            raise

    async def search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[SearchResult]:
        """Perform a text search operation using Weaviate.
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            **kwargs: Additional search parameters
            
        Returns:
            List of SearchResult objects
        """
        try:
            # Build the query
            query_builder = (
                self.client.query
                .get(self.class_name, self.properties)
                .with_bm25(query=query)
                .with_limit(limit)
            )

            # Add filters if provided
            if filters:
                for field, value in filters.items():
                    query_builder = query_builder.with_where({
                        "path": [field],
                        "operator": "Equal",
                        "valueString": value
                    })

            # Execute the query
            result = query_builder.do()

            # Process results
            search_results = []
            if "data" in result and "Get" in result["data"]:
                for item in result["data"]["Get"][self.class_name]:
                    search_results.append(SearchResult(
                        id=item.get("_additional", {}).get("id", generate_uuid()),
                        content=item.get("content", ""),
                        score=item.get("_additional", {}).get("score", 0.0),
                        metadata={k: v for k, v in item.items() if k not in ["content", "_additional"]},
                        source="weaviate_text_search"
                    ))

            return search_results

        except Exception as e:
            self.logger.error(f"Error in Weaviate text search: {str(e)}")
            raise

    async def semantic_search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[SearchResult]:
        """Perform a semantic search operation using Weaviate.
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            **kwargs: Additional search parameters
            
        Returns:
            List of SearchResult objects
        """
        try:
            # Build the query
            query_builder = (
                self.client.query
                .get(self.class_name, self.properties)
                .with_near_text({"concepts": [query]})
                .with_limit(limit)
            )

            # Add filters if provided
            if filters:
                for field, value in filters.items():
                    query_builder = query_builder.with_where({
                        "path": [field],
                        "operator": "Equal",
                        "valueString": value
                    })

            # Execute the query
            result = query_builder.do()

            # Process results
            search_results = []
            if "data" in result and "Get" in result["data"]:
                for item in result["data"]["Get"][self.class_name]:
                    search_results.append(SearchResult(
                        id=item.get("_additional", {}).get("id", generate_uuid()),
                        content=item.get("content", ""),
                        score=item.get("_additional", {}).get("certainty", 0.0),
                        metadata={k: v for k, v in item.items() if k not in ["content", "_additional"]},
                        source="weaviate_semantic_search"
                    ))

            return search_results

        except Exception as e:
            self.logger.error(f"Error in Weaviate semantic search: {str(e)}")
            raise

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
        try:
            # Get results from both search methods
            text_results = await self.search(query, limit, filters, **kwargs)
            semantic_results = await self.semantic_search(query, limit, filters, **kwargs)

            # Combine and normalize scores
            combined_results = {}
            for result in text_results:
                combined_results[result.id] = {
                    "result": result,
                    "score": result.score * (1 - semantic_weight)
                }

            for result in semantic_results:
                if result.id in combined_results:
                    combined_results[result.id]["score"] += result.score * semantic_weight
                else:
                    combined_results[result.id] = {
                        "result": result,
                        "score": result.score * semantic_weight
                    }

            # Sort by combined score and return top results
            sorted_results = sorted(
                combined_results.values(),
                key=lambda x: x["score"],
                reverse=True
            )[:limit]

            return [item["result"] for item in sorted_results]

        except Exception as e:
            self.logger.error(f"Error in Weaviate hybrid search: {str(e)}")
            raise

    async def close(self):
        """Clean up resources used by the search engine."""
        if hasattr(self, 'client'):
            self.client.close() 