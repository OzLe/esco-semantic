from typing import List, Dict, Any, Optional
import numpy as np
from .search_engine import SearchEngine, SearchResult
from .embedding_utils import get_embeddings, cosine_similarity

class SemanticSearchEngine(SearchEngine):
    """Semantic search engine implementation using embeddings."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the semantic search engine.
        
        Args:
            config: Dictionary containing search engine configuration
                Required keys:
                - model_name: Name of the embedding model to use
                - cache_dir: Directory to cache model files
                - batch_size: Batch size for embedding generation
        """
        super().__init__(config)
        self.model_name = config.get("model_name", "sentence-transformers/all-MiniLM-L6-v2")
        self.cache_dir = config.get("cache_dir", "models")
        self.batch_size = config.get("batch_size", 32)
        self._initialize_model()

    def _initialize_model(self):
        """Initialize the embedding model."""
        try:
            # Load the model and tokenizer
            self.model, self.tokenizer = get_embeddings(
                model_name=self.model_name,
                cache_dir=self.cache_dir
            )
            self.logger.info(f"Initialized semantic search engine with model: {self.model_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize semantic search engine: {str(e)}")
            raise

    async def search(
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
        return await self.semantic_search(query, limit, filters, **kwargs)

    async def semantic_search(
        self,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> List[SearchResult]:
        """Perform a semantic search operation using embeddings.
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            filters: Optional filters to apply to the search
            **kwargs: Additional search parameters
                - corpus: List of documents to search in
                - metadata: List of metadata for each document
            
        Returns:
            List of SearchResult objects
        """
        corpus = kwargs.get("corpus", [])
        metadata_list = kwargs.get("metadata", [{}] * len(corpus))
        
        if not corpus:
            self.logger.warning("No corpus provided for semantic search")
            return []

        try:
            # Generate embeddings for query and corpus
            query_embedding = get_embeddings(
                texts=[query],
                model=self.model,
                tokenizer=self.tokenizer,
                batch_size=1
            )[0]

            corpus_embeddings = get_embeddings(
                texts=corpus,
                model=self.model,
                tokenizer=self.tokenizer,
                batch_size=self.batch_size
            )

            # Calculate similarities
            similarities = cosine_similarity(query_embedding, corpus_embeddings)
            
            # Get top results
            top_indices = np.argsort(similarities)[-limit:][::-1]
            
            results = []
            for idx in top_indices:
                results.append(SearchResult(
                    id=str(idx),
                    content=corpus[idx],
                    score=float(similarities[idx]),
                    metadata=metadata_list[idx],
                    source="semantic_search"
                ))
            
            return results

        except Exception as e:
            self.logger.error(f"Error in semantic search: {str(e)}")
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
        # For now, we'll just return semantic search results
        # In a real implementation, this would combine results from both
        # text search and semantic search with appropriate weighting
        return await self.semantic_search(query, limit, filters, **kwargs)

    async def close(self):
        """Clean up resources used by the search engine."""
        # Clean up model resources if needed
        pass 