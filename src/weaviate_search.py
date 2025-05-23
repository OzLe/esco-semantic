import logging
from typing import Dict, List, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
from weaviate_client import WeaviateClient

logger = logging.getLogger(__name__)

class WeaviateSearchEngine:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        self.weaviate_client = WeaviateClient(config_path, profile)
        self.model = SentenceTransformer('sentence-transformers/multi-qa-MiniLM-L6-cos-v1')

    def search(self, query: str, limit: int = 10, certainty: float = 0.75) -> List[Dict]:
        """
        Perform semantic search using a text query.
        
        Args:
            query: The search query text
            limit: Maximum number of results to return
            certainty: Minimum similarity threshold (0-1)
            
        Returns:
            List of dictionaries containing search results with their related skills
        """
        try:
            # Generate query vector
            query_vector = self.model.encode(query)
            
            # Perform semantic search
            results = self.weaviate_client.semantic_search(
                query_vector=query_vector,
                limit=limit,
                certainty=certainty
            )
            
            # Enrich results with related skills
            enriched_results = []
            for result in results:
                skills = self.weaviate_client.get_related_skills(result["conceptUri"])
                enriched_results.append({
                    "occupation": result,
                    "skills": skills
                })
            
            return enriched_results
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return []

    def format_results(self, results: List[Dict]) -> str:
        """
        Format search results as a human-readable string.
        
        Args:
            results: List of search results from search() method
            
        Returns:
            Formatted string representation of results
        """
        if not results:
            return "No results found."
        
        output = []
        output.append("=" * 72)
        output.append(" ESCO Semantic Search Results ")
        output.append("=" * 72)
        output.append("")
        
        for i, result in enumerate(results, 1):
            occupation = result["occupation"]
            skills = result["skills"]
            
            # Format occupation
            output.append(f"{i}. [Occupation] {occupation['preferredLabel']}")
            output.append(f"   URI: {occupation['conceptUri']}")
            output.append(f"   Similarity: {occupation['_additional']['certainty']:.2%}")
            if occupation.get("description"):
                output.append(f"   Description: {occupation['description']}")
            
            # Format essential skills
            if skills["essential"]:
                output.append("\n   Essential Skills:")
                for skill in skills["essential"]:
                    output.append(f"   • {skill['preferredLabel']}")
            
            # Format optional skills
            if skills["optional"]:
                output.append("\n   Optional Skills:")
                for skill in skills["optional"]:
                    output.append(f"   • {skill['preferredLabel']}")
            
            output.append("\n" + "-" * 72)
        
        return "\n".join(output) 