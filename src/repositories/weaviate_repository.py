from typing import List, Dict, Any, Optional, TYPE_CHECKING
import numpy as np
import logging
from .base_repository import BaseRepository
from ..exceptions import WeaviateError

if TYPE_CHECKING:
    from ..weaviate_client import WeaviateClient

logger = logging.getLogger(__name__)

class WeaviateRepository(BaseRepository):
    """Weaviate-specific implementation of the base repository."""
    
    def __init__(self, client: 'WeaviateClient', class_name: str):
        """Initialize the repository with a Weaviate client and class name."""
        self.client = client
        self.class_name = class_name
    
    def create(self, data: Dict[str, Any], vector: Optional[List[float]] = None) -> str:
        """Create a new entity in Weaviate."""
        try:
            with self.client.client.batch as batch:
                batch.batch_size = 1
                result = batch.add_data_object(
                    data_object=data,
                    class_name=self.class_name,
                    vector=vector
                )
            return result
        except Exception as e:
            logger.error(f"Failed to create {self.class_name}: {str(e)}")
            raise WeaviateError(f"Failed to create {self.class_name}: {str(e)}")
    
    def get_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get an entity by its URI from Weaviate."""
        try:
            result = (
                self.client.client.query
                .get(self.class_name)
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .do()
            )
            items = result["data"]["Get"][self.class_name]
            return items[0] if items else None
        except Exception as e:
            logger.error(f"Failed to get {self.class_name} by URI {uri}: {str(e)}")
            return None
    
    def update(self, uri: str, data: Dict[str, Any]) -> bool:
        """Update an existing entity in Weaviate."""
        try:
            # Get the object ID first
            result = (
                self.client.client.query
                .get(self.class_name, ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .do()
            )
            
            if not result["data"]["Get"][self.class_name]:
                return False
                
            object_id = result["data"]["Get"][self.class_name][0]["_additional"]["id"]
            
            # Update the object
            self.client.client.data_object.update(
                data_object=data,
                class_name=self.class_name,
                uuid=object_id
            )
            return True
        except Exception as e:
            logger.error(f"Failed to update {self.class_name} {uri}: {str(e)}")
            return False
    
    def delete(self, uri: str) -> bool:
        """Delete an entity by its URI from Weaviate."""
        try:
            # Get the object ID first
            result = (
                self.client.client.query
                .get(self.class_name, ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .do()
            )
            
            if not result["data"]["Get"][self.class_name]:
                return False
                
            object_id = result["data"]["Get"][self.class_name][0]["_additional"]["id"]
            
            # Delete the object
            self.client.client.data_object.delete(
                class_name=self.class_name,
                uuid=object_id
            )
            return True
        except Exception as e:
            logger.error(f"Failed to delete {self.class_name} {uri}: {str(e)}")
            return False
    
    def search(self, query_vector: np.ndarray, limit: int = 10, certainty: float = 0.75) -> List[Dict[str, Any]]:
        """Perform semantic search using a query vector in Weaviate."""
        try:
            vector_list = query_vector.tolist() if isinstance(query_vector, np.ndarray) else query_vector
            
            result = (
                self.client.client.query
                .get(self.class_name)
                .with_near_vector({
                    "vector": vector_list,
                    "certainty": certainty
                })
                .with_limit(limit)
                .with_additional(["certainty"])
                .do()
            )
            return result["data"]["Get"][self.class_name]
        except Exception as e:
            logger.error(f"Semantic search failed for {self.class_name}: {str(e)}")
            return []
    
    def batch_create(self, data_list: List[Dict[str, Any]], vectors: List[np.ndarray]) -> List[str]:
        """Create multiple entities in a batch in Weaviate."""
        try:
            results = []
            with self.client.client.batch as batch:
                batch.batch_size = self.client.config['batch_size']
                for data, vector in zip(data_list, vectors):
                    vector_list = vector.tolist() if isinstance(vector, np.ndarray) else vector
                    result = batch.add_data_object(
                        data_object=data,
                        class_name=self.class_name,
                        vector=vector_list
                    )
                    results.append(result)
            return results
        except Exception as e:
            logger.error(f"Failed to batch create {self.class_name}: {str(e)}")
            raise WeaviateError(f"Failed to batch create {self.class_name}: {str(e)}")
    
    def exists(self, uri: str) -> bool:
        """Check if an entity exists by its URI in Weaviate."""
        try:
            result = (
                self.client.client.query
                .get(self.class_name, ["conceptUri"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .do()
            )
            return len(result["data"]["Get"][self.class_name]) > 0
        except Exception as e:
            logger.error(f"Error checking existence of {self.class_name} {uri}: {str(e)}")
            return False 