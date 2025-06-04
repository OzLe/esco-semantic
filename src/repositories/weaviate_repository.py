from typing import List, Dict, Any, Optional, TYPE_CHECKING
import numpy as np
import logging
from .base_repository import BaseRepository
from ..exceptions import WeaviateError

if TYPE_CHECKING:
    from ..esco_weaviate_client import WeaviateClient

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
    
    def create_object(self, properties: Dict[str, Any], uuid: Optional[str] = None, vector: Optional[List[float]] = None) -> str:
        """Legacy method for creating objects with properties and UUID."""
        try:
            # Map 'uri' to 'conceptUri' for consistency with schema
            if 'uri' in properties:
                properties['conceptUri'] = properties.pop('uri')
            
            if uuid:
                # Use batch to create with specific UUID
                with self.client.client.batch as batch:
                    batch.batch_size = 1
                    result = batch.add_data_object(
                        data_object=properties,
                        class_name=self.class_name,
                        uuid=uuid,
                        vector=vector
                    )
                return result
            else:
                return self.create(properties, vector)
        except Exception as e:
            logger.error(f"Failed to create {self.class_name} object: {str(e)}")
            raise WeaviateError(f"Failed to create {self.class_name} object: {str(e)}")
    
    def get_all_objects(self) -> List[Dict[str, Any]]:
        """Get all objects of this class type."""
        try:
            result = (
                self.client.client.query
                .get(self.class_name)
                .with_additional(["id"])
                .do()
            )
            objects = result.get("data", {}).get("Get", {}).get(self.class_name, [])
            # Map the additional id to _id for backward compatibility
            for obj in objects:
                if "_additional" in obj and "id" in obj["_additional"]:
                    obj["_id"] = obj["_additional"]["id"]
            return objects
        except Exception as e:
            logger.error(f"Failed to get all {self.class_name} objects: {str(e)}")
            return []
    
    def count_objects(self) -> int:
        """Count the number of objects in this class."""
        try:
            result = (
                self.client.client.query
                .aggregate(self.class_name)
                .with_meta_count()
                .do()
            )
            return result.get("data", {}).get("Aggregate", {}).get(self.class_name, [{}])[0].get("meta", {}).get("count", 0)
        except Exception as e:
            logger.error(f"Failed to count {self.class_name} objects: {str(e)}")
            return 0
    
    def get_objects_by_property(self, property_name: str, property_value: str) -> List[Dict[str, Any]]:
        """Get objects by a specific property value."""
        try:
            result = (
                self.client.client.query
                .get(self.class_name)
                .with_additional(["id"])
                .with_where({
                    "path": [property_name],
                    "operator": "Equal",
                    "valueString": property_value
                })
                .do()
            )
            objects = result.get("data", {}).get("Get", {}).get(self.class_name, [])
            # Map the additional id to _id for backward compatibility
            for obj in objects:
                if "_additional" in obj and "id" in obj["_additional"]:
                    obj["_id"] = obj["_additional"]["id"]
            return objects
        except Exception as e:
            logger.error(f"Failed to get {self.class_name} objects by {property_name}={property_value}: {str(e)}")
            return []
    
    def get_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get an entity by its URI from Weaviate, including its ID."""
        try:
            query = (
                self.client.client.query
                .get(self.class_name) # Get all properties
                .with_additional(["id"]) # Explicitly ask for ID
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .with_limit(1) # Ensure only one result
            )
            result = query.do()
            
            items = result.get("data", {}).get("Get", {}).get(self.class_name)
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
                batch.batch_size = self.client.config['weaviate']['batch_size']
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
    
    def batch_import(self, data_list: List[Dict[str, Any]], vectors: List[np.ndarray]) -> List[str]:
        """Import multiple entities in a batch (wrapper for batch_create)."""
        return self.batch_create(data_list, vectors)
    
    def check_object_exists(self, uri: str) -> bool:
        """Check if an object with the given URI exists."""
        return self.exists(uri)

    def add_skill_relations(self, occupation_uri: str, essential_skills: List[str], optional_skills: List[str]) -> bool:
        """Add skill relations to an occupation (delegate to occupation repository)."""
        try:
            occupation_repo = self.client.get_repository("Occupation")
            return occupation_repo.add_skill_relations(occupation_uri, essential_skills, optional_skills)
        except Exception as e:
            logger.error(f"Failed to add skill relations for {occupation_uri}: {str(e)}")
            return False

    def add_hierarchical_relation(self, broader_uri: str, narrower_uri: str, relation_type: str = "Skill") -> bool:
        """Add a hierarchical relation between two entities."""
        try:
            # Get broader entity ID
            broader_result = (
                self.client.client.query
                .get(relation_type, ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": broader_uri
                })
                .do()
            )
            
            if not broader_result["data"]["Get"][relation_type]:
                return False
                
            broader_id = broader_result["data"]["Get"][relation_type][0]["_additional"]["id"]
            
            # Get narrower entity ID
            narrower_result = (
                self.client.client.query
                .get(relation_type, ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": narrower_uri
                })
                .do()
            )
            
            if not narrower_result["data"]["Get"][relation_type]:
                return False
                
            narrower_id = narrower_result["data"]["Get"][relation_type][0]["_additional"]["id"]
            
            # Add relation
            self.client.client.data_object.reference.add(
                from_uuid=broader_id,
                from_class_name=relation_type,
                from_property_name="hasNarrower",
                to_uuid=narrower_id,
                to_class_name=relation_type
            )
            
            return True
        except Exception as e:
            logger.error(f"Failed to add hierarchical relation: {str(e)}")
            return False

    def add_skill_to_skill_relation(self, from_skill_uri: str, to_skill_uri: str, relation_type: str) -> bool:
        """Add a relation between two skills."""
        try:
            # Get source skill ID
            from_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": from_skill_uri
                })
                .do()
            )
            
            if not from_result["data"]["Get"]["Skill"]:
                return False
                
            from_id = from_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
            
            # Get target skill ID
            to_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": to_skill_uri
                })
                .do()
            )
            
            if not to_result["data"]["Get"]["Skill"]:
                return False
                
            to_id = to_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
            
            # Add relation based on type
            if relation_type == "broader":
                self.client.client.data_object.reference.add(
                    from_uuid=from_id,
                    from_class_name="Skill",
                    from_property_name="hasBroader",
                    to_uuid=to_id,
                    to_class_name="Skill"
                )
            elif relation_type == "narrower":
                self.client.client.data_object.reference.add(
                    from_uuid=from_id,
                    from_class_name="Skill",
                    from_property_name="hasNarrower",
                    to_uuid=to_id,
                    to_class_name="Skill"
                )
            elif relation_type == "related":
                self.client.client.data_object.reference.add(
                    from_uuid=from_id,
                    from_class_name="Skill",
                    from_property_name="hasRelated",
                    to_uuid=to_id,
                    to_class_name="Skill"
                )
            else:
                logger.error(f"Unknown relation type: {relation_type}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Failed to add skill-to-skill relation: {str(e)}")
            return False

    def upsert(self, data: Dict[str, Any], vector: Optional[List[float]] = None) -> str:
        """Create or update an entity based on conceptUri"""
        try:
            # Check if object exists
            existing_object = self.get_by_uri(data.get('conceptUri'))
            
            if existing_object:
                # Update existing object
                object_id = existing_object.get('_additional', {}).get('id')
                if object_id:
                    # Preserve the conceptUri in the update
                    update_data = {k: v for k, v in data.items() if k != 'conceptUri'}
                    self.client.client.data_object.update(
                        class_name=self.class_name,
                        uuid=object_id,
                        data_object=update_data
                        # Vector is not typically updated directly this way with Weaviate's standard update.
                        # If vector update is needed, object might need to be re-created or specific vector update API used.
                    )
                    logger.debug(f"Updated existing {self.class_name}: {data.get('conceptUri')}")
                    return object_id
            
            # Create new object if not existing or if update failed to find ID
            logger.debug(f"Creating new {self.class_name}: {data.get('conceptUri')}")
            return self.create(data, vector)
        except Exception as e:
            logger.error(f"Failed to upsert {self.class_name} for URI {data.get('conceptUri')}: {str(e)}")
            raise WeaviateError(f"Failed to upsert {self.class_name} for URI {data.get('conceptUri')}: {str(e)}")

    def batch_upsert(self, data_list: List[Dict[str, Any]], vectors: List[np.ndarray]) -> List[Optional[str]]:
        """Batch upsert operation"""
        results = []
        for data_item, vector_item in zip(data_list, vectors):
            try:
                # Convert numpy array to list if necessary for the upsert method
                vector_list = vector_item.tolist() if isinstance(vector_item, np.ndarray) else vector_item
                result_id = self.upsert(data_item, vector_list)
                results.append(result_id)
            except Exception as e: # Catching broad exception from upsert
                logger.error(f"Failed to upsert item {data_item.get('conceptUri', 'Unknown URI')} in batch: {str(e)}")
                results.append(None) # Append None or some indicator of failure for this item
        return results 