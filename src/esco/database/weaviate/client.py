from typing import List, Dict, Any, Optional
import weaviate
import numpy as np
import logging
from ..client import DatabaseClient
from ...core.exceptions import DatabaseError
from ...core.config import Config, get_config
from .schema import WeaviateSchemaManager

logger = logging.getLogger(__name__)

class WeaviateClient(DatabaseClient):
    """Weaviate-specific database client implementation"""
    
    def __init__(self):
        self.config = get_config()
        self.client = None
        self._schema_manager = None
    
    def connect(self) -> None:
        """Establish connection to Weaviate"""
        try:
            self.client = weaviate.Client(
                url=self.config.get('weaviate.url'),
                timeout_config=(
                    self.config.get('weaviate.connect_timeout', 5),
                    self.config.get('weaviate.read_timeout', 60)
                )
            )
            logger.info("Connected to Weaviate")
        except Exception as e:
            raise DatabaseError(f"Failed to connect to Weaviate: {e}")
    
    def disconnect(self) -> None:
        """Close Weaviate connection"""
        # Weaviate client doesn't require explicit disconnect
        self.client = None
        logger.info("Disconnected from Weaviate")
    
    def health_check(self) -> bool:
        """Check Weaviate health"""
        try:
            return self.client.is_ready()
        except Exception:
            return False
    
    def create_collection(self, name: str, schema: Dict[str, Any]) -> None:
        """Create a Weaviate class"""
        try:
            if not self.client.schema.contains(schema):
                self.client.schema.create_class(schema)
                logger.info(f"Created class: {name}")
        except Exception as e:
            raise DatabaseError(f"Failed to create class {name}: {e}")
    
    def delete_collection(self, name: str) -> None:
        """Delete a Weaviate class"""
        try:
            self.client.schema.delete_class(name)
            logger.info(f"Deleted class: {name}")
        except Exception as e:
            raise DatabaseError(f"Failed to delete class {name}: {e}")
    
    def insert_batch(
        self, 
        collection: str, 
        objects: List[Dict[str, Any]], 
        vectors: Optional[List[List[float]]] = None
    ) -> None:
        """Insert batch of objects into Weaviate"""
        try:
            with self.client.batch as batch:
                batch.batch_size = self.config.get('weaviate.batch_size', 100)
                for i, obj in enumerate(objects):
                    vector = vectors[i] if vectors else None
                    batch.add_data_object(
                        data_object=obj,
                        class_name=collection,
                        vector=vector
                    )
            logger.info(f"Inserted {len(objects)} objects into {collection}")
        except Exception as e:
            raise DatabaseError(f"Failed to insert batch into {collection}: {e}")
    
    def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Vector search in Weaviate"""
        try:
            query = (
                self.client.query
                .get(collection)
                .with_near_vector({
                    "vector": query_vector
                })
                .with_limit(limit)
            )
            
            if filters:
                query = query.with_where(filters)
            
            result = query.do()
            return result["data"]["Get"][collection]
        except Exception as e:
            raise DatabaseError(f"Failed to search in {collection}: {e}")
    
    def get_by_id(self, collection: str, object_id: str) -> Optional[Dict[str, Any]]:
        """Get object by ID from Weaviate"""
        try:
            result = (
                self.client.query
                .get(collection)
                .with_additional(["id"])
                .with_where({
                    "path": ["id"],
                    "operator": "Equal",
                    "valueString": object_id
                })
                .do()
            )
            objects = result["data"]["Get"][collection]
            return objects[0] if objects else None
        except Exception as e:
            raise DatabaseError(f"Failed to get object {object_id} from {collection}: {e}")
    
    def create_reference(
        self,
        from_collection: str,
        from_id: str,
        to_collection: str,
        to_id: str,
        property_name: str
    ) -> None:
        """Create reference between objects in Weaviate"""
        try:
            self.client.data_object.reference.add(
                from_class_name=from_collection,
                from_uuid=from_id,
                from_property_name=property_name,
                to_class_name=to_collection,
                to_uuid=to_id
            )
            logger.info(f"Created reference from {from_id} to {to_id}")
        except Exception as e:
            raise DatabaseError(f"Failed to create reference: {e}")
    
    def delete_by_id(self, collection: str, object_id: str) -> None:
        """Delete object by ID from Weaviate"""
        try:
            self.client.data_object.delete(
                uuid=object_id,
                class_name=collection
            )
            logger.info(f"Deleted object {object_id} from {collection}")
        except Exception as e:
            raise DatabaseError(f"Failed to delete object {object_id} from {collection}: {e}")
    
    def update_by_id(
        self,
        collection: str,
        object_id: str,
        data: Dict[str, Any]
    ) -> None:
        """Update object by ID in Weaviate"""
        try:
            self.client.data_object.update(
                data_object=data,
                class_name=collection,
                uuid=object_id
            )
            logger.info(f"Updated object {object_id} in {collection}")
        except Exception as e:
            raise DatabaseError(f"Failed to update object {object_id} in {collection}: {e}")
    
    def get_schema(self, collection: str) -> Dict[str, Any]:
        """Get Weaviate class schema"""
        try:
            return self.client.schema.get(collection)
        except Exception as e:
            raise DatabaseError(f"Failed to get schema for {collection}: {e}")
    
    def update_schema(
        self,
        collection: str,
        schema: Dict[str, Any]
    ) -> None:
        """Update Weaviate class schema"""
        try:
            self.client.schema.update_class(schema)
            logger.info(f"Updated schema for {collection}")
        except Exception as e:
            raise DatabaseError(f"Failed to update schema for {collection}: {e}")
    
    def _ensure_schema(self) -> None:
        """Ensure the required schema exists in Weaviate"""
        try:
            # Load and create base schemas
            schemas = self._schema_manager.get_base_schemas()
            
            # Create base collections first
            for class_name, schema in schemas.items():
                if not self.client.schema.exists(class_name):
                    self.client.schema.create_class(schema)
            
            # Add reference properties after all classes exist
            self._add_reference_properties()
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise DatabaseError(f"Failed to create schema: {str(e)}")
    
    def _add_reference_properties(self) -> None:
        """Add reference properties after all classes are created"""
        try:
            references = self._schema_manager.load_references()
            
            for class_name, refs in references.items():
                for ref in refs:
                    if not self._schema_manager.property_exists(self.client, class_name, ref["name"]):
                        self.client.schema.property.create(class_name, ref)
        except Exception as e:
            logger.error(f"Failed to add reference properties: {str(e)}")
            raise DatabaseError(f"Failed to add reference properties: {str(e)}")
    
    def create_schema(self, schema: Dict[str, Any]) -> None:
        """Create Weaviate schema"""
        try:
            self.client.schema.create(schema)
        except Exception as e:
            raise DatabaseError(f"Failed to create schema: {str(e)}")
    
    def insert(self, collection: str, data: List[Dict[str, Any]]) -> None:
        """Insert data into Weaviate collection"""
        try:
            with self.client.batch as batch:
                batch.batch_size = self.config.get('weaviate.batch_size', 100)
                for item in data:
                    batch.add_data_object(
                        data_object=item,
                        class_name=collection
                    )
        except Exception as e:
            raise DatabaseError(f"Failed to insert data: {str(e)}")
    
    def delete(self, collection: str, filters: Dict[str, Any]) -> None:
        """Delete data from Weaviate collection"""
        try:
            weaviate_filter = self._convert_filters(filters)
            self.client.batch.delete_objects(
                class_name=collection,
                where=weaviate_filter
            )
        except Exception as e:
            raise DatabaseError(f"Failed to delete data: {str(e)}")
    
    def update(self, collection: str, data: Dict[str, Any], filters: Dict[str, Any]) -> None:
        """Update data in Weaviate collection"""
        try:
            weaviate_filter = self._convert_filters(filters)
            self.client.data_object.update(
                data_object=data,
                class_name=collection,
                where=weaviate_filter
            )
        except Exception as e:
            raise DatabaseError(f"Failed to update data: {str(e)}")
    
    def _convert_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Convert generic filters to Weaviate filter format"""
        # Remove class_name as it's not part of the filter
        filters = filters.copy()
        filters.pop('class_name', None)
        
        # Convert to Weaviate filter format
        weaviate_filter = {
            "operator": "And",
            "operands": [
                {
                    "path": [key],
                    "operator": "Equal",
                    "valueString": value
                }
                for key, value in filters.items()
            ]
        }
        return weaviate_filter
    
    def batch_import_with_vectors(self, collection: str, items: List[Dict], vectors: List) -> None:
        """Import items with vectors in batches"""
        with self.client.batch as batch:
            batch.batch_size = self.config.get('weaviate.batch_size', 100)
            for item, vector in zip(items, vectors):
                try:
                    # Handle both numpy arrays and lists
                    if isinstance(vector, np.ndarray):
                        vector_list = vector.tolist()
                    elif isinstance(vector, list):
                        vector_list = vector
                    else:
                        raise ValueError(f"Unexpected vector type: {type(vector)}")
                    
                    batch.add_data_object(
                        data_object=item,
                        class_name=collection,
                        vector=vector_list
                    )
                except Exception as e:
                    logger.error(f"Failed to import item {item.get('conceptUri', '')}: {str(e)}")
    
    def add_relation(self, from_class: str, to_class: str, from_uri: str, to_uri: str, relation_type: str) -> None:
        """Add a relation between two objects"""
        try:
            # Get source object
            source_result = (
                self.client.query
                .get(from_class, ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": from_uri
                })
                .do()
            )
            
            if not source_result["data"]["Get"][from_class]:
                logger.warning(f"Source object {from_uri} not found")
                return
            
            source_id = source_result["data"]["Get"][from_class][0]["_additional"]["id"]
            
            # Get target object
            target_result = (
                self.client.query
                .get(to_class, ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": to_uri
                })
                .do()
            )
            
            if not target_result["data"]["Get"][to_class]:
                logger.warning(f"Target object {to_uri} not found")
                return
            
            target_id = target_result["data"]["Get"][to_class][0]["_additional"]["id"]
            
            # Add reference
            self.client.data_object.reference.add(
                from_class_name=from_class,
                from_uuid=source_id,
                from_property_name=relation_type,
                to_class_name=to_class,
                to_uuid=target_id
            )
        except Exception as e:
            raise DatabaseError(f"Failed to add relation: {str(e)}")
    
    def check_object_exists(self, class_name: str, uri: str) -> bool:
        """Check if an object exists in the database"""
        try:
            result = (
                self.client.query
                .get(class_name, ["conceptUri"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .do()
            )
            return bool(result["data"]["Get"][class_name])
        except Exception as e:
            logger.error(f"Failed to check object existence: {str(e)}")
            return False 