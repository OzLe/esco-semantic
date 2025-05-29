from typing import List, Dict, Any, Optional, TYPE_CHECKING
import numpy as np
from .weaviate_repository import WeaviateRepository

if TYPE_CHECKING:
    from ..weaviate_client import WeaviateClient

class ISCOGroupRepository(WeaviateRepository):
    """Repository for ISCO Group entities."""
    
    def __init__(self, client: 'WeaviateClient'):
        """Initialize the ISCO Group repository."""
        super().__init__(client, "ISCOGroup")
    
    def add_hierarchical_relation(self, broader_uri: str, narrower_uri: str) -> bool:
        """Add hierarchical relation between broader and narrower ISCO groups."""
        try:
            # Get the broader ISCO group ID
            broader_result = (
                self.client.client.query
                .get("ISCOGroup", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": broader_uri
                })
                .do()
            )
            
            if not broader_result["data"]["Get"]["ISCOGroup"]:
                self.logger.warning(f"Broader ISCO group {broader_uri} not found")
                return False
                
            broader_id = broader_result["data"]["Get"]["ISCOGroup"][0]["_additional"]["id"]
            
            # Get the narrower ISCO group ID
            narrower_result = (
                self.client.client.query
                .get("ISCOGroup", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": narrower_uri
                })
                .do()
            )
            
            if not narrower_result["data"]["Get"]["ISCOGroup"]:
                self.logger.warning(f"Narrower ISCO group {narrower_uri} not found")
                return False
                
            narrower_id = narrower_result["data"]["Get"]["ISCOGroup"][0]["_additional"]["id"]
            
            # Add broader-narrower relation
            self.client.client.data_object.reference.add(
                from_uuid=broader_id,
                from_class_name="ISCOGroup",
                from_property_name="narrowerISCOGroup",
                to_uuid=narrower_id,
                to_class_name="ISCOGroup"
            )
            self.client.client.data_object.reference.add(
                from_uuid=narrower_id,
                from_class_name="ISCOGroup",
                from_property_name="broaderISCOGroup",
                to_uuid=broader_id,
                to_class_name="ISCOGroup"
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add hierarchical relation between {broader_uri} and {narrower_uri}: {str(e)}")
            return False 