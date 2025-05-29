from typing import List, Dict, Any, Optional, TYPE_CHECKING
import numpy as np
from .weaviate_repository import WeaviateRepository

if TYPE_CHECKING:
    from ..esco_weaviate_client import WeaviateClient

class SkillCollectionRepository(WeaviateRepository):
    """Repository for Skill Collection entities."""
    
    def __init__(self, client: 'WeaviateClient'):
        """Initialize the Skill Collection repository."""
        super().__init__(client, "SkillCollection")
    
    def add_skill_relation(self, collection_uri: str, skill_uri: str) -> bool:
        """Add relation between skill collection and skill."""
        try:
            # Get the collection ID
            collection_result = (
                self.client.client.query
                .get("SkillCollection", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": collection_uri
                })
                .do()
            )
            
            if not collection_result["data"]["Get"]["SkillCollection"]:
                self.logger.warning(f"Skill collection {collection_uri} not found")
                return False
                
            collection_id = collection_result["data"]["Get"]["SkillCollection"][0]["_additional"]["id"]
            
            # Get the skill ID
            skill_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": skill_uri
                })
                .do()
            )
            
            if not skill_result["data"]["Get"]["Skill"]:
                self.logger.warning(f"Skill {skill_uri} not found")
                return False
                
            skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
            
            # Add relation
            self.client.client.data_object.reference.add(
                from_uuid=collection_id,
                from_class_name="SkillCollection",
                from_property_name="hasSkill",
                to_uuid=skill_id,
                to_class_name="Skill"
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add skill relation for collection {collection_uri}: {str(e)}")
            return False 

    def add_skill_collection_relation(self, collection_uri: str, skill_uri: str) -> bool:
        """Add a reference from a SkillCollection to a Skill."""
        try:
            # Get collection ID
            collection_result = (
                self.client.client.query
                .get("SkillCollection", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": collection_uri
                })
                .do()
            )
            if not collection_result["data"]["Get"]["SkillCollection"]:
                return False
            collection_id = collection_result["data"]["Get"]["SkillCollection"][0]["_additional"]["id"]

            # Get skill ID
            skill_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": skill_uri
                })
                .do()
            )
            if not skill_result["data"]["Get"]["Skill"]:
                return False
            skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]

            # Add relation
            self.client.client.data_object.reference.add(
                from_uuid=collection_id,
                from_class_name="SkillCollection",
                from_property_name="hasSkill",
                to_uuid=skill_id,
                to_class_name="Skill"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add skill collection relation: {str(e)}")
            return False 