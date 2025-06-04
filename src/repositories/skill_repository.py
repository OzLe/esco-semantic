from typing import List, Dict, Any, Optional, TYPE_CHECKING
import numpy as np
from .weaviate_repository import WeaviateRepository

if TYPE_CHECKING:
    from ..esco_weaviate_client import WeaviateClient

class SkillRepository(WeaviateRepository):
    """Repository for Skill entities."""
    
    def __init__(self, client: 'WeaviateClient'):
        """Initialize the Skill repository."""
        super().__init__(client, "Skill")
    
    def add_skill_to_skill_relation(self, from_skill_uri: str, to_skill_uri: str, relation_type: str) -> bool:
        """Add a related skill reference between two skills."""
        try:
            # Get the 'from' skill ID
            from_skill_result = (
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
            if not from_skill_result["data"]["Get"]["Skill"]:
                self.logger.warning(f"From-skill {from_skill_uri} not found for skill-to-skill relation.")
                return False
            from_skill_id = from_skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]

            # Get the 'to' skill ID
            to_skill_result = (
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
            if not to_skill_result["data"]["Get"]["Skill"]:
                self.logger.warning(f"To-skill {to_skill_uri} not found for skill-to-skill relation.")
                return False
            to_skill_id = to_skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]

            # Add reference from 'from_skill' to 'to_skill'
            self.client.client.data_object.reference.add(
                from_uuid=from_skill_id,
                from_class_name="Skill",
                from_property_name="hasRelatedSkill",
                to_uuid=to_skill_id,
                to_class_name="Skill"
            )
            
            # Add reference from 'to_skill' to 'from_skill' for bidirectionality
            self.client.client.data_object.reference.add(
                from_uuid=to_skill_id,
                from_class_name="Skill",
                from_property_name="hasRelatedSkill",
                to_uuid=from_skill_id,
                to_class_name="Skill"
            )
            
            self.logger.debug(f"Added skill-to-skill relation ({relation_type}) between {from_skill_uri} and {to_skill_uri}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to add skill-to-skill relation between {from_skill_uri} and {to_skill_uri}: {str(e)}")
            return False
    
    def add_hierarchical_relation(self, broader_uri: str, narrower_uri: str, relation_type: str = "Skill") -> bool:
        """Add hierarchical relation between broader and narrower skills."""
        try:
            # Get the broader skill ID
            broader_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": broader_uri
                })
                .do()
            )
            
            if not broader_result["data"]["Get"]["Skill"]:
                self.logger.warning(f"Broader skill {broader_uri} not found")
                return False
                
            broader_id = broader_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
            
            # Get the narrower skill ID
            narrower_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": narrower_uri
                })
                .do()
            )
            
            if not narrower_result["data"]["Get"]["Skill"]:
                self.logger.warning(f"Narrower skill {narrower_uri} not found")
                return False
                
            narrower_id = narrower_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
            
            # Add broader-narrower relation
            self.client.client.data_object.reference.add(
                from_uuid=broader_id,
                from_class_name="Skill",
                from_property_name="narrowerSkill",
                to_uuid=narrower_id,
                to_class_name="Skill"
            )
            self.client.client.data_object.reference.add(
                from_uuid=narrower_id,
                from_class_name="Skill",
                from_property_name="broaderSkill",
                to_uuid=broader_id,
                to_class_name="Skill"
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add hierarchical relation between {broader_uri} and {narrower_uri}: {str(e)}")
            return False 
    
    def add_broader_skill_relation(self, skill_uri: str, broader_uri: str) -> bool:
        """Add a broaderSkill reference from a Skill to another Skill."""
        try:
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

            # Get broader skill ID
            broader_result = (
                self.client.client.query
                .get("Skill", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": broader_uri
                })
                .do()
            )
            if not broader_result["data"]["Get"]["Skill"]:
                return False
            broader_id = broader_result["data"]["Get"]["Skill"][0]["_additional"]["id"]

            # Add relation
            self.client.client.data_object.reference.add(
                from_uuid=skill_id,
                from_class_name="Skill",
                from_property_name="broaderSkill",
                to_uuid=broader_id,
                to_class_name="Skill"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add broader skill relation: {str(e)}")
            return False 