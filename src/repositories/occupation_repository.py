from typing import List, Dict, Any, Optional, TYPE_CHECKING
import numpy as np
import logging
from .weaviate_repository import WeaviateRepository

if TYPE_CHECKING:
    from ..esco_weaviate_client import WeaviateClient

logger = logging.getLogger(__name__)

class OccupationRepository(WeaviateRepository):
    """Repository for Occupation entities."""
    
    def __init__(self, client: 'WeaviateClient'):
        """Initialize the Occupation repository."""
        super().__init__(client, "Occupation")
        self.logger = logger
    
    def get_related_skills(self, uri: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get essential and optional skills for an occupation."""
        try:
            result = (
                self.client.client.query
                .get("Occupation", ["conceptUri", "preferredLabel"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": uri
                })
                .do()
            )

            if not result["data"]["Get"]["Occupation"]:
                return {"essential": [], "optional": []}

            occupation = result["data"]["Get"]["Occupation"][0]
            
            # Get essential skills
            essential_skills = (
                self.client.client.query
                .get("Skill", ["conceptUri", "preferredLabel"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "In",
                    "valueString": occupation.get("hasEssentialSkill", [])
                })
                .do()
            )

            # Get optional skills
            optional_skills = (
                self.client.client.query
                .get("Skill", ["conceptUri", "preferredLabel"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "In",
                    "valueString": occupation.get("hasOptionalSkill", [])
                })
                .do()
            )

            return {
                "essential": essential_skills["data"]["Get"]["Skill"],
                "optional": optional_skills["data"]["Get"]["Skill"]
            }
        except Exception as e:
            self.logger.error(f"Failed to get related skills for {uri}: {str(e)}")
            return {"essential": [], "optional": []}
    
    def add_skill_relations(self, occupation_uri: str, essential_skills: List[str], optional_skills: List[str]) -> bool:
        """Add skill relations to an occupation."""
        try:
            # Get the occupation ID
            result = (
                self.client.client.query
                .get("Occupation", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .do()
            )
            
            if not result["data"]["Get"]["Occupation"]:
                return False
                
            occupation_id = result["data"]["Get"]["Occupation"][0]["_additional"]["id"]

            # Add essential skills
            for skill_uri in essential_skills:
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
                    continue
                    
                skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
                
                self.client.client.data_object.reference.add(
                    from_uuid=occupation_id,
                    from_class_name="Occupation",
                    from_property_name="hasEssentialSkill",
                    to_uuid=skill_id,
                    to_class_name="Skill"
                )

            # Add optional skills
            for skill_uri in optional_skills:
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
                    continue
                    
                skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
                
                self.client.client.data_object.reference.add(
                    from_uuid=occupation_id,
                    from_class_name="Occupation",
                    from_property_name="hasOptionalSkill",
                    to_uuid=skill_id,
                    to_class_name="Skill"
                )
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add skill relations for {occupation_uri}: {str(e)}")
            return False

    def add_occupation_group_relation(self, occupation_uri: str, group_uri: str) -> bool:
        """Add a reference from an Occupation to an ISCOGroup."""
        try:
            # Get occupation ID
            occ_result = (
                self.client.client.query
                .get("Occupation", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .do()
            )
            if not occ_result["data"]["Get"]["Occupation"]:
                return False
            occ_id = occ_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]

            # Get ISCOGroup ID
            group_result = (
                self.client.client.query
                .get("ISCOGroup", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": group_uri
                })
                .do()
            )
            if not group_result["data"]["Get"]["ISCOGroup"]:
                return False
            group_id = group_result["data"]["Get"]["ISCOGroup"][0]["_additional"]["id"]

            # Add reference
            self.client.client.data_object.reference.add(
                from_uuid=occ_id,
                from_class_name="Occupation",
                from_property_name="memberOfISCOGroup",
                to_uuid=group_id,
                to_class_name="ISCOGroup"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add occupation-group relation: {str(e)}")
            return False

    def add_isco_group_relation(self, occupation_uri: str, isco_group_uri: str) -> bool:
        """Add a reference from an Occupation to an ISCOGroup (alias for add_occupation_group_relation)."""
        return self.add_occupation_group_relation(occupation_uri, isco_group_uri)

    def add_essential_skill_relation(self, occupation_uri: str, skill_uri: str) -> bool:
        """Add an essential skill relation to an occupation."""
        try:
            # Get occupation ID
            occ_result = (
                self.client.client.query
                .get("Occupation", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .do()
            )
            if not occ_result["data"]["Get"]["Occupation"]:
                return False
            occ_id = occ_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]

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

            # Add reference
            self.client.client.data_object.reference.add(
                from_uuid=occ_id,
                from_class_name="Occupation",
                from_property_name="hasEssentialSkill",
                to_uuid=skill_id,
                to_class_name="Skill"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add essential skill relation: {str(e)}")
            return False

    def add_optional_skill_relation(self, occupation_uri: str, skill_uri: str) -> bool:
        """Add an optional skill relation to an occupation."""
        try:
            # Get occupation ID
            occ_result = (
                self.client.client.query
                .get("Occupation", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .do()
            )
            if not occ_result["data"]["Get"]["Occupation"]:
                return False
            occ_id = occ_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]

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

            # Add reference
            self.client.client.data_object.reference.add(
                from_uuid=occ_id,
                from_class_name="Occupation",
                from_property_name="hasOptionalSkill",
                to_uuid=skill_id,
                to_class_name="Skill"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add optional skill relation: {str(e)}")
            return False

    def add_broader_occupation_relation(self, occupation_uri: str, broader_uri: str) -> bool:
        """Add a broader occupation relation."""
        try:
            # Get occupation ID
            occ_result = (
                self.client.client.query
                .get("Occupation", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .do()
            )
            if not occ_result["data"]["Get"]["Occupation"]:
                return False
            occ_id = occ_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]

            # Get broader occupation ID
            broader_result = (
                self.client.client.query
                .get("Occupation", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": broader_uri
                })
                .do()
            )
            if not broader_result["data"]["Get"]["Occupation"]:
                return False
            broader_id = broader_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]

            # Add reference
            self.client.client.data_object.reference.add(
                from_uuid=occ_id,
                from_class_name="Occupation",
                from_property_name="hasBroaderOccupation",
                to_uuid=broader_id,
                to_class_name="Occupation"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add broader occupation relation: {str(e)}")
            return False 