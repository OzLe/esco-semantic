import weaviate
import yaml
import logging
from typing import Dict, List, Optional, Any
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

class WeaviateClient:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize Weaviate client with configuration."""
        if not config_path:
            config_path = "config/weaviate_config.yaml"
        self.config = self._load_config(config_path, profile)
        self.client = self._initialize_client()
        self._ensure_schema()

    def _load_config(self, config_path: str, profile: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            if profile not in config:
                raise ValueError(f"Profile '{profile}' not found in config file")
            return config[profile]
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except Exception as e:
            raise ValueError(f"Failed to load config file: {str(e)}")

    def _initialize_client(self) -> weaviate.Client:
        """Initialize Weaviate client with configuration."""
        return weaviate.Client(
            url=self.config['url'],
            additional_headers={},
            timeout_config=(5, 60)  # (connect timeout, read timeout)
        )

    def _ensure_schema(self):
        """Ensure the required schema exists in Weaviate."""
        # Define base ISCOGroup collection without references
        isco_group_collection = {
            "class": "ISCOGroup",
            "vectorizer": "none",
            "vectorIndexConfig": self.config['vector_index_config'],
            "properties": [
                {
                    "name": "conceptUri",
                    "dataType": ["string"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "code",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "preferredLabel_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "description_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                }
            ]
        }

        # Define base Occupation collection without references
        occupation_collection = {
            "class": "Occupation",
            "vectorizer": "none",
            "vectorIndexConfig": self.config['vector_index_config'],
            "properties": [
                {
                    "name": "conceptUri",
                    "dataType": ["string"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "code",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "preferredLabel_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "altLabels_en",
                    "dataType": ["text[]"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "description_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "definition_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                }
            ]
        }

        # Define base Skill collection without references
        skill_collection = {
            "class": "Skill",
            "vectorizer": "none",
            "vectorIndexConfig": self.config['vector_index_config'],
            "properties": [
                {
                    "name": "conceptUri",
                    "dataType": ["string"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "code",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "preferredLabel_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "altLabels_en",
                    "dataType": ["text[]"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "description_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "definition_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "skillType",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "reuseLevel",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                }
            ]
        }

        # Define base SkillCollection collection without references
        skill_collection_class = {
            "class": "SkillCollection",
            "vectorizer": "none",
            "vectorIndexConfig": self.config['vector_index_config'],
            "properties": [
                {
                    "name": "conceptUri",
                    "dataType": ["string"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "preferredLabel_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "description_en",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                }
            ]
        }

        # Create base collections first
        try:
            if not self.client.schema.exists("ISCOGroup"):
                self.client.schema.create_class(isco_group_collection)
            if not self.client.schema.exists("Skill"):
                self.client.schema.create_class(skill_collection)
            if not self.client.schema.exists("SkillCollection"):
                self.client.schema.create_class(skill_collection_class)
            if not self.client.schema.exists("Occupation"):
                self.client.schema.create_class(occupation_collection)
            
            # Add reference properties after all classes exist
            self._add_reference_properties()
                
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise

    def _add_reference_properties(self):
        """Add reference properties after all classes are created."""
        try:
            # Add ISCOGroup references
            isco_group_refs = [
                {
                    "name": "hasOccupation",
                    "dataType": ["Occupation"],
                    "isIndexed": True
                },
                {
                    "name": "broaderISCOGroup",
                    "dataType": ["ISCOGroup"],
                    "isIndexed": True
                },
                {
                    "name": "narrowerISCOGroup",
                    "dataType": ["ISCOGroup"],
                    "isIndexed": True
                }
            ]
            
            for ref in isco_group_refs:
                if not self._property_exists("ISCOGroup", ref["name"]):
                    self.client.schema.property.create(
                        "ISCOGroup",
                        ref
                    )

            # Add Occupation references
            occupation_refs = [
                {
                    "name": "memberOfISCOGroup",
                    "dataType": ["ISCOGroup"],
                    "isIndexed": True
                },
                {
                    "name": "hasEssentialSkill",
                    "dataType": ["Skill"],
                    "isIndexed": True
                },
                {
                    "name": "hasOptionalSkill",
                    "dataType": ["Skill"],
                    "isIndexed": True
                },
                {
                    "name": "broaderOccupation",
                    "dataType": ["Occupation"],
                    "isIndexed": True
                },
                {
                    "name": "narrowerOccupation",
                    "dataType": ["Occupation"],
                    "isIndexed": True
                }
            ]
            
            for ref in occupation_refs:
                if not self._property_exists("Occupation", ref["name"]):
                    self.client.schema.property.create(
                        "Occupation",
                        ref
                    )

            # Add Skill references
            skill_refs = [
                {
                    "name": "isEssentialForOccupation",
                    "dataType": ["Occupation"],
                    "isIndexed": True
                },
                {
                    "name": "isOptionalForOccupation",
                    "dataType": ["Occupation"],
                    "isIndexed": True
                },
                {
                    "name": "broaderSkill",
                    "dataType": ["Skill"],
                    "isIndexed": True
                },
                {
                    "name": "narrowerSkill",
                    "dataType": ["Skill"],
                    "isIndexed": True
                },
                {
                    "name": "hasRelatedSkill",
                    "dataType": ["Skill"],
                    "isIndexed": True
                },
                {
                    "name": "memberOfSkillCollection",
                    "dataType": ["SkillCollection"],
                    "isIndexed": True
                }
            ]
            
            for ref in skill_refs:
                if not self._property_exists("Skill", ref["name"]):
                    self.client.schema.property.create(
                        "Skill",
                        ref
                    )

            # Add SkillCollection references
            skill_collection_refs = [
                {
                    "name": "hasSkill",
                    "dataType": ["Skill"],
                    "isIndexed": True
                }
            ]
            
            for ref in skill_collection_refs:
                if not self._property_exists("SkillCollection", ref["name"]):
                    self.client.schema.property.create(
                        "SkillCollection",
                        ref
                    )
                
        except Exception as e:
            logger.error(f"Failed to add reference properties: {str(e)}")
            raise

    def _property_exists(self, class_name: str, property_name: str) -> bool:
        """Check if a property exists in a class."""
        try:
            schema = self.client.schema.get(class_name)
            return any(prop.get("name") == property_name for prop in schema.get("properties", []))
        except Exception:
            return False

    def batch_import_occupations(self, occupations: List[Dict], vectors: List):
        """Import occupations in batches."""
        with self.client.batch as batch:
            batch.batch_size = self.config['batch_size']
            for occupation, vector in zip(occupations, vectors):
                try:
                    # Handle both numpy arrays and lists
                    if isinstance(vector, np.ndarray):
                        vector_list = vector.tolist()
                    elif isinstance(vector, list):
                        vector_list = vector
                    else:
                        raise ValueError(f"Unexpected vector type: {type(vector)}")
                    
                    batch.add_data_object(
                        data_object={
                            "conceptUri": occupation["conceptUri"],
                            "code": occupation.get("code", ""),
                            "preferredLabel_en": occupation["preferredLabel_en"],
                            "description_en": occupation.get("description_en", ""),
                            "definition_en": occupation.get("definition_en", ""),
                            "iscoGroup": occupation.get("iscoGroup", ""),
                            "altLabels_en": occupation.get("altLabels_en", [])
                        },
                        class_name="Occupation",
                        vector=vector_list
                    )
                except Exception as e:
                    logger.error(f"Failed to import occupation {occupation['conceptUri']}: {str(e)}")

    def batch_import_skills(self, skills: List[Dict], vectors: List):
        """Import skills in batches."""
        with self.client.batch as batch:
            batch.batch_size = self.config['batch_size']
            for skill, vector in zip(skills, vectors):
                try:
                    # Handle both numpy arrays and lists
                    if isinstance(vector, np.ndarray):
                        vector_list = vector.tolist()
                    elif isinstance(vector, list):
                        vector_list = vector
                    else:
                        raise ValueError(f"Unexpected vector type: {type(vector)}")
                    
                    batch.add_data_object(
                        data_object={
                            "conceptUri": skill["conceptUri"],
                            "code": skill.get("code", ""),
                            "preferredLabel_en": skill["preferredLabel_en"],
                            "description_en": skill.get("description_en", ""),
                            "definition_en": skill.get("definition_en", ""),
                            "skillType": skill.get("skillType", ""),
                            "reuseLevel": skill.get("reuseLevel", ""),
                            "altLabels_en": skill.get("altLabels_en", [])
                        },
                        class_name="Skill",
                        vector=vector_list
                    )
                except Exception as e:
                    logger.error(f"Failed to import skill {skill['conceptUri']}: {str(e)}")

    def add_skill_relations(self, occupation_uri: str, essential_skills: List[str], optional_skills: List[str]):
        """Add skill relations to an occupation."""
        try:
            # Query for the occupation using conceptUri
            result = (
                self.client.query
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
                logger.warning(f"Occupation {occupation_uri} not found")
                return
                
            occupation = result["data"]["Get"]["Occupation"][0]
            occupation_id = occupation["_additional"]["id"]

            # Add essential skills
            for skill_uri in essential_skills:
                # Query for the skill using conceptUri
                skill_result = (
                    self.client.query
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
                    logger.warning(f"Skill {skill_uri} not found")
                    continue
                    
                skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
                
                self.client.data_object.reference.add(
                    from_uuid=occupation_id,
                    from_class_name="Occupation",
                    from_property_name="hasEssentialSkill",
                    to_uuid=skill_id,
                    to_class_name="Skill"
                )

            # Add optional skills
            for skill_uri in optional_skills:
                # Query for the skill using conceptUri
                skill_result = (
                    self.client.query
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
                    logger.warning(f"Skill {skill_uri} not found")
                    continue
                    
                skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
                
                self.client.data_object.reference.add(
                    from_uuid=occupation_id,
                    from_class_name="Occupation",
                    from_property_name="hasOptionalSkill",
                    to_uuid=skill_id,
                    to_class_name="Skill"
                )
        except Exception as e:
            logger.error(f"Failed to add relations for occupation {occupation_uri}: {str(e)}")

    def semantic_search(self, query_vector, limit: int = 10, certainty: float = 0.75) -> List[Dict]:
        """Perform semantic search using a query vector."""
        try:
            # Handle both numpy arrays and lists
            if isinstance(query_vector, np.ndarray):
                vector_list = query_vector.tolist()
            elif isinstance(query_vector, list):
                vector_list = query_vector
            else:
                raise ValueError(f"Unexpected query_vector type: {type(query_vector)}")
            
            result = (
                self.client.query
                .get("Occupation")
                .with_near_vector({
                    "vector": vector_list,
                    "certainty": certainty
                })
                .with_limit(limit)
                .with_additional(["certainty"])
                .do()
            )
            return result["data"]["Get"]["Occupation"]
        except Exception as e:
            logger.error(f"Semantic search failed: {str(e)}")
            return []

    def get_related_skills(self, occupation_uri: str) -> Dict[str, List[Dict]]:
        """Get essential and optional skills for an occupation."""
        try:
            result = (
                self.client.query
                .get("Occupation", ["conceptUri", "preferredLabel"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .with_near_vector({
                    "vector": [0] * 384,  # Dummy vector, we're not using it
                    "certainty": 0
                })
                .with_limit(1)
                .do()
            )

            if not result["data"]["Get"]["Occupation"]:
                return {"essential": [], "optional": []}

            occupation = result["data"]["Get"]["Occupation"][0]
            
            # Get essential skills
            essential_skills = (
                self.client.query
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
                self.client.query
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
            logger.error(f"Failed to get related skills for {occupation_uri}: {str(e)}")
            return {"essential": [], "optional": []}

    def add_hierarchical_relation(self, broader_uri: str, narrower_uri: str, relation_type: str):
        """Add hierarchical relation between broader and narrower concepts."""
        try:
            if relation_type == "Occupation":
                # Query for the broader occupation
                broader_result = (
                    self.client.query
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
                    logger.warning(f"Broader occupation {broader_uri} not found")
                    return
                    
                broader_id = broader_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]
                
                # Query for the narrower occupation
                narrower_result = (
                    self.client.query
                    .get("Occupation", ["conceptUri"])
                    .with_additional(["id"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "Equal",
                        "valueString": narrower_uri
                    })
                    .do()
                )
                
                if not narrower_result["data"]["Get"]["Occupation"]:
                    logger.warning(f"Narrower occupation {narrower_uri} not found")
                    return
                    
                narrower_id = narrower_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]
                
                # Add broader-narrower relation for occupations
                self.client.data_object.reference.add(
                    from_uuid=broader_id,
                    from_class_name="Occupation",
                    from_property_name="narrowerOccupation",
                    to_uuid=narrower_id,
                    to_class_name="Occupation"
                )
                self.client.data_object.reference.add(
                    from_uuid=narrower_id,
                    from_class_name="Occupation",
                    from_property_name="broaderOccupation",
                    to_uuid=broader_id,
                    to_class_name="Occupation"
                )
            elif relation_type == "Skill":
                # Query for the broader skill
                broader_result = (
                    self.client.query
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
                    logger.warning(f"Broader skill {broader_uri} not found")
                    return
                    
                broader_id = broader_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
                
                # Query for the narrower skill
                narrower_result = (
                    self.client.query
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
                    logger.warning(f"Narrower skill {narrower_uri} not found")
                    return
                    
                narrower_id = narrower_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
                
                # Add broader-narrower relation for skills
                self.client.data_object.reference.add(
                    from_uuid=broader_id,
                    from_class_name="Skill",
                    from_property_name="narrowerSkill",
                    to_uuid=narrower_id,
                    to_class_name="Skill"
                )
                self.client.data_object.reference.add(
                    from_uuid=narrower_id,
                    from_class_name="Skill",
                    from_property_name="broaderSkill",
                    to_uuid=broader_id,
                    to_class_name="Skill"
                )
        except Exception as e:
            logger.error(f"Failed to add hierarchical relation: {str(e)}")
            raise

    def batch_import_isco_groups(self, groups: List[Dict], vectors: List):
        """Import ISCO groups in batches."""
        with self.client.batch as batch:
            batch.batch_size = self.config['batch_size']
            for group, vector in zip(groups, vectors):
                try:
                    # Handle both numpy arrays and lists
                    if isinstance(vector, np.ndarray):
                        vector_list = vector.tolist()
                    elif isinstance(vector, list):
                        vector_list = vector
                    else:
                        raise ValueError(f"Unexpected vector type: {type(vector)}")
                    
                    batch.add_data_object(
                        data_object={
                            "conceptUri": group["conceptUri"],
                            "code": group.get("code", ""),
                            "preferredLabel_en": group["preferredLabel_en"],
                            "description_en": group.get("description_en", "")
                        },
                        class_name="ISCOGroup",
                        vector=vector_list
                    )
                except Exception as e:
                    logger.error(f"Failed to import ISCO group {group['conceptUri']}: {str(e)}")

    def batch_import_skill_collections(self, collections: List[Dict], vectors: List):
        """Import skill collections in batches."""
        with self.client.batch as batch:
            batch.batch_size = self.config['batch_size']
            for collection, vector in zip(collections, vectors):
                try:
                    # Handle both numpy arrays and lists
                    if isinstance(vector, np.ndarray):
                        vector_list = vector.tolist()
                    elif isinstance(vector, list):
                        vector_list = vector
                    else:
                        raise ValueError(f"Unexpected vector type: {type(vector)}")
                    
                    batch.add_data_object(
                        data_object={
                            "conceptUri": collection["conceptUri"],
                            "preferredLabel_en": collection["preferredLabel_en"],
                            "description_en": collection.get("description_en", "")
                        },
                        class_name="SkillCollection",
                        vector=vector_list
                    )
                except Exception as e:
                    logger.error(f"Failed to import skill collection {collection['conceptUri']}: {str(e)}")

    def add_skill_collection_relation(self, collection_uri: str, skill_uri: str):
        """Add relation between skill collection and skill."""
        try:
            self.client.data_object.reference.add(
                from_uuid=collection_uri,
                from_class_name="SkillCollection",
                from_property_name="hasSkill",
                to_uuid=skill_uri,
                to_class_name="Skill"
            )
        except Exception as e:
            logger.error(f"Failed to add skill collection relation: {str(e)}")
            raise 