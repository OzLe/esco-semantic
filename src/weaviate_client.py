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
        # Define Occupation collection
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
                    "name": "preferredLabel",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "description",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                }
            ]
        }

        # Define Skill collection
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
                    "name": "preferredLabel",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                },
                {
                    "name": "description",
                    "dataType": ["text"],
                    "isIndexed": True,
                    "tokenization": "word"
                }
            ]
        }

        # Create collections if they don't exist
        try:
            if not self.client.schema.exists("Occupation"):
                self.client.schema.create_class(occupation_collection)
            if not self.client.schema.exists("Skill"):
                self.client.schema.create_class(skill_collection)
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise

    def batch_import_occupations(self, occupations: List[Dict], vectors: List[np.ndarray]):
        """Import occupations in batches."""
        with self.client.batch as batch:
            batch.batch_size = self.config['batch_size']
            for occupation, vector in zip(occupations, vectors):
                try:
                    batch.add_data_object(
                        data_object={
                            "conceptUri": occupation["conceptUri"],
                            "preferredLabel": occupation["preferredLabel"],
                            "description": occupation.get("description", "")
                        },
                        class_name="Occupation",
                        vector=vector.tolist()
                    )
                except Exception as e:
                    logger.error(f"Failed to import occupation {occupation['conceptUri']}: {str(e)}")

    def batch_import_skills(self, skills: List[Dict], vectors: List[np.ndarray]):
        """Import skills in batches."""
        with self.client.batch as batch:
            batch.batch_size = self.config['batch_size']
            for skill, vector in zip(skills, vectors):
                try:
                    batch.add_data_object(
                        data_object={
                            "conceptUri": skill["conceptUri"],
                            "preferredLabel": skill["preferredLabel"],
                            "description": skill.get("description", "")
                        },
                        class_name="Skill",
                        vector=vector.tolist()
                    )
                except Exception as e:
                    logger.error(f"Failed to import skill {skill['conceptUri']}: {str(e)}")

    def add_skill_relations(self, occupation_uri: str, essential_skills: List[str], optional_skills: List[str]):
        """Add skill relations to an occupation."""
        try:
            occupation = self.client.data_object.get_by_id(
                occupation_uri,
                class_name="Occupation"
            )
            
            if not occupation:
                logger.warning(f"Occupation {occupation_uri} not found")
                return

            # Add essential skills
            for skill_uri in essential_skills:
                self.client.data_object.reference.add(
                    from_uuid=occupation_uri,
                    from_class_name="Occupation",
                    from_property_name="hasEssentialSkill",
                    to_uuid=skill_uri,
                    to_class_name="Skill"
                )

            # Add optional skills
            for skill_uri in optional_skills:
                self.client.data_object.reference.add(
                    from_uuid=occupation_uri,
                    from_class_name="Occupation",
                    from_property_name="hasOptionalSkill",
                    to_uuid=skill_uri,
                    to_class_name="Skill"
                )
        except Exception as e:
            logger.error(f"Failed to add relations for occupation {occupation_uri}: {str(e)}")

    def semantic_search(self, query_vector: np.ndarray, limit: int = 10, certainty: float = 0.75) -> List[Dict]:
        """Perform semantic search using a query vector."""
        try:
            result = (
                self.client.query
                .get("Occupation")
                .with_near_vector({
                    "vector": query_vector.tolist(),
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