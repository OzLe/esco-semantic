import weaviate
import yaml
import logging
from typing import Dict, List, Optional, Any, TYPE_CHECKING
import numpy as np
from pathlib import Path
import os
from weaviate.exceptions import UnexpectedStatusCodeException
from .exceptions import WeaviateError, ConfigurationError
from .logging_config import log_error
from .repositories.repository_factory import RepositoryFactory

logger = logging.getLogger(__name__)

class WeaviateClient:
    _instance = None
    _initialized = False

    def __new__(cls, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        if cls._instance is None:
            cls._instance = super(WeaviateClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize Weaviate client with configuration."""
        if self._initialized:
            return

        if not config_path:
            config_path = "config/weaviate_config.yaml"
        try:
            self.config = self._load_config(config_path, profile)
            self.client = self._initialize_client()
            self._schema_initialized = False  # Add flag to track schema initialization
            self._ensure_schema()
            self._initialized = True
        except Exception as e:
            log_error(logger, e, {'config_path': config_path, 'profile': profile})
            raise WeaviateError(f"Failed to initialize Weaviate client: {str(e)}")

    def _load_config(self, config_path: str, profile: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            if profile not in config:
                raise ConfigurationError(f"Profile '{profile}' not found in config file")
            return config[profile]
        except FileNotFoundError as e:
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        except Exception as e:
            raise ConfigurationError(f"Failed to load config file: {str(e)}")

    def _initialize_client(self) -> weaviate.Client:
        """Initialize Weaviate client with configuration."""
        try:
            weaviate_config = self.config.get('weaviate', {})
            return weaviate.Client(
                url=weaviate_config['url'],
                additional_headers={},
                timeout_config=(5, 60)  # (connect timeout, read timeout)
            )
        except Exception as e:
            raise WeaviateError(f"Failed to initialize Weaviate client: {str(e)}")

    def _load_schema_file(self, schema_name: str) -> Dict:
        """Load a schema file from the resources directory."""
        schema_path = Path("resources/schemas") / f"{schema_name}.yaml"
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
            # Replace vector_index_config placeholder with actual config
            if isinstance(schema.get('vectorIndexConfig'), str) and schema['vectorIndexConfig'] == '${vector_index_config}':
                schema['vectorIndexConfig'] = self.config['weaviate']['vector_index_config']
            return schema
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        except Exception as e:
            raise ValueError(f"Failed to load schema file {schema_name}: {str(e)}")

    def _load_references(self) -> Dict:
        """Load reference properties from the references file."""
        return self._load_schema_file("references")

    def _ensure_schema(self):
        """Ensure the required schema exists in Weaviate."""
        if self._schema_initialized:  # Skip if already initialized
            logger.info("Schema already initialized, skipping...")
            return

        try:
            # Load and create base schemas
            schemas = {
                "ISCOGroup": self._load_schema_file("isco_group"),
                "Skill": self._load_schema_file("skill"),
                "SkillCollection": self._load_schema_file("skill_collection"),
                "Occupation": self._load_schema_file("occupation"),
                "SkillGroup": self._load_schema_file("skill_group")
            }

            # Create base collections first
            for class_name, schema in schemas.items():
                if not self.client.schema.exists(class_name):
                    logger.info(f"Creating schema class: {class_name}")
                    try:
                        self.client.schema.create_class(schema)
                    except UnexpectedStatusCodeException as e:
                        # Check if the error is due to the class already existing
                        if "already exists" in str(e).lower():
                            logger.warning(f"Schema class {class_name} already exists, but 'exists' check failed. Continuing.")
                        else:
                            raise WeaviateError(f"Failed to create schema class {class_name}: {str(e)}")
                else:
                    logger.info(f"Schema class {class_name} already exists")

            # Add reference properties after all classes exist
            self._add_reference_properties()
            self._schema_initialized = True  # Mark schema as initialized
                
        except Exception as e:
            log_error(logger, e, {'operation': 'ensure_schema'})
            raise WeaviateError(f"Failed to create schema: {str(e)}")

    def _add_reference_properties(self):
        """Add reference properties after all classes are created."""
        try:
            references = self._load_references()
            
            for class_name, refs in references.items():
                # Get existing properties for the class
                existing_props = self.client.schema.get(class_name).get("properties", [])
                existing_prop_names = {prop.get("name") for prop in existing_props}
                
                for ref in refs:
                    # Skip if property already exists
                    if ref["name"] in existing_prop_names:
                        logger.info(f"Property {ref['name']} already exists in class {class_name}, skipping...")
                        continue
                        
                    try:
                        self.client.schema.property.create(
                            class_name,
                            ref
                        )
                        logger.info(f"Successfully added property {ref['name']} to class {class_name}")
                    except UnexpectedStatusCodeException as e:
                        if "already exists" in str(e).lower():
                            logger.warning(f"Property {ref['name']} already exists in class {class_name}, skipping...")
                        else:
                            raise WeaviateError(f"Failed to add property {ref['name']} to class {class_name}: {str(e)}")
        except Exception as e:
            log_error(logger, e, {'operation': 'add_reference_properties'})
            raise WeaviateError(f"Failed to add reference properties: {str(e)}")

    def _property_exists(self, class_name: str, property_name: str) -> bool:
        """Check if a property exists in a class."""
        try:
            schema = self.client.schema.get(class_name)
            return any(prop.get("name") == property_name for prop in schema.get("properties", []))
        except Exception as e:
            log_error(logger, e, {
                'operation': 'property_exists',
                'class_name': class_name,
                'property_name': property_name
            })
            return False

    def get_repository(self, repository_type: str):
        """Get a repository instance for the specified type."""
        return RepositoryFactory.get_repository(self, repository_type)

    def close(self):
        """Close the Weaviate client and clear repositories."""
        RepositoryFactory.clear_repositories()
        # Weaviate client doesn't need explicit closing
        pass

    def check_object_exists(self, class_name: str, object_uri: str) -> bool:
        """Check if an object exists by its URI."""
        try:
            result = (
                self.client.query
                .get(class_name, ["conceptUri"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": object_uri
                })
                .do()
            )
            return len(result["data"]["Get"][class_name]) > 0
        except Exception as e:
            logger.error(f"Error checking existence of {class_name} {object_uri}: {str(e)}")
            return False

    def batch_import_skill_groups(self, data_list: List[Dict[str, Any]], vectors: List[List[float]]):
        """Import skill groups using the repository."""
        repo = self.get_repository("SkillGroup")
        return repo.batch_import(data_list, vectors)

    def batch_import_skill_collections(self, data_list: List[Dict[str, Any]], vectors: List[List[float]]):
        """Import skill collections using the repository."""
        repo = self.get_repository("SkillCollection")
        return repo.batch_import(data_list, vectors)

    def add_occupation_group_relation(self, occupation_uri: str, group_uri: str) -> bool:
        """Add relation between occupation and ISCO group."""
        try:
            # Get occupation ID
            occ_result = (
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
            
            if not occ_result["data"]["Get"]["Occupation"]:
                return False
                
            occ_id = occ_result["data"]["Get"]["Occupation"][0]["_additional"]["id"]
            
            # Get ISCO group ID
            group_result = (
                self.client.query
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
            
            # Add relation
            self.client.data_object.reference.add(
                from_uuid=occ_id,
                from_class_name="Occupation",
                from_property_name="memberOfISCOGroup",
                to_uuid=group_id,
                to_class_name="ISCOGroup"
            )
            
            return True
        except Exception as e:
            logger.error(f"Failed to add occupation-group relation: {str(e)}")
            return False

    def add_skill_collection_relation(self, collection_uri: str, skill_uri: str) -> bool:
        """Add relation between skill collection and skill."""
        try:
            # Get collection ID
            coll_result = (
                self.client.query
                .get("SkillCollection", ["conceptUri"])
                .with_additional(["id"])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": collection_uri
                })
                .do()
            )
            
            if not coll_result["data"]["Get"]["SkillCollection"]:
                return False
                
            coll_id = coll_result["data"]["Get"]["SkillCollection"][0]["_additional"]["id"]
            
            # Get skill ID
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
                return False
                
            skill_id = skill_result["data"]["Get"]["Skill"][0]["_additional"]["id"]
            
            # Add relation
            self.client.data_object.reference.add(
                from_uuid=coll_id,
                from_class_name="SkillCollection",
                from_property_name="hasSkill",
                to_uuid=skill_id,
                to_class_name="Skill"
            )
            
            return True
        except Exception as e:
            logger.error(f"Failed to add skill-collection relation: {str(e)}")
            return False

    def add_skill_to_skill_relation(self, from_skill_uri: str, to_skill_uri: str, relation_type: str) -> bool:
        """Add skill-to-skill relation."""
        skill_repo = self.get_repository("Skill")
        return skill_repo.add_skill_to_skill_relation(from_skill_uri, to_skill_uri, relation_type)

    def add_broader_skill_relation(self, skill_uri: str, broader_uri: str) -> bool:
        """Add broader skill relation."""
        skill_repo = self.get_repository("Skill")
        return skill_repo.add_hierarchical_relation(broader_uri, skill_uri) 