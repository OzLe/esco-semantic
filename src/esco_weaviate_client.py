import weaviate
import yaml
import logging
from typing import Dict, List, Any
from pathlib import Path
from threading import Lock
from weaviate.exceptions import UnexpectedStatusCodeException
from .exceptions import WeaviateError, ConfigurationError
from .logging_config import log_error
from .repositories.repository_factory import RepositoryFactory
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class WeaviateClient:
    __instance = None
    __lock = Lock()
    __config_path = None
    __profile = None
    __schema_lock = Lock()  # New lock specifically for schema operations

    def __new__(cls, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        with cls.__lock:
            if cls.__instance is None:
                cls.__instance = super(WeaviateClient, cls).__new__(cls)
                # Store initial configuration
                cls.__config_path = config_path
                cls.__profile = profile
                # Initialize instance attributes
                cls.__instance.__initialized = False
                cls.__instance.__schema_initialized = False
                cls.__instance.client = None
                cls.__instance.config = None
            else:
                # Check if configuration matches
                if cls.__config_path != config_path or cls.__profile != profile:
                    logger.warning(
                        f"Attempting to create WeaviateClient with different configuration. "
                        f"Using existing instance with config_path='{cls.__config_path}' "
                        f"and profile='{cls.__profile}' instead."
                    )
            return cls.__instance

    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize Weaviate client with configuration."""
        with self.__lock:
            if self.__initialized:
                return
            
            try:
                self.config = self._load_config(config_path, profile)
                self.client = self._initialize_client()
                self.__initialized = True
            except Exception as e:
                log_error(logger, e, {'config_path': config_path, 'profile': profile})
                # Reset instance on initialization failure
                self.__class__.__instance = None
                raise WeaviateError(f"Failed to initialize Weaviate client: {str(e)}")

    @classmethod
    def get_instance(cls, config_path: str = "config/weaviate_config.yaml", profile: str = "default") -> 'WeaviateClient':
        """Factory method to get or create a WeaviateClient instance."""
        return cls(config_path, profile)

    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance. Mainly useful for testing."""
        with cls.__lock:
            if cls.__instance is not None:
                cls.__instance.close()
            cls.__instance = None
            cls.__config_path = None
            cls.__profile = None

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
            url = weaviate_config['url']
            
            # Create client with the new API
            return weaviate.Client(
                url=url,
                timeout_config=(5, 60)  # (connect timeout, read timeout)
            )
        except Exception as e:
            raise WeaviateError(f"Failed to initialize Weaviate client: {str(e)}")

    def _load_schema_file(self, schema_name: str) -> Dict:
        """Load a schema file from the resources directory."""
        # Get the absolute path to the project root directory
        project_root = Path(__file__).parent.parent
        schema_path = project_root / "resources" / "schemas" / f"{schema_name}.yaml"
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

    def is_schema_initialized(self) -> bool:
        """Check if schema is already initialized."""
        return self.__schema_initialized

    def _ensure_schema(self):
        """Ensure the required schema exists in Weaviate."""
        if self.__schema_initialized:  # Quick check without lock
            logger.debug("Schema already initialized, skipping...")
            return

        with self.__schema_lock:  # Use schema-specific lock
            if self.__schema_initialized:  # Double-check after acquiring lock
                logger.debug("Schema already initialized (after lock), skipping...")
                return

            try:
                # Load and create base schemas
                schemas = {
                    "ISCOGroup": self._load_schema_file("isco_group"),
                    "Skill": self._load_schema_file("skill"),
                    "SkillCollection": self._load_schema_file("skill_collection"),
                    "Occupation": self._load_schema_file("occupation"),
                    "SkillGroup": self._load_schema_file("skill_group"),
                    "Metadata": self._load_schema_file("metadata")  # Add Metadata class
                }

                # Create base collections first
                for class_name, schema in schemas.items():
                    try:
                        if not self.client.schema.exists(class_name):
                            logger.info(f"Creating schema class: {class_name}")
                            self.client.schema.create_class(schema)
                        else:
                            logger.debug(f"Schema class {class_name} already exists")
                    except UnexpectedStatusCodeException as e:
                        if "already exists" in str(e).lower():
                            logger.debug(f"Schema class {class_name} already exists (caught exception)")
                        else:
                            raise WeaviateError(f"Failed to create schema class {class_name}: {str(e)}")

                # Add reference properties after all classes exist
                self._add_reference_properties()
                self.__schema_initialized = True  # Mark schema as initialized
                logger.info("Schema initialization completed successfully")
                    
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
                        logger.debug(f"Property {ref['name']} already exists in class {class_name}, skipping...")
                        continue
                        
                    try:
                        self.client.schema.property.create(
                            class_name,
                            ref
                        )
                        logger.info(f"Successfully added property {ref['name']} to class {class_name}")
                    except UnexpectedStatusCodeException as e:
                        if "already exists" in str(e).lower():
                            logger.debug(f"Property {ref['name']} already exists in class {class_name}, skipping...")
                        else:
                            raise WeaviateError(f"Failed to add property {ref['name']} to class {class_name}: {str(e)}")
        except Exception as e:
            log_error(logger, e, {'operation': 'add_reference_properties'})
            raise WeaviateError(f"Failed to add reference properties: {str(e)}")

    def reset_schema(self):
        """Reset schema initialization state and delete all schema classes."""
        with self.__schema_lock:
            try:
                # Get all existing classes
                schema = self.client.schema.get()
                classes = [cls["class"] for cls in schema.get("classes", [])]
                
                # Delete each class
                for class_name in classes:
                    try:
                        self.client.schema.delete_class(class_name)
                        logger.info(f"Deleted schema class: {class_name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete class {class_name}: {str(e)}")
                
                # Reset initialization flag
                self.__schema_initialized = False
                logger.info("Schema reset completed")
            except Exception as e:
                log_error(logger, e, {'operation': 'reset_schema'})
                raise WeaviateError(f"Failed to reset schema: {str(e)}")

    def ensure_schema(self):
        """Public method to ensure schema exists. This is the recommended way to initialize schema."""
        if not self.__initialized:
            raise WeaviateError("Client not initialized. Call __init__ first.")
        self._ensure_schema()

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
        """Add a broader skill relation."""
        skill_repo = self.get_repository("Skill")
        return skill_repo.add_hierarchical_relation(broader_uri, skill_uri)

    def set_ingestion_metadata(self, status: str, details: dict = None):
        """Store ingestion metadata in Weaviate"""
        try:
            # Create a special metadata object
            metadata = {
                "metaType": "ingestion_status",
                "status": status,  # "in_progress", "completed", "failed"
                "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),  # RFC3339 format
                "version": "1.0",
                "details": json.dumps(details or {})
            }
            
            # Store in a special Metadata class
            self.client.data_object.create(
                class_name="Metadata",
                data_object=metadata
            )
            logger.info(f"Set ingestion status to: {status}")
        except Exception as e:
            logger.error(f"Failed to set ingestion metadata: {str(e)}")

    def get_ingestion_status(self) -> dict:
        """Check if ingestion was completed successfully"""
        try:
            result = (
                self.client.query
                .get("Metadata", ["metaType", "status", "timestamp", "details"])
                .with_where({
                    "path": ["metaType"],
                    "operator": "Equal",
                    "valueString": "ingestion_status"
                })
                .with_sort({"path": ["timestamp"], "order": "desc"})
                .with_limit(1)
                .do()
            )
            
            if result["data"]["Get"]["Metadata"]:
                status_data = result["data"]["Get"]["Metadata"][0]
                # Parse the JSON details field back to a dict
                if "details" in status_data and isinstance(status_data["details"], str):
                    try:
                        status_data["details"] = json.loads(status_data["details"])
                    except (json.JSONDecodeError, TypeError):
                        # If parsing fails, set details to empty dict
                        status_data["details"] = {}
                return status_data
            return {"status": "not_started"}
        except Exception as e:
            logger.error(f"Failed to get ingestion status: {str(e)}")
            return {"status": "unknown", "error": str(e)} 