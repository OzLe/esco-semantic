import weaviate
import yaml
import logging
from typing import Dict, List, Optional, Any
import numpy as np
from pathlib import Path
import os
from weaviate.exceptions import UnexpectedStatusCodeException
from .exceptions import WeaviateError, ConfigurationError
from .logging_config import log_error
from .repositories.repository_factory import RepositoryFactory

logger = logging.getLogger(__name__)

class WeaviateClient:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize Weaviate client with configuration."""
        if not config_path:
            config_path = "config/weaviate_config.yaml"
        try:
            self.config = self._load_config(config_path, profile)
            self.client = self._initialize_client()
            self._ensure_schema()
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
                
        except Exception as e:
            log_error(logger, e, {'operation': 'ensure_schema'})
            raise WeaviateError(f"Failed to create schema: {str(e)}")

    def _add_reference_properties(self):
        """Add reference properties after all classes are created."""
        try:
            references = self._load_references()
            
            for class_name, refs in references.items():
                for ref in refs:
                    if not self._property_exists(class_name, ref["name"]):
                        self.client.schema.property.create(
                            class_name,
                            ref
                        )
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