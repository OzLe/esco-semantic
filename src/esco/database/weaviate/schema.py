import yaml
from pathlib import Path
from typing import Dict, Any, List
import logging
from ...core.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages Weaviate schema definitions and operations"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.schema_dir = Path(__file__).parent.parent.parent.parent.parent / 'config' / 'schemas'
    
    def get_base_schemas(self) -> Dict[str, Any]:
        """Get base schema definitions"""
        schemas = {}
        for schema_file in self.schema_dir.glob('*.yaml'):
            with open(schema_file, 'r') as f:
                schema = yaml.safe_load(f)
                schemas[schema['class']] = schema
        return schemas
    
    def load_references(self) -> Dict[str, List[Dict[str, Any]]]:
        """Load reference definitions between classes"""
        references = {}
        for schema_file in self.schema_dir.glob('*.yaml'):
            with open(schema_file, 'r') as f:
                schema = yaml.safe_load(f)
                if 'references' in schema:
                    references[schema['class']] = schema['references']
        return references
    
    def property_exists(self, client: Any, class_name: str, property_name: str) -> bool:
        """Check if property exists in class"""
        try:
            schema = client.schema.get(class_name)
            return any(p['name'] == property_name for p in schema['properties'])
        except Exception:
            return False
    
    def create_class_schema(
        self,
        class_name: str,
        properties: List[Dict[str, Any]],
        vectorizer: str = "text2vec-transformers"
    ) -> Dict[str, Any]:
        """Create class schema definition"""
        return {
            "class": class_name,
            "vectorizer": vectorizer,
            "moduleConfig": {
                "text2vec-transformers": {
                    "vectorizeClassName": True
                }
            },
            "properties": properties
        }
    
    def create_property(
        self,
        name: str,
        data_type: str,
        description: str = "",
        index: bool = True,
        tokenization: str = "word"
    ) -> Dict[str, Any]:
        """Create property definition"""
        return {
            "name": name,
            "dataType": [data_type],
            "description": description,
            "indexInverted": index,
            "tokenization": tokenization
        }
    
    def create_reference_property(
        self,
        name: str,
        target_class: str,
        description: str = ""
    ) -> Dict[str, Any]:
        """Create reference property definition"""
        return {
            "name": name,
            "dataType": [target_class],
            "description": description
        }
    
    def validate_schema(self, schema: Dict[str, Any]) -> bool:
        """Validate schema definition"""
        required_fields = ['class', 'vectorizer', 'properties']
        if not all(field in schema for field in required_fields):
            return False
        
        for prop in schema['properties']:
            if not all(field in prop for field in ['name', 'dataType']):
                return False
        
        return True

    def load_schema_file(self, schema_name: str) -> Dict[str, Any]:
        """Load a schema file from the resources directory."""
        schema_path = self.schema_dir / f"{schema_name}.yaml"
        try:
            with open(schema_path, 'r') as f:
                schema = yaml.safe_load(f)
            # Replace vector_index_config placeholder with actual config
            if isinstance(schema.get('vectorIndexConfig'), str) and schema['vectorIndexConfig'] == '${vector_index_config}':
                schema['vectorIndexConfig'] = self.config['vector_index_config']
            return schema
        except FileNotFoundError:
            raise DatabaseError(f"Schema file not found: {schema_path}")
        except Exception as e:
            raise DatabaseError(f"Failed to load schema file {schema_name}: {str(e)}") 