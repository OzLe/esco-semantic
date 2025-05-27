from typing import List, Dict, Any, Optional
import pandas as pd
import logging
from .base import BaseProcessor
from ...core.exceptions import IngestionError, ValidationError
from ...core.config import Config
from ...models.skill import Skill
from ...database.weaviate.client import WeaviateClient
from ..base import BaseIngestor

logger = logging.getLogger(__name__)

class SkillProcessor(BaseProcessor):
    """Processor for ESCO skill data"""
    
    def __init__(self, config: Config, client: WeaviateClient):
        super().__init__(config)
        self.client = client
    
    def process(self, data: pd.DataFrame) -> None:
        """Process a batch of skill data"""
        try:
            if not self.validate(data):
                raise IngestionError("Data validation failed")
            
            transformed_data = self.transform(data)
            self.ingest(transformed_data)
        except Exception as e:
            raise IngestionError(f"Failed to process skill data: {str(e)}")
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate skill data"""
        required_columns = ['conceptUri', 'preferredLabel_en', 'skillType']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            raise IngestionError(f"Missing required columns: {missing_columns}")
        
        # Check for empty required fields
        for col in required_columns:
            if data[col].isna().any():
                raise IngestionError(f"Found empty values in required column: {col}")
        
        return True
    
    def transform(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform skill data for ingestion"""
        transformed = []
        
        for _, row in data.iterrows():
            skill = Skill(
                uri=row['conceptUri'],
                label=row['preferredLabel_en'],
                description=row.get('description_en'),
                skill_type=row['skillType'],
                alt_labels=row.get('altLabels_en', []),
                skill_groups=[],  # Will be populated later
                related_skills=[]  # Will be populated later
            )
            
            transformed.append(skill.to_dict())
        
        return transformed
    
    def ingest(self, data: List[Dict[str, Any]]) -> None:
        """Ingest transformed skill data"""
        try:
            self.client.insert('Skill', data)
        except Exception as e:
            raise IngestionError(f"Failed to ingest skill data: {str(e)}")
    
    def add_skill_group_relation(self, skill_uri: str, group_uri: str) -> None:
        """Add skill group relation to a skill"""
        try:
            self.client.add_relation(
                from_class='Skill',
                to_class='SkillGroup',
                from_uri=skill_uri,
                to_uri=group_uri,
                relation_type='belongsToSkillGroup'
            )
        except Exception as e:
            raise IngestionError(f"Failed to add skill group relation: {str(e)}")
    
    def add_related_skill_relation(self, from_skill_uri: str, to_skill_uri: str, relation_type: str) -> None:
        """Add relation between two skills"""
        try:
            self.client.add_relation(
                from_class='Skill',
                to_class='Skill',
                from_uri=from_skill_uri,
                to_uri=to_skill_uri,
                relation_type=relation_type
            )
        except Exception as e:
            raise IngestionError(f"Failed to add related skill relation: {str(e)}")
    
    def add_skill_collection_relation(self, skill_uri: str, collection_uri: str) -> None:
        """Add skill collection relation to a skill"""
        try:
            self.client.add_relation(
                from_class='Skill',
                to_class='SkillCollection',
                from_uri=skill_uri,
                to_uri=collection_uri,
                relation_type='belongsToSkillCollection'
            )
        except Exception as e:
            raise IngestionError(f"Failed to add skill collection relation: {str(e)}")

class SkillIngestor(BaseIngestor):
    """Ingestor for Skill entities"""
    
    def get_entity_type(self) -> str:
        return "Skill"
    
    def validate_csv(self, df: pd.DataFrame) -> bool:
        """Validate CSV has required columns"""
        required_columns = {'conceptUri', 'preferredLabel'}
        return required_columns.issubset(df.columns)
    
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        """Transform a CSV row to Skill entity"""
        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
            return None
        
        return {
            'conceptUri': row['conceptUri'].split('/')[-1],
            'preferredLabel_en': row['preferredLabel'],
            'description_en': row.get('description', ''),
            'definition_en': row.get('definition', ''),
            'skillType': row.get('skillType', ''),
            'reuseLevel': row.get('reuseLevel', ''),
            'altLabels_en': row.get('altLabels', '').split('|') if row.get('altLabels') else []
        }
    
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        """Get text representation for embedding generation"""
        parts = [
            entity.get('preferredLabel_en', ''),
            ' '.join(entity.get('altLabels_en', [])),
            entity.get('description_en', ''),
            entity.get('definition_en', ''),
            entity.get('skillType', ''),
            entity.get('reuseLevel', '')
        ]
        return '. '.join(filter(None, parts))
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate data before ingestion"""
        if not self.validate_csv(df):
            raise ValidationError("CSV missing required columns")
        
        # Check for empty required fields
        empty_uris = df[df['conceptUri'].isna() | (df['conceptUri'] == '')]
        if not empty_uris.empty:
            raise ValidationError(f"Found {len(empty_uris)} rows with empty conceptUri")
        
        empty_labels = df[df['preferredLabel'].isna() | (df['preferredLabel'] == '')]
        if not empty_labels.empty:
            raise ValidationError(f"Found {len(empty_labels)} rows with empty preferredLabel")
        
        return True 