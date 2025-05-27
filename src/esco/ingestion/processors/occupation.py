from typing import List, Dict, Any, Optional
import pandas as pd
import logging
from .base import BaseProcessor
from ...core.exceptions import IngestionError, ValidationError
from ...core.config import Config
from ...models.occupation import Occupation
from ...database.weaviate.client import WeaviateClient
from ..base import BaseIngestor

logger = logging.getLogger(__name__)

class OccupationProcessor(BaseProcessor):
    """Processor for ESCO occupation data"""
    
    def __init__(self, config: Config, client: WeaviateClient):
        super().__init__(config)
        self.client = client
    
    def process(self, data: pd.DataFrame) -> None:
        """Process a batch of occupation data"""
        try:
            if not self.validate(data):
                raise IngestionError("Data validation failed")
            
            transformed_data = self.transform(data)
            self.ingest(transformed_data)
        except Exception as e:
            raise IngestionError(f"Failed to process occupation data: {str(e)}")
    
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate occupation data"""
        required_columns = ['conceptUri', 'preferredLabel_en', 'iscoGroup']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            raise IngestionError(f"Missing required columns: {missing_columns}")
        
        # Check for empty required fields
        for col in required_columns:
            if data[col].isna().any():
                raise IngestionError(f"Found empty values in required column: {col}")
        
        return True
    
    def transform(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform occupation data for ingestion"""
        transformed = []
        
        for _, row in data.iterrows():
            occupation = Occupation(
                uri=row['conceptUri'],
                label=row['preferredLabel_en'],
                description=row.get('description_en'),
                isco_group=row['iscoGroup'],
                alt_labels=row.get('altLabels_en', []),
                skills=[],  # Will be populated later
                skill_groups=[]  # Will be populated later
            )
            
            transformed.append(occupation.to_dict())
        
        return transformed
    
    def ingest(self, data: List[Dict[str, Any]]) -> None:
        """Ingest transformed occupation data"""
        try:
            self.client.insert('Occupation', data)
        except Exception as e:
            raise IngestionError(f"Failed to ingest occupation data: {str(e)}")
    
    def add_skill_relations(self, occupation_uri: str, essential_skills: List[str], optional_skills: List[str]) -> None:
        """Add skill relations to an occupation"""
        try:
            # Add essential skills
            for skill_uri in essential_skills:
                self.client.add_relation(
                    from_class='Occupation',
                    to_class='Skill',
                    from_uri=occupation_uri,
                    to_uri=skill_uri,
                    relation_type='hasEssentialSkill'
                )
            
            # Add optional skills
            for skill_uri in optional_skills:
                self.client.add_relation(
                    from_class='Occupation',
                    to_class='Skill',
                    from_uri=occupation_uri,
                    to_uri=skill_uri,
                    relation_type='hasOptionalSkill'
                )
        except Exception as e:
            raise IngestionError(f"Failed to add skill relations: {str(e)}")
    
    def add_isco_group_relation(self, occupation_uri: str, isco_group_uri: str) -> None:
        """Add ISCO group relation to an occupation"""
        try:
            self.client.add_relation(
                from_class='Occupation',
                to_class='ISCOGroup',
                from_uri=occupation_uri,
                to_uri=isco_group_uri,
                relation_type='belongsToISCOGroup'
            )
        except Exception as e:
            raise IngestionError(f"Failed to add ISCO group relation: {str(e)}")

class OccupationIngestor(BaseIngestor):
    """Ingestor for Occupation entities"""
    
    def get_entity_type(self) -> str:
        return "Occupation"
    
    def validate_csv(self, df: pd.DataFrame) -> bool:
        """Validate CSV has required columns"""
        required_columns = {'conceptUri', 'preferredLabel', 'code'}
        return required_columns.issubset(df.columns)
    
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        """Transform a CSV row to Occupation entity"""
        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
            return None
        
        return {
            'conceptUri': row['conceptUri'].split('/')[-1],
            'code': row['code'],
            'preferredLabel_en': row['preferredLabel'],
            'description_en': row.get('description', ''),
            'definition_en': row.get('definition', ''),
            'iscoGroup': row.get('iscoGroup', ''),
            'altLabels_en': row.get('altLabels', '').split('|') if row.get('altLabels') else [],
            'essentialSkills': row.get('essentialSkills', '').split('|') if row.get('essentialSkills') else [],
            'optionalSkills': row.get('optionalSkills', '').split('|') if row.get('optionalSkills') else []
        }
    
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        """Get text representation for embedding generation"""
        parts = [
            entity.get('preferredLabel_en', ''),
            ' '.join(entity.get('altLabels_en', [])),
            entity.get('description_en', ''),
            entity.get('definition_en', ''),
            entity.get('code', ''),
            entity.get('iscoGroup', '')
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
        
        empty_codes = df[df['code'].isna() | (df['code'] == '')]
        if not empty_codes.empty:
            raise ValidationError(f"Found {len(empty_codes)} rows with empty code")
        
        return True 