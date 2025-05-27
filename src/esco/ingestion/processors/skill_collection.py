from typing import Optional, Dict, Any
import pandas as pd
from ..base import BaseIngestor
from ...models.skill_collection import SkillCollection
from ...core.exceptions import ValidationError

class SkillCollectionIngestor(BaseIngestor):
    """Ingestor for SkillCollection entities"""
    
    def get_entity_type(self) -> str:
        return "SkillCollection"
    
    def validate_csv(self, df: pd.DataFrame) -> bool:
        required_columns = {'conceptUri', 'preferredLabel', 'collectionType'}
        return required_columns.issubset(df.columns)
    
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
            return None
        return {
            'conceptUri': row['conceptUri'].split('/')[-1],
            'preferredLabel_en': row['preferredLabel'],
            'description_en': row.get('description', ''),
            'altLabels_en': row.get('altLabels', '').split('|') if row.get('altLabels') else [],
            'collectionType': row.get('collectionType', ''),
            'skills': row.get('skills', '').split('|') if row.get('skills') else [],
            'parentCollection': row.get('parentCollection', ''),
            'childCollections': row.get('childCollections', '').split('|') if row.get('childCollections') else []
        }
    
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        parts = [
            entity.get('preferredLabel_en', ''),
            ' '.join(entity.get('altLabels_en', [])),
            entity.get('description_en', ''),
            f"Collection Type: {entity.get('collectionType', '')}"
        ]
        return '. '.join(filter(None, parts))
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        if not self.validate_csv(df):
            raise ValidationError("CSV missing required columns")
        empty_uris = df[df['conceptUri'].isna() | (df['conceptUri'] == '')]
        if not empty_uris.empty:
            raise ValidationError(f"Found {len(empty_uris)} rows with empty conceptUri")
        empty_labels = df[df['preferredLabel'].isna() | (df['preferredLabel'] == '')]
        if not empty_labels.empty:
            raise ValidationError(f"Found {len(empty_labels)} rows with empty preferredLabel")
        empty_types = df[df['collectionType'].isna() | (df['collectionType'] == '')]
        if not empty_types.empty:
            raise ValidationError(f"Found {len(empty_types)} rows with empty collectionType")
        return True 