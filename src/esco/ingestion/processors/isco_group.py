from typing import Optional, Dict, Any
import pandas as pd
from ..base import BaseIngestor
from ...models.isco_group import ISCOGroup
from ...core.exceptions import ValidationError

class ISCOGroupIngestor(BaseIngestor):
    """Ingestor for ISCOGroup entities"""
    
    def get_entity_type(self) -> str:
        return "ISCOGroup"
    
    def validate_csv(self, df: pd.DataFrame) -> bool:
        required_columns = {'conceptUri', 'preferredLabel', 'code', 'level'}
        return required_columns.issubset(df.columns)
    
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
            return None
        return {
            'conceptUri': row['conceptUri'].split('/')[-1],
            'code': row['code'],
            'preferredLabel_en': row['preferredLabel'],
            'description_en': row.get('description', ''),
            'altLabels_en': row.get('altLabels', '').split('|') if row.get('altLabels') else [],
            'level': int(row['level']) if row.get('level') else None,
            'parentCode': row.get('parentCode', ''),
            'childCodes': row.get('childCodes', '').split('|') if row.get('childCodes') else [],
            'occupations': row.get('occupations', '').split('|') if row.get('occupations') else []
        }
    
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        parts = [
            entity.get('preferredLabel_en', ''),
            ' '.join(entity.get('altLabels_en', [])),
            entity.get('description_en', ''),
            f"ISCO Group {entity.get('code', '')}",
            f"Level {entity.get('level', '')}"
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
        empty_codes = df[df['code'].isna() | (df['code'] == '')]
        if not empty_codes.empty:
            raise ValidationError(f"Found {len(empty_codes)} rows with empty code")
        return True 