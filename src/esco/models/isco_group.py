from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from .base import BaseEntity
from ..core.exceptions import ValidationError

@dataclass
class ISCOGroup(BaseEntity):
    """ISCO Group entity model"""
    code: Optional[str] = None
    level: Optional[int] = None
    parent_code: Optional[str] = None
    child_codes: List[str] = field(default_factory=list)
    occupations: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert ISCO group to dictionary representation"""
        return {
            'conceptUri': self.uri,
            'code': self.code,
            'preferredLabel_en': self.preferred_label,
            'description_en': self.description,
            'altLabels_en': self.alt_labels,
            'level': self.level,
            'parentCode': self.parent_code,
            'childCodes': self.child_codes,
            'occupations': self.occupations,
            'metadata': self.metadata
        }
    
    def validate(self) -> bool:
        """Validate ISCO group data"""
        if not self.uri:
            raise ValidationError("ISCO Group URI is required")
        if not self.preferred_label:
            raise ValidationError("ISCO Group preferred label is required")
        if not self.code:
            raise ValidationError("ISCO Group code is required")
        if self.level is not None and not (1 <= self.level <= 4):
            raise ValidationError("ISCO Group level must be between 1 and 4")
        return True
    
    def add_child_code(self, code: str) -> None:
        """Add child ISCO group code"""
        if code and code not in self.child_codes:
            self.child_codes.append(code)
    
    def add_occupation(self, occupation_uri: str) -> None:
        """Add occupation to this ISCO group"""
        if occupation_uri and occupation_uri not in self.occupations:
            self.occupations.append(occupation_uri)
    
    def get_text_for_embedding(self) -> str:
        """Get text representation for embedding generation"""
        parts = [
            self.preferred_label,
            ' '.join(self.alt_labels),
            self.description or '',
            f"ISCO Group {self.code}",
            f"Level {self.level}" if self.level else ''
        ]
        return '. '.join(filter(None, parts))
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ISCOGroup':
        """Create ISCOGroup instance from dictionary"""
        return cls(
            uri=data['conceptUri'],
            code=data.get('code'),
            preferred_label=data.get('preferredLabel_en', ''),
            description=data.get('description_en'),
            alt_labels=data.get('altLabels_en', []),
            level=data.get('level'),
            parent_code=data.get('parentCode'),
            child_codes=data.get('childCodes', []),
            occupations=data.get('occupations', []),
            metadata=data.get('metadata', {})
        ) 