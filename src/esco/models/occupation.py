from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from .base import BaseEntity
from ..core.exceptions import ValidationError

@dataclass
class Occupation(BaseEntity):
    """Occupation entity model"""
    code: Optional[str] = None
    isco_group: Optional[str] = None
    definition: Optional[str] = None
    essential_skills: List[str] = field(default_factory=list)
    optional_skills: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert occupation to dictionary representation"""
        return {
            'conceptUri': self.uri,
            'code': self.code,
            'preferredLabel_en': self.preferred_label,
            'description_en': self.description,
            'altLabels_en': self.alt_labels,
            'definition_en': self.definition,
            'iscoGroup': self.isco_group,
            'essentialSkills': self.essential_skills,
            'optionalSkills': self.optional_skills,
            'metadata': self.metadata
        }
    
    def validate(self) -> bool:
        """Validate occupation data"""
        if not self.uri:
            raise ValidationError("Occupation URI is required")
        if not self.preferred_label:
            raise ValidationError("Occupation preferred label is required")
        if not self.code:
            raise ValidationError("Occupation code is required")
        return True
    
    def add_essential_skill(self, skill_uri: str) -> None:
        """Add essential skill"""
        if skill_uri and skill_uri not in self.essential_skills:
            self.essential_skills.append(skill_uri)
    
    def add_optional_skill(self, skill_uri: str) -> None:
        """Add optional skill"""
        if skill_uri and skill_uri not in self.optional_skills:
            self.optional_skills.append(skill_uri)
    
    def get_text_for_embedding(self) -> str:
        """Get text representation for embedding generation"""
        parts = [
            self.preferred_label,
            ' '.join(self.alt_labels),
            self.description or '',
            self.definition or '',
            self.code or '',
            self.isco_group or ''
        ]
        return '. '.join(filter(None, parts))
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Occupation':
        """Create Occupation instance from dictionary"""
        return cls(
            uri=data['conceptUri'],
            code=data.get('code'),
            preferred_label=data.get('preferredLabel_en', ''),
            description=data.get('description_en'),
            alt_labels=data.get('altLabels_en', []),
            definition=data.get('definition_en'),
            isco_group=data.get('iscoGroup'),
            essential_skills=data.get('essentialSkills', []),
            optional_skills=data.get('optionalSkills', []),
            metadata=data.get('metadata', {})
        ) 