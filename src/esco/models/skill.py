from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from .base import BaseEntity
from ..core.exceptions import ValidationError

@dataclass
class Skill(BaseEntity):
    """Skill entity model"""
    skill_type: Optional[str] = None
    reuse_level: Optional[str] = None
    definition: Optional[str] = None
    related_skills: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert skill to dictionary representation"""
        return {
            'conceptUri': self.uri,
            'preferredLabel_en': self.preferred_label,
            'description_en': self.description,
            'altLabels_en': self.alt_labels,
            'skillType': self.skill_type,
            'reuseLevel': self.reuse_level,
            'definition_en': self.definition,
            'relatedSkills': self.related_skills,
            'metadata': self.metadata
        }
    
    def validate(self) -> bool:
        """Validate skill data"""
        if not self.uri:
            raise ValidationError("Skill URI is required")
        if not self.preferred_label:
            raise ValidationError("Skill preferred label is required")
        return True
    
    def add_related_skill(self, skill_uri: str) -> None:
        """Add related skill"""
        if skill_uri and skill_uri not in self.related_skills:
            self.related_skills.append(skill_uri)
    
    def get_text_for_embedding(self) -> str:
        """Get text representation for embedding generation"""
        parts = [
            self.preferred_label,
            ' '.join(self.alt_labels),
            self.description or '',
            self.definition or '',
            self.skill_type or '',
            self.reuse_level or ''
        ]
        return '. '.join(filter(None, parts))
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Skill':
        """Create Skill instance from dictionary"""
        return cls(
            uri=data['conceptUri'],
            preferred_label=data.get('preferredLabel_en', ''),
            description=data.get('description_en'),
            alt_labels=data.get('altLabels_en', []),
            skill_type=data.get('skillType'),
            reuse_level=data.get('reuseLevel'),
            definition=data.get('definition_en'),
            related_skills=data.get('relatedSkills', []),
            metadata=data.get('metadata', {})
        ) 