from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from .base import BaseEntity
from ..core.exceptions import ValidationError

@dataclass
class SkillCollection(BaseEntity):
    """Skill Collection entity model for grouping related skills"""
    collection_type: Optional[str] = None
    skills: List[str] = field(default_factory=list)
    parent_collection: Optional[str] = None
    child_collections: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert skill collection to dictionary representation"""
        return {
            'conceptUri': self.uri,
            'preferredLabel_en': self.preferred_label,
            'description_en': self.description,
            'altLabels_en': self.alt_labels,
            'collectionType': self.collection_type,
            'skills': self.skills,
            'parentCollection': self.parent_collection,
            'childCollections': self.child_collections,
            'metadata': self.metadata
        }
    
    def validate(self) -> bool:
        """Validate skill collection data"""
        if not self.uri:
            raise ValidationError("Skill Collection URI is required")
        if not self.preferred_label:
            raise ValidationError("Skill Collection preferred label is required")
        if not self.collection_type:
            raise ValidationError("Skill Collection type is required")
        return True
    
    def add_skill(self, skill_uri: str) -> None:
        """Add skill to this collection"""
        if skill_uri and skill_uri not in self.skills:
            self.skills.append(skill_uri)
    
    def add_child_collection(self, collection_uri: str) -> None:
        """Add child collection"""
        if collection_uri and collection_uri not in self.child_collections:
            self.child_collections.append(collection_uri)
    
    def get_text_for_embedding(self) -> str:
        """Get text representation for embedding generation"""
        parts = [
            self.preferred_label,
            ' '.join(self.alt_labels),
            self.description or '',
            f"Collection Type: {self.collection_type}" if self.collection_type else ''
        ]
        return '. '.join(filter(None, parts))
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SkillCollection':
        """Create SkillCollection instance from dictionary"""
        return cls(
            uri=data['conceptUri'],
            preferred_label=data.get('preferredLabel_en', ''),
            description=data.get('description_en'),
            alt_labels=data.get('altLabels_en', []),
            collection_type=data.get('collectionType'),
            skills=data.get('skills', []),
            parent_collection=data.get('parentCollection'),
            child_collections=data.get('childCollections', []),
            metadata=data.get('metadata', {})
        ) 