from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, field
from ..core.exceptions import DatabaseError, ValidationError
from datetime import datetime

@dataclass
class BaseEntity(ABC):
    """Base class for all ESCO entities"""
    uri: str
    preferred_label: str
    description: Optional[str] = None
    alt_labels: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[str, Set[str]] = field(default_factory=dict)
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary representation"""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate entity data"""
        pass
    
    def get_text_for_embedding(self) -> str:
        """Get text representation for embedding generation"""
        parts = [
            self.preferred_label,
            ' '.join(self.alt_labels),
            self.description or ''
        ]
        return '. '.join(filter(None, parts))
    
    def add_alt_label(self, label: str) -> None:
        """Add alternative label"""
        if label and label not in self.alt_labels:
            self.alt_labels.append(label)
    
    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata field"""
        self.metadata[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata field"""
        return self.metadata.get(key, default)
    
    def add_relationship(self, relationship_type: str, target_uri: str) -> None:
        """Add a relationship to another entity"""
        if not relationship_type or not target_uri:
            return
        
        if relationship_type not in self.relationships:
            self.relationships[relationship_type] = set()
        
        self.relationships[relationship_type].add(target_uri)
    
    def remove_relationship(self, relationship_type: str, target_uri: str) -> None:
        """Remove a relationship to another entity"""
        if relationship_type in self.relationships:
            self.relationships[relationship_type].discard(target_uri)
    
    def get_relationships(self, relationship_type: Optional[str] = None) -> Dict[str, Set[str]]:
        """Get relationships, optionally filtered by type"""
        if relationship_type:
            return {relationship_type: self.relationships.get(relationship_type, set())}
        return self.relationships
    
    def has_relationship(self, relationship_type: str, target_uri: str) -> bool:
        """Check if entity has a specific relationship"""
        return (
            relationship_type in self.relationships and
            target_uri in self.relationships[relationship_type]
        )
    
    def validate_relationships(self) -> bool:
        """Validate relationships"""
        for rel_type, targets in self.relationships.items():
            if not rel_type:
                raise ValidationError("Relationship type cannot be empty")
            if not all(targets):
                raise ValidationError(f"Found empty target URI in relationship type: {rel_type}")
        return True
    
    def __str__(self) -> str:
        """String representation"""
        return f"{self.__class__.__name__}(uri={self.uri}, label={self.preferred_label})"
    
    def __repr__(self) -> str:
        """Detailed string representation"""
        return (
            f"{self.__class__.__name__}("
            f"uri={self.uri}, "
            f"preferred_label={self.preferred_label}, "
            f"description={self.description}, "
            f"alt_labels={self.alt_labels}, "
            f"relationships={self.relationships}, "
            f"metadata={self.metadata}"
            f")"
        )

@dataclass
class BaseModel(ABC):
    """Base class for all ESCO models"""
    uri: str
    label: str
    description: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary format"""
        return {
            'uri': self.uri,
            'label': self.label,
            'description': self.description,
            **self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """Create model instance from dictionary"""
        return cls(**data)
    
    def validate(self) -> bool:
        """Validate model data"""
        if not self.uri or not self.label:
            raise DatabaseError("URI and label are required fields")
        return True 