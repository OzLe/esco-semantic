from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from ..core.exceptions import ValidationError

@dataclass
class SearchFilter:
    """Search filter configuration"""
    entity_types: Optional[List[str]] = None
    min_score: float = 0.0
    max_score: float = 1.0
    has_relationships: Optional[List[str]] = None
    has_metadata: Optional[Dict[str, Any]] = None
    hierarchical_level: Optional[int] = None
    language: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert filter to dictionary format"""
        filter_dict = {}
        
        if self.entity_types:
            filter_dict['entityTypes'] = self.entity_types
        
        if self.min_score > 0:
            filter_dict['minScore'] = self.min_score
        
        if self.max_score < 1.0:
            filter_dict['maxScore'] = self.max_score
        
        if self.has_relationships:
            filter_dict['hasRelationships'] = self.has_relationships
        
        if self.has_metadata:
            filter_dict['hasMetadata'] = self.has_metadata
        
        if self.hierarchical_level is not None:
            filter_dict['hierarchicalLevel'] = self.hierarchical_level
        
        if self.language:
            filter_dict['language'] = self.language
        
        return filter_dict
    
    def validate(self) -> bool:
        """Validate filter configuration"""
        if self.min_score < 0 or self.min_score > 1:
            raise ValidationError("min_score must be between 0 and 1")
        
        if self.max_score < 0 or self.max_score > 1:
            raise ValidationError("max_score must be between 0 and 1")
        
        if self.min_score > self.max_score:
            raise ValidationError("min_score cannot be greater than max_score")
        
        if self.hierarchical_level is not None and not (1 <= self.hierarchical_level <= 4):
            raise ValidationError("hierarchical_level must be between 1 and 4")
        
        return True

class FilterBuilder:
    """Builder for constructing search filters"""
    
    def __init__(self):
        self.filter = SearchFilter()
    
    def with_entity_types(self, types: List[str]) -> 'FilterBuilder':
        """Add entity type filter"""
        self.filter.entity_types = types
        return self
    
    def with_score_range(self, min_score: float, max_score: float) -> 'FilterBuilder':
        """Add score range filter"""
        self.filter.min_score = min_score
        self.filter.max_score = max_score
        return self
    
    def with_relationships(self, relationship_types: List[str]) -> 'FilterBuilder':
        """Add relationship filter"""
        self.filter.has_relationships = relationship_types
        return self
    
    def with_metadata(self, metadata: Dict[str, Any]) -> 'FilterBuilder':
        """Add metadata filter"""
        self.filter.has_metadata = metadata
        return self
    
    def with_hierarchical_level(self, level: int) -> 'FilterBuilder':
        """Add hierarchical level filter"""
        self.filter.hierarchical_level = level
        return self
    
    def with_language(self, language: str) -> 'FilterBuilder':
        """Add language filter"""
        self.filter.language = language
        return self
    
    def build(self) -> SearchFilter:
        """Build and validate the filter"""
        self.filter.validate()
        return self.filter 