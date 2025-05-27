from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from .engine import SearchResult
from ..core.exceptions import SearchError
from ..database.client import DatabaseClient

@dataclass
class HierarchicalResult:
    """Result of hierarchical search"""
    result: SearchResult
    level: int
    parent: Optional[str] = None
    children: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            **self.result.to_dict(),
            'level': self.level,
            'parent': self.parent,
            'children': self.children
        }

class HierarchicalSearch:
    """Handles hierarchical search operations"""
    
    def __init__(self, db_client: DatabaseClient):
        self.db_client = db_client
    
    def search_up(
        self,
        start_id: str,
        max_levels: int = 4,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[HierarchicalResult]:
        """Search up the hierarchy"""
        try:
            results = []
            current_id = start_id
            level = 0
            
            while level < max_levels:
                # Get current item
                item = self.db_client.get_by_id("entities", current_id)
                if not item:
                    break
                
                # Add to results
                results.append(
                    HierarchicalResult(
                        result=SearchResult(
                            uri=item['uri'],
                            label=item['preferredLabel'],
                            description=item.get('description'),
                            type=item.get('type', 'unknown'),
                            metadata=item.get('metadata', {})
                        ),
                        level=level,
                        parent=item.get('parentUri')
                    )
                )
                
                # Move up
                current_id = item.get('parentUri')
                if not current_id:
                    break
                
                level += 1
            
            return results
        except Exception as e:
            raise SearchError(f"Failed to search up hierarchy: {str(e)}")
    
    def search_down(
        self,
        start_id: str,
        max_levels: int = 4,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[HierarchicalResult]:
        """Search down the hierarchy"""
        try:
            results = []
            to_process = [(start_id, 0)]  # (id, level)
            processed = set()
            
            while to_process:
                current_id, level = to_process.pop(0)
                
                if current_id in processed or level >= max_levels:
                    continue
                
                processed.add(current_id)
                
                # Get current item
                item = self.db_client.get_by_id("entities", current_id)
                if not item:
                    continue
                
                # Add to results
                results.append(
                    HierarchicalResult(
                        result=SearchResult(
                            uri=item['uri'],
                            label=item['preferredLabel'],
                            description=item.get('description'),
                            type=item.get('type', 'unknown'),
                            metadata=item.get('metadata', {})
                        ),
                        level=level,
                        parent=item.get('parentUri'),
                        children=item.get('childUris', [])
                    )
                )
                
                # Add children to process
                for child_id in item.get('childUris', []):
                    if child_id not in processed:
                        to_process.append((child_id, level + 1))
            
            return results
        except Exception as e:
            raise SearchError(f"Failed to search down hierarchy: {str(e)}")
    
    def get_hierarchy(
        self,
        id: str,
        max_levels: int = 4,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get complete hierarchy for an item"""
        try:
            # Get item
            item = self.db_client.get_by_id("entities", id)
            if not item:
                return {}
            
            # Get up and down results
            up_results = self.search_up(id, max_levels, filters)
            down_results = self.search_down(id, max_levels, filters)
            
            # Combine results
            all_results = {r.result.uri: r for r in up_results + down_results}
            
            # Build hierarchy
            hierarchy = {
                'item': item,
                'parents': [r.to_dict() for r in up_results],
                'children': [r.to_dict() for r in down_results],
                'all_items': [r.to_dict() for r in all_results.values()]
            }
            
            return hierarchy
        except Exception as e:
            raise SearchError(f"Failed to get hierarchy: {str(e)}") 