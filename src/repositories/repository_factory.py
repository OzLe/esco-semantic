from typing import Dict, Type, TYPE_CHECKING
from .base_repository import BaseRepository
from .weaviate_repository import WeaviateRepository
from .occupation_repository import OccupationRepository
from .skill_repository import SkillRepository
from .isco_group_repository import ISCOGroupRepository
from .skill_collection_repository import SkillCollectionRepository
from .skill_group_repository import SkillGroupRepository

if TYPE_CHECKING:
    from ..weaviate_client import WeaviateClient

class RepositoryFactory:
    """Factory for creating and managing repository instances."""
    
    _repositories: Dict[str, BaseRepository] = {}
    
    @classmethod
    def get_repository(cls, client: 'WeaviateClient', repository_type: str) -> BaseRepository:
        """Get or create a repository instance."""
        if repository_type not in cls._repositories:
            if repository_type == "Occupation":
                cls._repositories[repository_type] = OccupationRepository(client)
            elif repository_type == "Skill":
                cls._repositories[repository_type] = SkillRepository(client)
            elif repository_type == "ISCOGroup":
                cls._repositories[repository_type] = ISCOGroupRepository(client)
            elif repository_type == "SkillCollection":
                cls._repositories[repository_type] = SkillCollectionRepository(client)
            elif repository_type == "SkillGroup":
                cls._repositories[repository_type] = SkillGroupRepository(client)
            else:
                cls._repositories[repository_type] = WeaviateRepository(client, repository_type)
        
        return cls._repositories[repository_type]
    
    @classmethod
    def clear_repositories(cls):
        """Clear all repository instances."""
        cls._repositories.clear() 