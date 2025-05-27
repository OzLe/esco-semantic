from typing import Dict, Any, Optional
import logging
from pathlib import Path
import pandas as pd
from .processors.occupation import OccupationProcessor
from .processors.skill import SkillProcessor
from .processors.isco_group import ISCOGroupIngestor
from .processors.skill_collection import SkillCollectionIngestor
from ..core.config import Config
from ..core.exceptions import IngestionError
from ..database.weaviate.client import WeaviateClient

logger = logging.getLogger(__name__)

class IngestionOrchestrator:
    """Orchestrates the ESCO data ingestion process"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = WeaviateClient(config)
        self.data_dir = Path(config.get('data.input', 'data/input'))
        
        # Initialize processors
        self.occupation_processor = OccupationProcessor(config, self.client)
        self.skill_processor = SkillProcessor(config, self.client)
        self.isco_group_ingestor = ISCOGroupIngestor(self.client, self.config.get('embeddings.generator'), self.config.get('ingestion.batch_size', 100))
        self.skill_collection_ingestor = SkillCollectionIngestor(self.client, self.config.get('embeddings.generator'), self.config.get('ingestion.batch_size', 100))
    
    def run_ingest(self, force_reingest: bool = False) -> None:
        """Run the complete ingestion process"""
        try:
            # Delete existing data if force_reingest is True
            if force_reingest:
                self._delete_all_data()
            
            # Process each data type
            self._process_isco_groups()
            self._process_occupations()
            self._process_skills()
            self._process_skill_groups()
            self._process_skill_collections()
            
            # Create relations
            self._create_skill_relations()
            self._create_hierarchical_relations()
            self._create_isco_group_relations()
            self._create_skill_collection_relations()
            self._create_skill_skill_relations()
            
        except Exception as e:
            raise IngestionError(f"Failed to run ingestion process: {str(e)}")
    
    def _delete_all_data(self) -> None:
        """Delete all data from the database"""
        try:
            self.client.delete_all_data()
        except Exception as e:
            raise IngestionError(f"Failed to delete existing data: {str(e)}")
    
    def _process_isco_groups(self) -> None:
        """Process ISCO groups"""
        file_path = self.data_dir / 'isco_groups.csv'
        if not file_path.exists():
            raise IngestionError(f"ISCO groups file not found: {file_path}")
        self.isco_group_ingestor.ingest_file(str(file_path))
    
    def _process_occupations(self) -> None:
        """Process occupations"""
        file_path = self.data_dir / 'occupations.csv'
        if not file_path.exists():
            raise IngestionError(f"Occupations file not found: {file_path}")
        self.occupation_processor.process_csv_in_batches(str(file_path), self.occupation_processor.process)
    
    def _process_skills(self) -> None:
        """Process skills"""
        file_path = self.data_dir / 'skills.csv'
        if not file_path.exists():
            raise IngestionError(f"Skills file not found: {file_path}")
        self.skill_processor.process_csv_in_batches(str(file_path), self.skill_processor.process)
    
    def _process_skill_groups(self) -> None:
        """Process skill groups"""
        file_path = self.data_dir / 'skill_groups.csv'
        if not file_path.exists():
            raise IngestionError(f"Skill groups file not found: {file_path}")
        self.skill_processor.process_csv_in_batches(str(file_path), self.skill_processor.process)
    
    def _process_skill_collections(self) -> None:
        """Process skill collections"""
        file_path = self.data_dir / 'skill_collections.csv'
        if not file_path.exists():
            raise IngestionError(f"Skill collections file not found: {file_path}")
        self.skill_collection_ingestor.ingest_file(str(file_path))
    
    def _create_skill_relations(self) -> None:
        """Create skill relations for occupations"""
        file_path = self.data_dir / 'occupation_skill_relations.csv'
        if not file_path.exists():
            raise IngestionError(f"Occupation-skill relations file not found: {file_path}")
        self.occupation_processor.process_csv_in_batches(str(file_path), lambda df: self._process_skill_relations_batch(df))
    
    def _process_skill_relations_batch(self, df: pd.DataFrame) -> None:
        """Process a batch of skill relations"""
        for _, row in df.iterrows():
            self.occupation_processor.add_skill_relations(
                occupation_uri=row['occupationUri'],
                essential_skills=row.get('essentialSkills', []),
                optional_skills=row.get('optionalSkills', [])
            )
    
    def _create_hierarchical_relations(self) -> None:
        """Create hierarchical relations"""
        file_path = self.data_dir / 'hierarchical_relations.csv'
        if not file_path.exists():
            raise IngestionError(f"Hierarchical relations file not found: {file_path}")
        self.skill_processor.process_csv_in_batches(str(file_path), lambda df: self._process_hierarchical_relations_batch(df))
    
    def _process_hierarchical_relations_batch(self, df: pd.DataFrame) -> None:
        """Process a batch of hierarchical relations"""
        for _, row in df.iterrows():
            self.skill_processor.add_related_skill_relation(
                from_skill_uri=row['broaderUri'],
                to_skill_uri=row['narrowerUri'],
                relation_type='hasNarrowerSkill'
            )
    
    def _create_isco_group_relations(self) -> None:
        """Create ISCO group relations"""
        file_path = self.data_dir / 'isco_group_relations.csv'
        if not file_path.exists():
            raise IngestionError(f"ISCO group relations file not found: {file_path}")
        self.occupation_processor.process_csv_in_batches(str(file_path), lambda df: self._process_isco_group_relations_batch(df))
    
    def _process_isco_group_relations_batch(self, df: pd.DataFrame) -> None:
        """Process a batch of ISCO group relations"""
        for _, row in df.iterrows():
            self.occupation_processor.add_isco_group_relation(
                occupation_uri=row['occupationUri'],
                isco_group_uri=row['iscoGroupUri']
            )
    
    def _create_skill_collection_relations(self) -> None:
        """Create skill collection relations"""
        file_path = self.data_dir / 'skill_collection_relations.csv'
        if not file_path.exists():
            raise IngestionError(f"Skill collection relations file not found: {file_path}")
        self.skill_processor.process_csv_in_batches(str(file_path), lambda df: self._process_skill_collection_relations_batch(df))
    
    def _process_skill_collection_relations_batch(self, df: pd.DataFrame) -> None:
        """Process a batch of skill collection relations"""
        for _, row in df.iterrows():
            self.skill_processor.add_skill_collection_relation(
                skill_uri=row['skillUri'],
                collection_uri=row['collectionUri']
            )
    
    def _create_skill_skill_relations(self) -> None:
        """Create skill-to-skill relations"""
        file_path = self.data_dir / 'skill_skill_relations.csv'
        if not file_path.exists():
            raise IngestionError(f"Skill-skill relations file not found: {file_path}")
        self.skill_processor.process_csv_in_batches(str(file_path), lambda df: self._process_skill_skill_relations_batch(df))
    
    def _process_skill_skill_relations_batch(self, df: pd.DataFrame) -> None:
        """Process a batch of skill-to-skill relations"""
        for _, row in df.iterrows():
            self.skill_processor.add_related_skill_relation(
                from_skill_uri=row['fromSkillUri'],
                to_skill_uri=row['toSkillUri'],
                relation_type=row.get('relationType', 'relatedTo')
            ) 