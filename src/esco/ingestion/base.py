from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from ..core.exceptions import IngestionError
from ..database.client import DatabaseClient
from ..embeddings.generator import EmbeddingGenerator
import logging

logger = logging.getLogger(__name__)

class BaseIngestor(ABC):
    """Base class for all entity ingestors"""
    
    def __init__(
        self, 
        db_client: DatabaseClient,
        embedding_generator: EmbeddingGenerator,
        batch_size: int = 100
    ):
        self.db_client = db_client
        self.embedding_generator = embedding_generator
        self.batch_size = batch_size
    
    @abstractmethod
    def get_entity_type(self) -> str:
        """Get the entity type this ingestor handles"""
        pass
    
    @abstractmethod
    def validate_csv(self, df: pd.DataFrame) -> bool:
        """Validate CSV has required columns"""
        pass
    
    @abstractmethod
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        """Transform a CSV row to entity dict"""
        pass
    
    def ingest_file(self, file_path: str) -> int:
        """Ingest entities from CSV file"""
        logger.info(f"Starting ingestion of {file_path}")
        
        # Read CSV
        df = pd.read_csv(file_path, dtype=str)
        df = df.fillna('')
        
        # Validate
        if not self.validate_csv(df):
            raise ValueError(f"Invalid CSV format for {self.get_entity_type()}")
        
        # Process in batches
        total_processed = 0
        entities = []
        
        for idx, row in df.iterrows():
            entity = self.transform_row(row)
            if entity:
                entities.append(entity)
                
                if len(entities) >= self.batch_size:
                    self._process_batch(entities)
                    total_processed += len(entities)
                    entities = []
        
        # Process remaining
        if entities:
            self._process_batch(entities)
            total_processed += len(entities)
        
        logger.info(f"Completed ingestion: {total_processed} entities")
        return total_processed
    
    def _process_batch(self, entities: List[Dict[str, Any]]) -> None:
        """Process a batch of entities"""
        # Generate embeddings
        texts = [self._get_text_for_embedding(e) for e in entities]
        embeddings = self.embedding_generator.generate_batch(texts)
        
        # Insert to database
        self.db_client.insert_batch(
            collection=self.get_entity_type(),
            objects=entities,
            vectors=embeddings
        )
    
    @abstractmethod
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        """Get text representation for embedding generation"""
        pass

    @abstractmethod
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate input data"""
        pass
    
    @abstractmethod
    def transform_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform data for ingestion"""
        pass
    
    @abstractmethod
    def ingest(self, data: List[Dict[str, Any]]) -> None:
        """Ingest transformed data"""
        pass
    
    def process(self, df: pd.DataFrame) -> None:
        """Process data through the ingestion pipeline"""
        try:
            if not self.validate_data(df):
                raise IngestionError("Data validation failed")
            
            transformed_data = self.transform_data(df)
            self.ingest(transformed_data)
        except Exception as e:
            raise IngestionError(f"Failed to process data: {str(e)}")
    
    def batch_process(self, dfs: List[pd.DataFrame]) -> None:
        """Process multiple dataframes through the ingestion pipeline"""
        for df in dfs:
            self.process(df) 