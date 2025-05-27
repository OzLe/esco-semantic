import pytest
import pandas as pd
from pathlib import Path
from esco.ingestion.orchestrator import IngestionOrchestrator
from esco.database.weaviate.client import WeaviateClient
from esco.embeddings.generator import EmbeddingGenerator

@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary directory with test data files."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    
    # Create sample CSV files
    skills_df = pd.DataFrame({
        'conceptUri': ['skill1', 'skill2'],
        'preferredLabel': ['Skill 1', 'Skill 2'],
        'description': ['Test skill 1', 'Test skill 2'],
        'skillType': ['technical', 'soft']
    })
    skills_df.to_csv(data_dir / "skills.csv", index=False)
    
    occupations_df = pd.DataFrame({
        'conceptUri': ['occ1', 'occ2'],
        'preferredLabel': ['Occupation 1', 'Occupation 2'],
        'code': ['1234', '5678'],
        'description': ['Test occupation 1', 'Test occupation 2']
    })
    occupations_df.to_csv(data_dir / "occupations.csv", index=False)
    
    isco_df = pd.DataFrame({
        'conceptUri': ['isco1', 'isco2'],
        'preferredLabel': ['ISCO 1', 'ISCO 2'],
        'code': ['1', '2'],
        'level': ['1', '2']
    })
    isco_df.to_csv(data_dir / "isco_groups.csv", index=False)
    
    collections_df = pd.DataFrame({
        'conceptUri': ['coll1', 'coll2'],
        'preferredLabel': ['Collection 1', 'Collection 2'],
        'collectionType': ['type1', 'type2']
    })
    collections_df.to_csv(data_dir / "skill_collections.csv", index=False)
    
    return data_dir

@pytest.fixture
def orchestrator(mocker):
    """Create an IngestionOrchestrator with mocked dependencies."""
    mock_db = mocker.Mock(spec=WeaviateClient)
    mock_embeddings = mocker.Mock(spec=EmbeddingGenerator)
    return IngestionOrchestrator(mock_db, mock_embeddings)

class TestIngestionPipeline:
    def test_full_ingestion_process(self, orchestrator, test_data_dir, mocker):
        """Test the complete ingestion process."""
        # Mock the database operations
        mocker.patch.object(orchestrator.db_client, 'create_schema')
        mocker.patch.object(orchestrator.db_client, 'batch_upsert')
        
        # Mock the embedding generation
        mocker.patch.object(orchestrator.embedding_generator, 'generate_embeddings')
        
        # Run the ingestion process
        orchestrator.ingest_all(test_data_dir)
        
        # Verify schema creation
        orchestrator.db_client.create_schema.assert_called_once()
        
        # Verify batch upserts were called for each ingestor
        assert orchestrator.db_client.batch_upsert.call_count == 4
        
        # Verify embedding generation
        assert orchestrator.embedding_generator.generate_embeddings.call_count == 4
    
    def test_ingestion_with_invalid_data(self, orchestrator, test_data_dir, mocker):
        """Test ingestion process with invalid data."""
        # Create invalid CSV file
        invalid_df = pd.DataFrame({
            'invalid_column': ['value1', 'value2']
        })
        invalid_df.to_csv(test_data_dir / "invalid.csv", index=False)
        
        # Mock the database operations
        mocker.patch.object(orchestrator.db_client, 'create_schema')
        mocker.patch.object(orchestrator.db_client, 'batch_upsert')
        
        # Run the ingestion process
        with pytest.raises(Exception):
            orchestrator.ingest_all(test_data_dir)
        
        # Verify schema creation was attempted
        orchestrator.db_client.create_schema.assert_called_once()
        
        # Verify no batch upserts were performed
        orchestrator.db_client.batch_upsert.assert_not_called()
    
    def test_ingestion_with_missing_files(self, orchestrator, test_data_dir, mocker):
        """Test ingestion process with missing data files."""
        # Remove one of the required files
        (test_data_dir / "skills.csv").unlink()
        
        # Mock the database operations
        mocker.patch.object(orchestrator.db_client, 'create_schema')
        mocker.patch.object(orchestrator.db_client, 'batch_upsert')
        
        # Run the ingestion process
        with pytest.raises(FileNotFoundError):
            orchestrator.ingest_all(test_data_dir)
        
        # Verify schema creation was attempted
        orchestrator.db_client.create_schema.assert_called_once()
        
        # Verify no batch upserts were performed
        orchestrator.db_client.batch_upsert.assert_not_called() 