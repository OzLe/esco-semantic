import pytest
import pandas as pd
from pathlib import Path
from esco.ingestion.processors.skill import SkillIngestor
from esco.ingestion.processors.occupation import OccupationIngestor
from esco.ingestion.processors.isco_group import ISCOGroupIngestor
from esco.ingestion.processors.skill_collection import SkillCollectionIngestor
from esco.database.weaviate.client import WeaviateClient
from esco.embeddings.generator import EmbeddingGenerator

@pytest.fixture
def mock_db_client(mocker):
    return mocker.Mock(spec=WeaviateClient)

@pytest.fixture
def mock_embedding_generator(mocker):
    return mocker.Mock(spec=EmbeddingGenerator)

class TestSkillIngestor:
    def test_validate_csv(self, mock_db_client, mock_embedding_generator):
        ingestor = SkillIngestor(mock_db_client, mock_embedding_generator)
        df = pd.DataFrame({
            'conceptUri': ['skill1', 'skill2'],
            'preferredLabel': ['Skill 1', 'Skill 2']
        })
        assert ingestor.validate_csv(df) is True
    
    def test_transform_row(self, mock_db_client, mock_embedding_generator):
        ingestor = SkillIngestor(mock_db_client, mock_embedding_generator)
        row = pd.Series({
            'conceptUri': 'skill1',
            'preferredLabel': 'Skill 1',
            'description': 'Test skill',
            'skillType': 'technical'
        })
        result = ingestor.transform_row(row)
        assert result['conceptUri'] == 'skill1'
        assert result['preferredLabel_en'] == 'Skill 1'

class TestOccupationIngestor:
    def test_validate_csv(self, mock_db_client, mock_embedding_generator):
        ingestor = OccupationIngestor(mock_db_client, mock_embedding_generator)
        df = pd.DataFrame({
            'conceptUri': ['occ1', 'occ2'],
            'preferredLabel': ['Occupation 1', 'Occupation 2'],
            'code': ['1234', '5678']
        })
        assert ingestor.validate_csv(df) is True
    
    def test_transform_row(self, mock_db_client, mock_embedding_generator):
        ingestor = OccupationIngestor(mock_db_client, mock_embedding_generator)
        row = pd.Series({
            'conceptUri': 'occ1',
            'preferredLabel': 'Occupation 1',
            'code': '1234',
            'description': 'Test occupation'
        })
        result = ingestor.transform_row(row)
        assert result['conceptUri'] == 'occ1'
        assert result['preferredLabel_en'] == 'Occupation 1'

class TestISCOGroupIngestor:
    def test_validate_csv(self, mock_db_client, mock_embedding_generator):
        ingestor = ISCOGroupIngestor(mock_db_client, mock_embedding_generator)
        df = pd.DataFrame({
            'conceptUri': ['isco1', 'isco2'],
            'preferredLabel': ['ISCO 1', 'ISCO 2'],
            'code': ['1', '2'],
            'level': [1, 2]
        })
        assert ingestor.validate_csv(df) is True
    
    def test_transform_row(self, mock_db_client, mock_embedding_generator):
        ingestor = ISCOGroupIngestor(mock_db_client, mock_embedding_generator)
        row = pd.Series({
            'conceptUri': 'isco1',
            'preferredLabel': 'ISCO 1',
            'code': '1',
            'level': '1'
        })
        result = ingestor.transform_row(row)
        assert result['conceptUri'] == 'isco1'
        assert result['preferredLabel_en'] == 'ISCO 1'

class TestSkillCollectionIngestor:
    def test_validate_csv(self, mock_db_client, mock_embedding_generator):
        ingestor = SkillCollectionIngestor(mock_db_client, mock_embedding_generator)
        df = pd.DataFrame({
            'conceptUri': ['coll1', 'coll2'],
            'preferredLabel': ['Collection 1', 'Collection 2'],
            'collectionType': ['type1', 'type2']
        })
        assert ingestor.validate_csv(df) is True
    
    def test_transform_row(self, mock_db_client, mock_embedding_generator):
        ingestor = SkillCollectionIngestor(mock_db_client, mock_embedding_generator)
        row = pd.Series({
            'conceptUri': 'coll1',
            'preferredLabel': 'Collection 1',
            'collectionType': 'type1'
        })
        result = ingestor.transform_row(row)
        assert result['conceptUri'] == 'coll1'
        assert result['preferredLabel_en'] == 'Collection 1' 