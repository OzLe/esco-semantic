import pytest
from pathlib import Path
import pandas as pd
from esco.database.weaviate.client import WeaviateClient
from esco.embeddings.generator import EmbeddingGenerator
from esco.search.semantic import SemanticSearchEngine
from esco.translation.translator import TranslationEngine
from esco.translation.batch import BatchTranslator
from ..fixtures.sample_data import (
    get_sample_skills,
    get_sample_occupations,
    get_sample_search_results
)

@pytest.fixture(scope="module")
def db_client():
    """Create test database client"""
    client = WeaviateClient()
    client.connect()
    yield client
    client.disconnect()

@pytest.fixture(scope="module")
def embedding_generator():
    """Create embedding generator"""
    return EmbeddingGenerator()

@pytest.fixture(scope="module")
def search_engine(db_client, embedding_generator):
    """Create search engine"""
    return SemanticSearchEngine(db_client, embedding_generator)

@pytest.fixture(scope="module")
def translation_engine():
    """Create translation engine"""
    return TranslationEngine()

class TestEndToEnd:
    def test_ingestion_and_search(self, db_client, search_engine):
        """Test data ingestion and search"""
        # Ingest sample data
        skills = get_sample_skills()
        occupations = get_sample_occupations()
        
        # Insert skills
        for skill in skills:
            db_client.insert("skills", skill)
        
        # Insert occupations
        for occupation in occupations:
            db_client.insert("occupations", occupation)
        
        # Test search
        results = search_engine.search("python programming")
        assert len(results) > 0
        assert any(r.label == "Python Programming" for r in results)
    
    def test_translation_and_search(self, translation_engine, search_engine):
        """Test translation and search"""
        # Translate query
        query = "python programming"
        translated = translation_engine.translate(query)
        
        # Search with translated query
        results = search_engine.search(translated)
        assert len(results) > 0
    
    def test_batch_translation(self, translation_engine):
        """Test batch translation"""
        # Create sample data
        df = pd.DataFrame({
            'text': [
                'Python Programming',
                'Data Analysis',
                'Project Management'
            ]
        })
        
        # Create batch translator
        batch_translator = BatchTranslator(translation_engine)
        
        # Translate
        result = batch_translator.translate_dataframe(
            df=df,
            columns=['text'],
            output_columns=['translated']
        )
        
        assert 'translated' in result.columns
        assert len(result) == 3
    
    def test_search_with_filters(self, search_engine):
        """Test search with filters"""
        # Search with type filter
        results = search_engine.search(
            "programming",
            filters={"type": "skill"}
        )
        
        assert len(results) > 0
        assert all(r.type == "skill" for r in results)
    
    def test_similar_items(self, search_engine):
        """Test similar items search"""
        # Get similar items for a skill
        results = search_engine.get_similar("skill-001")
        
        assert len(results) > 0
        assert all(r.type == "skill" for r in results)
    
    def test_multilingual_search(self, translation_engine, search_engine):
        """Test search in multiple languages"""
        # Test queries in different languages
        queries = [
            "python programming",
            "programación en python",
            "programmation python",
            "python-programmierung"
        ]
        
        for query in queries:
            results = search_engine.search(query)
            assert len(results) > 0
            assert any("python" in r.label.lower() for r in results) 