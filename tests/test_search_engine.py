import pytest
import numpy as np
from datetime import datetime
from src.search_engine import SearchEngine, SearchResult
from src.semantic_search_engine import SemanticSearchEngine

@pytest.fixture
def sample_corpus():
    return [
        "The quick brown fox jumps over the lazy dog",
        "A fast orange fox leaps over a sleepy canine",
        "The lazy dog sleeps in the sun",
        "A sleepy canine rests in the sunlight",
        "The fox is quick and brown",
        "The dog is lazy and sleepy"
    ]

@pytest.fixture
def sample_metadata():
    return [
        {"type": "sentence", "length": 43},
        {"type": "sentence", "length": 45},
        {"type": "sentence", "length": 30},
        {"type": "sentence", "length": 35},
        {"type": "sentence", "length": 25},
        {"type": "sentence", "length": 25}
    ]

@pytest.fixture
def search_engine_config():
    return {
        "model_name": "sentence-transformers/all-MiniLM-L6-v2",
        "cache_dir": "models",
        "batch_size": 32
    }

def test_search_result_creation():
    """Test creation of SearchResult objects."""
    result = SearchResult(
        id="1",
        content="test content",
        score=0.95,
        metadata={"type": "test"},
        source="test_source"
    )
    
    assert result.id == "1"
    assert result.content == "test content"
    assert result.score == 0.95
    assert result.metadata == {"type": "test"}
    assert result.source == "test_source"
    assert isinstance(result.timestamp, datetime)

def test_search_result_to_dict():
    """Test conversion of SearchResult to dictionary."""
    result = SearchResult(
        id="1",
        content="test content",
        score=0.95,
        metadata={"type": "test"},
        source="test_source"
    )
    
    result_dict = result.to_dict()
    assert result_dict["id"] == "1"
    assert result_dict["content"] == "test content"
    assert result_dict["score"] == 0.95
    assert result_dict["metadata"] == {"type": "test"}
    assert result_dict["source"] == "test_source"
    assert "timestamp" in result_dict

@pytest.mark.asyncio
async def test_semantic_search_engine_initialization(search_engine_config):
    """Test initialization of SemanticSearchEngine."""
    engine = SemanticSearchEngine(search_engine_config)
    assert engine.model_name == search_engine_config["model_name"]
    assert engine.cache_dir == search_engine_config["cache_dir"]
    assert engine.batch_size == search_engine_config["batch_size"]

@pytest.mark.asyncio
async def test_semantic_search(search_engine_config, sample_corpus, sample_metadata):
    """Test semantic search functionality."""
    engine = SemanticSearchEngine(search_engine_config)
    
    # Test search with a query
    query = "quick fox"
    results = await engine.semantic_search(
        query=query,
        limit=3,
        corpus=sample_corpus,
        metadata=sample_metadata
    )
    
    assert len(results) <= 3
    assert all(isinstance(result, SearchResult) for result in results)
    assert all(result.score >= 0 and result.score <= 1 for result in results)
    
    # Verify results are sorted by score
    scores = [result.score for result in results]
    assert scores == sorted(scores, reverse=True)

@pytest.mark.asyncio
async def test_batch_search(search_engine_config, sample_corpus, sample_metadata):
    """Test batch search functionality."""
    engine = SemanticSearchEngine(search_engine_config)
    
    queries = ["quick fox", "lazy dog"]
    results = await engine.batch_search(
        queries=queries,
        limit=2,
        corpus=sample_corpus,
        metadata=sample_metadata
    )
    
    assert len(results) == len(queries)
    assert all(query in results for query in queries)
    assert all(len(results[query]) <= 2 for query in queries)

@pytest.mark.asyncio
async def test_hybrid_search(search_engine_config, sample_corpus, sample_metadata):
    """Test hybrid search functionality."""
    engine = SemanticSearchEngine(search_engine_config)
    
    query = "quick fox"
    results = await engine.hybrid_search(
        query=query,
        limit=3,
        semantic_weight=0.7,
        corpus=sample_corpus,
        metadata=sample_metadata
    )
    
    assert len(results) <= 3
    assert all(isinstance(result, SearchResult) for result in results)
    assert all(result.score >= 0 and result.score <= 1 for result in results)

@pytest.mark.asyncio
async def test_search_engine_error_handling(search_engine_config):
    """Test error handling in search engine."""
    engine = SemanticSearchEngine(search_engine_config)
    
    # Test with empty corpus
    results = await engine.semantic_search(
        query="test",
        corpus=[],
        metadata=[]
    )
    assert len(results) == 0
    
    # Test with invalid query
    with pytest.raises(Exception):
        await engine.semantic_search(
            query="",
            corpus=["test"],
            metadata=[{}]
        ) 