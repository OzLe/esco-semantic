import pytest
from unittest.mock import Mock, patch
from src.weaviate_search_engine import WeaviateSearchEngine
from src.search_engine import SearchResult

@pytest.fixture
def weaviate_config():
    return {
        "url": "http://localhost:8080",
        "auth_client_secret": None,
        "class_name": "TestClass",
        "properties": ["content", "metadata"]
    }

@pytest.fixture
def mock_weaviate_client():
    with patch("weaviate.Client") as mock_client:
        yield mock_client

@pytest.fixture
def mock_query_builder():
    mock_builder = Mock()
    mock_builder.with_bm25.return_value = mock_builder
    mock_builder.with_near_text.return_value = mock_builder
    mock_builder.with_limit.return_value = mock_builder
    mock_builder.with_where.return_value = mock_builder
    return mock_builder

@pytest.mark.asyncio
async def test_weaviate_search_engine_initialization(weaviate_config, mock_weaviate_client):
    """Test initialization of WeaviateSearchEngine."""
    engine = WeaviateSearchEngine(weaviate_config)
    assert engine.url == weaviate_config["url"]
    assert engine.class_name == weaviate_config["class_name"]
    assert engine.properties == weaviate_config["properties"]
    mock_weaviate_client.assert_called_once()

@pytest.mark.asyncio
async def test_text_search(weaviate_config, mock_weaviate_client, mock_query_builder):
    """Test text search functionality."""
    # Setup mock response
    mock_response = {
        "data": {
            "Get": {
                "TestClass": [
                    {
                        "_additional": {"id": "1", "score": 0.8},
                        "content": "test content",
                        "metadata": {"type": "test"}
                    }
                ]
            }
        }
    }
    mock_query_builder.do.return_value = mock_response
    mock_weaviate_client.return_value.query.get.return_value = mock_query_builder

    engine = WeaviateSearchEngine(weaviate_config)
    results = await engine.search("test query", limit=1)

    assert len(results) == 1
    assert isinstance(results[0], SearchResult)
    assert results[0].id == "1"
    assert results[0].content == "test content"
    assert results[0].score == 0.8
    assert results[0].metadata == {"type": "test"}
    assert results[0].source == "weaviate_text_search"

@pytest.mark.asyncio
async def test_semantic_search(weaviate_config, mock_weaviate_client, mock_query_builder):
    """Test semantic search functionality."""
    # Setup mock response
    mock_response = {
        "data": {
            "Get": {
                "TestClass": [
                    {
                        "_additional": {"id": "1", "certainty": 0.9},
                        "content": "test content",
                        "metadata": {"type": "test"}
                    }
                ]
            }
        }
    }
    mock_query_builder.do.return_value = mock_response
    mock_weaviate_client.return_value.query.get.return_value = mock_query_builder

    engine = WeaviateSearchEngine(weaviate_config)
    results = await engine.semantic_search("test query", limit=1)

    assert len(results) == 1
    assert isinstance(results[0], SearchResult)
    assert results[0].id == "1"
    assert results[0].content == "test content"
    assert results[0].score == 0.9
    assert results[0].metadata == {"type": "test"}
    assert results[0].source == "weaviate_semantic_search"

@pytest.mark.asyncio
async def test_hybrid_search(weaviate_config, mock_weaviate_client, mock_query_builder):
    """Test hybrid search functionality."""
    # Setup mock responses for both text and semantic search
    text_response = {
        "data": {
            "Get": {
                "TestClass": [
                    {
                        "_additional": {"id": "1", "score": 0.8},
                        "content": "test content 1",
                        "metadata": {"type": "test"}
                    }
                ]
            }
        }
    }
    semantic_response = {
        "data": {
            "Get": {
                "TestClass": [
                    {
                        "_additional": {"id": "1", "certainty": 0.9},
                        "content": "test content 1",
                        "metadata": {"type": "test"}
                    }
                ]
            }
        }
    }
    
    # Configure mock to return different responses for different calls
    mock_query_builder.do.side_effect = [text_response, semantic_response]
    mock_weaviate_client.return_value.query.get.return_value = mock_query_builder

    engine = WeaviateSearchEngine(weaviate_config)
    results = await engine.hybrid_search("test query", limit=1, semantic_weight=0.5)

    assert len(results) == 1
    assert isinstance(results[0], SearchResult)
    assert results[0].id == "1"
    assert results[0].content == "test content 1"
    assert results[0].metadata == {"type": "test"}

@pytest.mark.asyncio
async def test_search_with_filters(weaviate_config, mock_weaviate_client, mock_query_builder):
    """Test search with filters."""
    mock_response = {
        "data": {
            "Get": {
                "TestClass": []
            }
        }
    }
    mock_query_builder.do.return_value = mock_response
    mock_weaviate_client.return_value.query.get.return_value = mock_query_builder

    engine = WeaviateSearchEngine(weaviate_config)
    filters = {"type": "test"}
    await engine.search("test query", filters=filters)

    # Verify that with_where was called with the correct filter
    mock_query_builder.with_where.assert_called_with({
        "path": ["type"],
        "operator": "Equal",
        "valueString": "test"
    })

@pytest.mark.asyncio
async def test_error_handling(weaviate_config, mock_weaviate_client):
    """Test error handling in search operations."""
    mock_weaviate_client.return_value.query.get.side_effect = Exception("Test error")

    engine = WeaviateSearchEngine(weaviate_config)
    with pytest.raises(Exception) as exc_info:
        await engine.search("test query")
    assert str(exc_info.value) == "Test error"

@pytest.mark.asyncio
async def test_close(weaviate_config, mock_weaviate_client):
    """Test cleanup of resources."""
    engine = WeaviateSearchEngine(weaviate_config)
    await engine.close()
    mock_weaviate_client.return_value.close.assert_called_once() 