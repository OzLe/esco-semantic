import pytest
from unittest.mock import Mock, patch
from esco.search.engine import SearchResult
from esco.search.semantic import SemanticSearchEngine
from esco.search.results import SearchResultFormatter

class TestSearchResult:
    def test_search_result_creation(self):
        result = SearchResult(
            uri="test-uri",
            label="Test Label",
            description="Test Description",
            score=0.8,
            type="skill"
        )
        
        assert result.uri == "test-uri"
        assert result.label == "Test Label"
        assert result.description == "Test Description"
        assert result.score == 0.8
        assert result.type == "skill"
    
    def test_search_result_to_dict(self):
        result = SearchResult(
            uri="test-uri",
            label="Test Label",
            description="Test Description",
            score=0.8,
            type="skill",
            metadata={"key": "value"}
        )
        
        data = result.to_dict()
        assert data["uri"] == "test-uri"
        assert data["label"] == "Test Label"
        assert data["description"] == "Test Description"
        assert data["score"] == 0.8
        assert data["type"] == "skill"
        assert data["metadata"] == {"key": "value"}

class TestSearchResultFormatter:
    def test_to_dict(self):
        results = [
            SearchResult(
                uri="test-uri-1",
                label="Test Label 1",
                score=0.8
            ),
            SearchResult(
                uri="test-uri-2",
                label="Test Label 2",
                score=0.6
            )
        ]
        
        formatted = SearchResultFormatter.to_dict(results)
        assert len(formatted) == 2
        assert formatted[0]["uri"] == "test-uri-1"
        assert formatted[1]["uri"] == "test-uri-2"
    
    def test_to_json(self):
        results = [
            SearchResult(
                uri="test-uri",
                label="Test Label",
                score=0.8
            )
        ]
        
        json_str = SearchResultFormatter.to_json(results)
        assert "test-uri" in json_str
        assert "Test Label" in json_str
        assert "0.8" in json_str
    
    def test_to_text(self):
        results = [
            SearchResult(
                uri="test-uri",
                label="Test Label",
                description="Test Description",
                score=0.8,
                type="skill"
            )
        ]
        
        text = SearchResultFormatter.to_text(results)
        assert "Test Label" in text
        assert "Test Description" in text
        assert "0.8" in text
        assert "skill" in text

class TestSemanticSearchEngine:
    @pytest.fixture
    def mock_db_client(self):
        client = Mock()
        client.search.return_value = [
            {
                "uri": "test-uri",
                "preferredLabel": "Test Label",
                "description": "Test Description",
                "score": 0.8,
                "type": "skill"
            }
        ]
        return client
    
    @pytest.fixture
    def mock_embedding_generator(self):
        generator = Mock()
        generator.generate.return_value = [0.1, 0.2, 0.3]
        return generator
    
    def test_search(self, mock_db_client, mock_embedding_generator):
        engine = SemanticSearchEngine(mock_db_client, mock_embedding_generator)
        
        results = engine.search("test query")
        
        assert len(results) == 1
        assert results[0].uri == "test-uri"
        assert results[0].label == "Test Label"
        assert results[0].score == 0.8
    
    def test_search_with_filters(self, mock_db_client, mock_embedding_generator):
        engine = SemanticSearchEngine(mock_db_client, mock_embedding_generator)
        
        filters = {"type": "skill"}
        engine.search("test query", filters=filters)
        
        mock_db_client.search.assert_called_once()
        call_args = mock_db_client.search.call_args[1]
        assert call_args["filters"] == filters
    
    def test_get_by_id(self, mock_db_client, mock_embedding_generator):
        mock_db_client.get_by_id.return_value = {
            "uri": "test-uri",
            "preferredLabel": "Test Label",
            "description": "Test Description",
            "type": "skill"
        }
        
        engine = SemanticSearchEngine(mock_db_client, mock_embedding_generator)
        result = engine.get_by_id("test-uri")
        
        assert result is not None
        assert result.uri == "test-uri"
        assert result.label == "Test Label"
    
    def test_get_similar(self, mock_db_client, mock_embedding_generator):
        mock_db_client.get_vector.return_value = [0.1, 0.2, 0.3]
        mock_db_client.search.return_value = [
            {
                "uri": "similar-uri",
                "preferredLabel": "Similar Label",
                "score": 0.9
            }
        ]
        
        engine = SemanticSearchEngine(mock_db_client, mock_embedding_generator)
        results = engine.get_similar("test-uri")
        
        assert len(results) == 1
        assert results[0].uri == "similar-uri"
        assert results[0].label == "Similar Label"
        assert results[0].score == 0.9 