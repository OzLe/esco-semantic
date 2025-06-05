"""
Test configuration and fixtures for ESCO ingestion tests.
"""

import pytest
from typing import Dict, Any
from datetime import datetime, timedelta
from unittest.mock import Mock

from src.models.ingestion_models import IngestionConfig
from src.services.ingestion_service import IngestionService
from src.services.search_service import SearchService
from src.weaviate_semantic_search import WeaviateSemanticSearch

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Test configuration with shorter timeouts for testing."""
    return {
        "weaviate": {
            "url": "http://test:8080",
            "vector_index_config": {
                "distance": "cosine",
                "efConstruction": 128,
                "maxConnections": 64
            },
            "batch_size": 100,
            "retry_attempts": 3,
            "retry_delay": 5
        },
        "app": {
            "data_dir": "test_data",
            "log_dir": "test_logs",
            "log_level": "DEBUG",
            "stale_timeout_hours": 1,
            "ingestion_wait_timeout_minutes": 5,
            "ingestion_poll_interval_seconds": 1,
            "staleness_threshold_seconds": 10
        }
    }

@pytest.fixture
def mock_weaviate_client():
    """Mock Weaviate client for testing."""
    client = Mock()
    client.get_meta.return_value = {
        "classes": [
            {"class": "Occupation", "vectorIndexConfig": {"distance": "cosine"}},
            {"class": "Skill", "vectorIndexConfig": {"distance": "cosine"}}
        ]
    }
    return client

@pytest.fixture
def mock_search_client():
    """Mock search client for testing."""
    client = Mock(spec=WeaviateSemanticSearch)
    client.validate_data.return_value = (True, "Data is valid")
    return client

@pytest.fixture
def ingestion_service(test_config, mock_weaviate_client):
    """Create an IngestionService instance with test configuration."""
    config = IngestionConfig(
        config_path="test_config.yaml",
        profile="test",
        raw_config=test_config
    )
    service = IngestionService(config)
    service.client = mock_weaviate_client
    return service

@pytest.fixture
def search_service(test_config, mock_weaviate_client, mock_search_client):
    """Create a SearchService instance with test configuration."""
    service = SearchService(test_config)
    service.client = mock_weaviate_client
    service.search = mock_search_client
    return service

@pytest.fixture
def mock_progress():
    """Create a mock progress object for testing."""
    return {
        "current_step": "Processing occupations",
        "step_number": 1,
        "total_steps": 12,
        "items_processed": 50,
        "total_items": 100,
        "started_at": datetime.utcnow().isoformat(),
        "heartbeat": datetime.utcnow().isoformat()
    }

@pytest.fixture
def mock_stale_progress():
    """Create a mock stale progress object for testing."""
    return {
        "current_step": "Processing occupations",
        "step_number": 1,
        "total_steps": 12,
        "items_processed": 50,
        "total_items": 100,
        "started_at": (datetime.utcnow() - timedelta(hours=2)).isoformat(),
        "heartbeat": (datetime.utcnow() - timedelta(hours=2)).isoformat()
    } 