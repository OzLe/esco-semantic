"""
Integration tests for long-running ingestion scenarios.

This module contains tests for:
- Heartbeat-based staleness detection
- Search service waiting logic
- Long-running ingestion scenarios
"""

import pytest
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from typing import Dict, Any

from src.services.ingestion_service import IngestionService
from src.services.search_service import SearchService
from src.models.ingestion_models import (
    IngestionState,
    IngestionProgress,
    IngestionConfig
)
from src.weaviate_semantic_search import WeaviateSemanticSearch

@pytest.fixture
def mock_weaviate_client():
    """Mock Weaviate client for testing"""
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
    """Mock search client for testing"""
    client = Mock(spec=WeaviateSemanticSearch)
    client.validate_data.return_value = (True, "Data is valid")
    return client

@pytest.fixture
def test_config():
    """Test configuration with shorter timeouts"""
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
            "stale_timeout_hours": 1,
            "ingestion_wait_timeout_minutes": 5,
            "ingestion_poll_interval_seconds": 1,
            "staleness_threshold_seconds": 10
        }
    }

class TestLongRunningIngestion:
    """Test suite for long-running ingestion scenarios"""

    def test_heartbeat_based_staleness_detection(self, mock_weaviate_client, test_config):
        """Test that ingestion is marked as stale when heartbeat is too old"""
        # Setup
        service = IngestionService(IngestionConfig(test_config))
        service.client = mock_weaviate_client
        
        # Mock metadata with old heartbeat
        old_time = datetime.utcnow() - timedelta(hours=2)
        mock_weaviate_client.get_meta.return_value = {
            "classes": [
                {
                    "class": "Occupation",
                    "vectorIndexConfig": {"distance": "cosine"},
                    "properties": [
                        {
                            "name": "ingestion_metadata",
                            "dataType": ["text"],
                            "description": "Ingestion metadata"
                        }
                    ]
                }
            ]
        }
        
        # Mock query result with old heartbeat
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": {
                                "state": "IN_PROGRESS",
                                "heartbeat": old_time.isoformat(),
                                "current_step": "Processing occupations"
                            }
                        }
                    ]
                }
            }
        }
        
        # Test
        is_stale = service._is_ingestion_stale()
        assert is_stale, "Ingestion should be marked as stale with old heartbeat"

    def test_search_service_waiting_logic(self, mock_weaviate_client, mock_search_client, test_config):
        """Test that search service properly waits for ingestion completion"""
        # Setup
        search_service = SearchService(test_config)
        search_service.client = mock_weaviate_client
        search_service.search = mock_search_client
        
        # Mock initial state as in progress
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": {
                                "state": "IN_PROGRESS",
                                "heartbeat": datetime.utcnow().isoformat(),
                                "current_step": "Processing occupations"
                            }
                        }
                    ]
                }
            }
        }
        
        # Mock eventual completion
        def mock_query_side_effect(*args, **kwargs):
            if mock_query_side_effect.call_count > 2:
                mock_weaviate_client.query.get.return_value = {
                    "data": {
                        "Get": {
                            "Occupation": [
                                {
                                    "ingestion_metadata": {
                                        "state": "COMPLETED",
                                        "heartbeat": datetime.utcnow().isoformat()
                                    }
                                }
                            ]
                        }
                    }
                }
            mock_query_side_effect.call_count += 1
            return mock_weaviate_client.query.get.return_value
        
        mock_query_side_effect.call_count = 0
        mock_weaviate_client.query.get.side_effect = mock_query_side_effect
        
        # Test
        with patch('time.sleep') as mock_sleep:
            search_service.wait_for_ingestion_completion()
            assert mock_sleep.call_count > 0, "Should have waited for completion"
            assert mock_search_client.validate_data.called, "Should have validated data"

    def test_long_running_ingestion_progress(self, mock_weaviate_client, test_config):
        """Test progress tracking during long-running ingestion"""
        # Setup
        service = IngestionService(IngestionConfig(test_config))
        service.client = mock_weaviate_client
        
        # Mock progress updates
        progress_updates = []
        def progress_callback(progress: IngestionProgress):
            progress_updates.append(progress)
        
        # Mock metadata updates
        def mock_meta_side_effect(*args, **kwargs):
            if len(progress_updates) > 0:
                latest_progress = progress_updates[-1]
                mock_weaviate_client.get_meta.return_value = {
                    "classes": [
                        {
                            "class": "Occupation",
                            "vectorIndexConfig": {"distance": "cosine"},
                            "properties": [
                                {
                                    "name": "ingestion_metadata",
                                    "dataType": ["text"],
                                    "description": "Ingestion metadata"
                                }
                            ]
                        }
                    ]
                }
                mock_weaviate_client.query.get.return_value = {
                    "data": {
                        "Get": {
                            "Occupation": [
                                {
                                    "ingestion_metadata": {
                                        "state": "IN_PROGRESS",
                                        "heartbeat": datetime.utcnow().isoformat(),
                                        "current_step": latest_progress.current_step,
                                        "step_number": latest_progress.step_number,
                                        "total_steps": latest_progress.total_steps,
                                        "items_processed": latest_progress.items_processed,
                                        "total_items": latest_progress.total_items
                                    }
                                }
                            ]
                        }
                    }
                }
            return mock_weaviate_client.get_meta.return_value
        
        mock_weaviate_client.get_meta.side_effect = mock_meta_side_effect
        
        # Test
        with patch('time.sleep') as mock_sleep:
            service.run_ingestion(progress_callback=progress_callback)
            assert len(progress_updates) > 0, "Should have received progress updates"
            assert any(p.step_progress_percentage > 0 for p in progress_updates), "Should have non-zero progress"

    def test_ingestion_timeout_handling(self, mock_weaviate_client, test_config):
        """Test handling of ingestion timeouts"""
        # Setup
        service = IngestionService(IngestionConfig(test_config))
        service.client = mock_weaviate_client
        
        # Mock stuck ingestion
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": {
                                "state": "IN_PROGRESS",
                                "heartbeat": datetime.utcnow().isoformat(),
                                "current_step": "Processing occupations"
                            }
                        }
                    ]
                }
            }
        }
        
        # Test
        with pytest.raises(TimeoutError):
            with patch('time.sleep') as mock_sleep:
                service.wait_for_ingestion_completion(timeout_minutes=1)

    def test_search_service_validation_during_ingestion(self, mock_weaviate_client, mock_search_client, test_config):
        """Test search service validation behavior during ingestion"""
        # Setup
        search_service = SearchService(test_config)
        search_service.client = mock_weaviate_client
        search_service.search = mock_search_client
        
        # Mock in-progress ingestion
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": {
                                "state": "IN_PROGRESS",
                                "heartbeat": datetime.utcnow().isoformat(),
                                "current_step": "Processing occupations"
                            }
                        }
                    ]
                }
            }
        }
        
        # Test
        is_valid, details = search_service.validate_data()
        assert not is_valid, "Should not be valid during ingestion"
        assert "ingestion in progress" in details.lower(), "Should indicate ingestion in progress" 