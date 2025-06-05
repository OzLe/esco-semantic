"""
Tests for the ingestion service layer.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import time

from src.services.ingestion_service import IngestionService
from src.models.ingestion_models import (
    IngestionState,
    IngestionDecision,
    IngestionProgress,
    IngestionResult,
    ValidationResult
)

class TestIngestionService:
    """Test suite for the ingestion service."""

    def test_should_run_ingestion_when_not_started(self, ingestion_service, mock_weaviate_client):
        """Test that ingestion should run when not started."""
        # Mock empty metadata
        mock_weaviate_client.query.get.return_value = {
            "data": {"Get": {"Occupation": []}}
        }
        
        decision = ingestion_service.should_run_ingestion()
        assert decision.should_run
        assert decision.current_state == IngestionState.NOT_STARTED
        assert "No existing data" in decision.reason

    def test_should_not_run_ingestion_when_completed(self, ingestion_service, mock_weaviate_client):
        """Test that ingestion should not run when already completed."""
        # Mock completed metadata
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
        
        decision = ingestion_service.should_run_ingestion()
        assert not decision.should_run
        assert decision.current_state == IngestionState.COMPLETED
        assert "already completed" in decision.reason

    def test_should_run_ingestion_when_stale(self, ingestion_service, mock_weaviate_client, mock_stale_progress):
        """Test that ingestion should run when previous run is stale."""
        # Mock stale metadata
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": mock_stale_progress
                        }
                    ]
                }
            }
        }
        
        decision = ingestion_service.should_run_ingestion()
        assert decision.should_run
        assert decision.current_state == IngestionState.IN_PROGRESS
        assert "stale" in decision.reason.lower()

    def test_should_not_run_ingestion_when_active(self, ingestion_service, mock_weaviate_client, mock_progress):
        """Test that ingestion should not run when active."""
        # Mock active metadata
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": mock_progress
                        }
                    ]
                }
            }
        }
        
        decision = ingestion_service.should_run_ingestion()
        assert not decision.should_run
        assert decision.current_state == IngestionState.IN_PROGRESS
        assert "in progress" in decision.reason.lower()

    def test_force_reingest_overrides_checks(self, ingestion_service, mock_weaviate_client, mock_progress):
        """Test that force_reingest overrides all checks."""
        # Mock active metadata
        mock_weaviate_client.query.get.return_value = {
            "data": {
                "Get": {
                    "Occupation": [
                        {
                            "ingestion_metadata": mock_progress
                        }
                    ]
                }
            }
        }
        
        # Set force_reingest
        ingestion_service.config.force_reingest = True
        
        decision = ingestion_service.should_run_ingestion()
        assert decision.should_run
        assert "forced" in decision.reason.lower()

    def test_validate_prerequisites(self, ingestion_service, mock_weaviate_client):
        """Test prerequisite validation."""
        # Mock successful schema check
        mock_weaviate_client.ensure_schema.return_value = True
        
        validation = ingestion_service.validate_prerequisites()
        assert validation.is_valid
        assert "weaviate_connectivity" in validation.checks_performed

    def test_validate_prerequisites_fails(self, ingestion_service, mock_weaviate_client):
        """Test prerequisite validation failure."""
        # Mock failed schema check
        mock_weaviate_client.ensure_schema.side_effect = Exception("Connection failed")
        
        validation = ingestion_service.validate_prerequisites()
        assert not validation.is_valid
        assert any("connection failed" in error.lower() for error in validation.errors)

    def test_run_ingestion_progress_tracking(self, ingestion_service, mock_weaviate_client):
        """Test progress tracking during ingestion."""
        # Track progress updates
        progress_updates = []
        def progress_callback(progress: IngestionProgress):
            progress_updates.append(progress)
        
        # Mock successful ingestion
        with patch.object(ingestion_service, '_step_initialization') as mock_init:
            mock_init.return_value = True
            result = ingestion_service.run_ingestion(progress_callback=progress_callback)
        
        assert result.success
        assert len(progress_updates) > 0
        assert any(p.step_progress_percentage > 0 for p in progress_updates)

    def test_run_ingestion_handles_failure(self, ingestion_service, mock_weaviate_client):
        """Test handling of ingestion failure."""
        # Mock failed step
        with patch.object(ingestion_service, '_step_initialization') as mock_init:
            mock_init.side_effect = Exception("Step failed")
            result = ingestion_service.run_ingestion()
        
        assert not result.success
        assert any("step failed" in error.lower() for error in result.errors)

    def test_heartbeat_updates_during_ingestion(self, ingestion_service, mock_weaviate_client):
        """Test that heartbeats are updated during ingestion."""
        # Track metadata updates
        metadata_updates = []
        def mock_update_meta(*args, **kwargs):
            metadata_updates.append(kwargs.get('data', {}))
            return True
        
        mock_weaviate_client.data_object.update.side_effect = mock_update_meta
        
        # Run a step
        with patch.object(ingestion_service, '_step_initialization') as mock_init:
            mock_init.return_value = True
            ingestion_service.run_ingestion()
        
        # Verify heartbeat updates
        assert len(metadata_updates) > 0
        assert all('heartbeat' in update.get('ingestion_metadata', {}) 
                  for update in metadata_updates)

    def test_ingestion_timeout_handling(self, ingestion_service, mock_weaviate_client):
        """Test handling of ingestion timeouts."""
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
        
        # Test timeout
        with pytest.raises(TimeoutError):
            with patch('time.sleep') as mock_sleep:
                ingestion_service.wait_for_ingestion_completion(timeout_minutes=1)

    def test_ingestion_progress_estimation(self, ingestion_service, mock_weaviate_client):
        """Test progress and ETA estimation during ingestion."""
        # Track progress updates
        progress_updates = []
        def progress_callback(progress: IngestionProgress):
            progress_updates.append(progress)
        
        # Mock step with known duration
        with patch.object(ingestion_service, '_step_initialization') as mock_init:
            def mock_step():
                time.sleep(0.1)  # Simulate work
                return True
            mock_init.side_effect = mock_step
            result = ingestion_service.run_ingestion(progress_callback=progress_callback)
        
        assert result.success
        assert len(progress_updates) > 0
        assert any(p.estimated_time_remaining is not None for p in progress_updates)
        assert any(p.step_progress_percentage > 0 for p in progress_updates) 