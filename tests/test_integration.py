"""
Integration tests for the ESCO ingestion and search system.

This module contains tests for:
- Cold start behavior
- Warm start behavior
- Interruption and recovery
- Search service startup
"""

import os
import time
import pytest
import requests
import subprocess
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from src.services.ingestion_service import IngestionService
from src.services.search_service import SearchService
from src.models.ingestion_models import IngestionConfig

class TestIntegration:
    """Integration test suite for the ESCO system."""

    @pytest.fixture(scope="class")
    def docker_compose(self):
        """Start and stop Docker Compose services for testing."""
        # Start services
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        time.sleep(10)  # Wait for services to start
        
        yield
        
        # Stop services
        subprocess.run(["docker-compose", "down"], check=True)

    @pytest.fixture
    def test_config(self) -> Dict[str, Any]:
        """Test configuration with shorter timeouts."""
        return {
            "weaviate": {
                "url": "http://localhost:8080",
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

    def test_cold_start(self, docker_compose, test_config):
        """Test cold start behavior with long ingestion."""
        # Start with clean state
        subprocess.run(["docker-compose", "down", "-v"], check=True)
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        
        # Wait for Weaviate to be ready
        self._wait_for_weaviate()
        
        # Start ingestion
        ingestion_service = IngestionService(IngestionConfig(test_config))
        result = ingestion_service.run_ingestion()
        
        assert result.success, "Ingestion should complete successfully"
        
        # Verify search service starts
        self._wait_for_search_service()
        
        # Test search functionality
        response = requests.get("http://localhost:8000/health")
        assert response.status_code == 200, "Search service should be healthy"

    def test_warm_start(self, docker_compose, test_config):
        """Test warm start behavior (skipping ingestion)."""
        # Start services
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        
        # Wait for Weaviate to be ready
        self._wait_for_weaviate()
        
        # Check ingestion status
        ingestion_service = IngestionService(IngestionConfig(test_config))
        decision = ingestion_service.should_run_ingestion()
        
        assert not decision.should_run, "Should not run ingestion on warm start"
        assert "already completed" in decision.reason.lower()
        
        # Verify search service starts immediately
        self._wait_for_search_service()
        
        # Test search functionality
        response = requests.get("http://localhost:8000/health")
        assert response.status_code == 200, "Search service should be healthy"

    def test_interruption_recovery(self, docker_compose, test_config):
        """Test interruption and recovery of ingestion."""
        # Start with clean state
        subprocess.run(["docker-compose", "down", "-v"], check=True)
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        
        # Wait for Weaviate to be ready
        self._wait_for_weaviate()
        
        # Start ingestion
        ingestion_service = IngestionService(IngestionConfig(test_config))
        
        # Simulate interruption
        subprocess.run(["docker-compose", "stop", "init-container"], check=True)
        time.sleep(5)
        
        # Restart container
        subprocess.run(["docker-compose", "start", "init-container"], check=True)
        
        # Wait for recovery
        time.sleep(10)
        
        # Check status
        decision = ingestion_service.should_run_ingestion()
        assert decision.should_run, "Should detect interrupted ingestion"
        assert "stale" in decision.reason.lower()
        
        # Complete ingestion
        result = ingestion_service.run_ingestion()
        assert result.success, "Should complete ingestion after recovery"

    def test_search_service_waiting(self, docker_compose, test_config):
        """Test search service waiting behavior during ingestion."""
        # Start with clean state
        subprocess.run(["docker-compose", "down", "-v"], check=True)
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        
        # Wait for Weaviate to be ready
        self._wait_for_weaviate()
        
        # Start ingestion
        ingestion_service = IngestionService(IngestionConfig(test_config))
        
        # Start search service
        search_service = SearchService(test_config)
        
        # Verify search service waits
        start_time = datetime.utcnow()
        is_valid, _ = search_service.validate_data()
        wait_time = (datetime.utcnow() - start_time).total_seconds()
        
        assert not is_valid, "Search service should not be valid during ingestion"
        assert wait_time >= 1, "Search service should wait for ingestion"
        
        # Complete ingestion
        result = ingestion_service.run_ingestion()
        assert result.success, "Should complete ingestion"
        
        # Verify search service becomes valid
        is_valid, _ = search_service.validate_data()
        assert is_valid, "Search service should be valid after ingestion"

    def _wait_for_weaviate(self, timeout: int = 60) -> None:
        """Wait for Weaviate to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get("http://localhost:8080/v1/.well-known/ready")
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(1)
        raise TimeoutError("Weaviate failed to start")

    def _wait_for_search_service(self, timeout: int = 60) -> None:
        """Wait for search service to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get("http://localhost:8000/health")
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(1)
        raise TimeoutError("Search service failed to start") 