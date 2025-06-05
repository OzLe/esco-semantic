#!/usr/bin/env python3
import logging
import os
import time
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from src.weaviate_semantic_search import ESCOSemanticSearch
from src.exceptions import SearchError, DataValidationError
from src.logging_config import setup_logging, log_error
from src.models.ingestion_models import IngestionState
import yaml
from typing import Dict, Any, List, Optional, Tuple
from .esco_weaviate_client import WeaviateClient
from .weaviate_semantic_search import ESCOSemanticSearch

# Setup logging
logger = setup_logging()

# Constants
DEFAULT_TIMEOUT_MINUTES = 60
POLL_INTERVAL_SECONDS = 30
LOG_INTERVAL_SECONDS = 300  # Log progress every 5 minutes to reduce spam

class SearchService:
    """Service layer for ESCO search operations."""
    
    def __init__(self, config_path: str, profile: str = "default"):
        """
        Initialize the search service.
        
        Args:
            config_path: Path to the configuration file
            profile: Configuration profile to use
        """
        self.config_path = config_path
        self.profile = profile
        self._client: Optional[WeaviateClient] = None
        self._search: Optional[ESCOSemanticSearch] = None
        self._config: Dict[str, Any] = {}
        self._last_log_time: Optional[float] = None
        
        # Load configuration
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            if not isinstance(raw_config, dict):
                raise ValueError("Invalid config file format")
            
            if self.profile not in raw_config:
                raise ValueError(f"Profile '{self.profile}' not found in config file")
            
            self._config = raw_config[self.profile]
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    @property
    def client(self) -> WeaviateClient:
        """Get or create WeaviateClient instance."""
        if self._client is None:
            self._client = WeaviateClient(self.config_path, self.profile)
        return self._client
    
    @property
    def search(self) -> ESCOSemanticSearch:
        """Get or create ESCOSemanticSearch instance."""
        if self._search is None:
            self._search = ESCOSemanticSearch(self.config_path, self.profile)
        return self._search
    
    def _should_log_progress(self) -> bool:
        """Check if enough time has passed to log progress update."""
        current_time = time.time()
        if self._last_log_time is None or (current_time - self._last_log_time) >= LOG_INTERVAL_SECONDS:
            self._last_log_time = current_time
            return True
        return False
    
    def wait_for_ingestion_completion(self, timeout_minutes: Optional[int] = None, log_component: str = "search_service") -> bool:
        """
        Wait for ingestion to complete.
        
        Args:
            timeout_minutes: Maximum time to wait (defaults to config value)
            log_component: Component name for contextual logging
        
        Returns:
            bool: True if ingestion completed successfully, False if timed out
        """
        # Get timeout configuration with defaults
        if timeout_minutes is None:
            timeout_minutes = self._config.get('app', {}).get('ingestion_wait_timeout_minutes', DEFAULT_TIMEOUT_MINUTES)
        poll_interval = self._config.get('app', {}).get('ingestion_poll_interval_seconds', POLL_INTERVAL_SECONDS)
        
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        
        # Log once at start with context
        logger.info(f"[{log_component}] Monitoring ingestion completion (timeout: {timeout_minutes}min, poll: {poll_interval}s)")
        
        while True:
            # Check if we've exceeded the timeout
            if time.time() - start_time > timeout_seconds:
                logger.warning(f"[{log_component}] Ingestion wait timeout after {timeout_minutes} minutes")
                return False
            
            # Get current ingestion state
            try:
                status_data = self.client.get_ingestion_status()
                current_status = status_data.get('status')
                
                if current_status == 'completed':
                    logger.info(f"[{log_component}] Ingestion completed successfully")
                    return True
                elif current_status == 'failed':
                    logger.error(f"[{log_component}] Ingestion failed")
                    return False
                elif current_status == 'in_progress':
                    # Check if ingestion is stale
                    details = status_data.get('details', {})
                    heartbeat_str = details.get('last_heartbeat')
                    
                    if heartbeat_str:
                        try:
                            heartbeat_time = datetime.fromisoformat(heartbeat_str)
                            age_seconds = (datetime.utcnow() - heartbeat_time).total_seconds()
                            staleness_threshold = self._config.get('app', {}).get('staleness_threshold_seconds', 7200)
                            
                            if age_seconds > staleness_threshold:
                                logger.warning(f"[{log_component}] Ingestion stale (heartbeat age: {age_seconds:.0f}s > {staleness_threshold}s)")
                                return False
                        except ValueError as e:
                            logger.warning(f"[{log_component}] Invalid heartbeat timestamp: {str(e)}")
                    
                    # Periodic progress logging (reduced frequency)
                    if self._should_log_progress() and 'current_step' in details and 'step_number' in details:
                        step_info = f"{details['current_step']} ({details['step_number']}/12)"
                        logger.info(f"[{log_component}] Ingestion progress: {step_info}")
                
            except Exception as e:
                logger.warning(f"[{log_component}] Status check error: {str(e)}")
            
            # Wait before next check
            time.sleep(poll_interval)
    
    def validate_data(self) -> Tuple[bool, str]:
        """
        Validate that data is available and ready for search.
        
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        try:
            # First check if ingestion is in progress
            status_data = self.client.get_ingestion_status()
            current_status = status_data.get('status')
            
            if current_status == 'in_progress':
                # Wait for completion with search service context
                if self.wait_for_ingestion_completion(log_component="search_validation"):
                    # Ingestion completed successfully, proceed with validation
                    pass
                else:
                    return False, "Ingestion timeout during validation"
            
            # Now validate the data
            return self.search.validate_data()
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            return False, f"Validation error: {str(e)}"
    
    def semantic_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Perform semantic search.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List[Dict[str, Any]]: Search results
        """
        # Validate data before search
        is_valid, message = self.validate_data()
        if not is_valid:
            raise ValueError(f"Data validation failed: {message}")
        
        return self.search.semantic_search(query, limit)
    
    def close(self) -> None:
        """Clean up resources."""
        if self._search:
            self._search.close()
        if self._client:
            # Client cleanup if needed
            pass

# Unified ingestion wait function for standalone use
def wait_for_ingestion_completion(
    search_client: ESCOSemanticSearch, 
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES,
    log_component: str = "standalone"
) -> bool:
    """
    Wait for ingestion to complete using a search client instance.
    
    Args:
        search_client: The search client instance
        timeout_minutes: Maximum time to wait in minutes
        log_component: Component name for contextual logging
        
    Returns:
        bool: True if ingestion completed successfully, False if timed out or failed
        
    Raises:
        SearchError: If ingestion failed or other error occurred
    """
    start_time = datetime.utcnow()
    timeout_delta = timedelta(minutes=timeout_minutes)
    last_log_time = None
    
    # Single initial log with context
    logger.info(f"[{log_component}] Monitoring ingestion (timeout: {timeout_minutes}min)")
    
    while True:
        try:
            # Check if we've exceeded the timeout
            if datetime.utcnow() - start_time > timeout_delta:
                error_msg = f"[{log_component}] Timeout after {timeout_minutes} minutes"
                logger.error(error_msg)
                return False
            
            # Get current ingestion status
            status_data = search_client.client.get_ingestion_status()
            current_status = status_data.get('status', 'unknown')
            
            # Map status to IngestionState
            status_mapping = {
                'not_started': IngestionState.NOT_STARTED,
                'in_progress': IngestionState.IN_PROGRESS,
                'completed': IngestionState.COMPLETED,
                'failed': IngestionState.FAILED,
                'unknown': IngestionState.UNKNOWN
            }
            
            current_state = status_mapping.get(current_status, IngestionState.UNKNOWN)
            
            # Handle different states
            if current_state == IngestionState.COMPLETED:
                logger.info(f"[{log_component}] Ingestion completed")
                return True
                
            elif current_state == IngestionState.FAILED:
                error_msg = f"[{log_component}] Ingestion failed: {status_data.get('details', {}).get('error', 'Unknown error')}"
                logger.error(error_msg)
                raise SearchError(error_msg)
                
            elif current_state == IngestionState.IN_PROGRESS:
                # Reduced frequency progress logging
                current_time = datetime.utcnow()
                should_log = (last_log_time is None or 
                             (current_time - last_log_time).seconds >= LOG_INTERVAL_SECONDS)
                
                if should_log:
                    details = status_data.get('details', {})
                    if 'current_step' in details and 'step_number' in details:
                        step_info = f"{details['current_step']} ({details['step_number']}/12)"
                        logger.info(f"[{log_component}] Progress: {step_info}")
                    else:
                        logger.info(f"[{log_component}] Ingestion in progress...")
                    last_log_time = current_time
                
            else:
                # Only log unknown states periodically
                current_time = datetime.utcnow()
                if (last_log_time is None or 
                    (current_time - last_log_time).seconds >= LOG_INTERVAL_SECONDS):
                    logger.info(f"[{log_component}] Status: {current_status}")
                    last_log_time = current_time
            
            # Wait before next check
            time.sleep(POLL_INTERVAL_SECONDS)
            
        except Exception as e:
            error_msg = f"[{log_component}] Status check error: {str(e)}"
            logger.error(error_msg)
            raise SearchError(error_msg)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status": "healthy"}')
        else:
            self.send_response(404)
            self.end_headers()

def run_health_check_server():
    """Run a simple health check server"""
    try:
        # Implementation of health check server
        server = HTTPServer(('0.0.0.0', 8000), HealthCheckHandler)
        logger.info("Health check server started on port 8000")
        server.serve_forever()
    except Exception as e:
        log_error(logger, e, {'operation': 'health_check_server'})
        raise SearchError(f"Health check server failed: {str(e)}")

def main():
    """Run the search service"""
    try:
        # Initialize the search client
        search_client = ESCOSemanticSearch()
        
        # Wait for ingestion to complete with search service context
        if not wait_for_ingestion_completion(search_client, log_component="search_service"):
            error_msg = "Search service startup failed: ingestion timeout"
            logger.error(error_msg)
            raise SearchError(error_msg)
        
        # Validate that data is indexed
        is_valid, validation_details = search_client.validate_data()
        if not is_valid:
            error_msg = f"Search service startup failed: data validation. Details: {validation_details}"
            logger.error(error_msg)
            return
        
        logger.info("Search service ready")
        logger.info(f"Available data: {validation_details}")
        
        # Start health check server in a separate thread
        health_check_thread = threading.Thread(target=run_health_check_server, daemon=True)
        health_check_thread.start()
        
        # Keep the service running
        while True:
            pass
            
    except Exception as e:
        log_error(logger, e, {'operation': 'search_service_main'})
        raise SearchError(f"Search service error: {str(e)}")

if __name__ == "__main__":
    main() 