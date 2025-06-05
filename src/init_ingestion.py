#!/usr/bin/env python3
"""
Initialization script for ESCO data ingestion.
This script handles the initialization and monitoring of the ingestion process,
ensuring proper status tracking and error handling.

This module now uses the Service Layer pattern instead of duplicating business logic.
"""

import sys
import os
import time
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from .services.ingestion_service import IngestionService
from .models.ingestion_models import (
    IngestionConfig,
    IngestionState,
    IngestionDecision,
    IngestionResult,
    ValidationResult
)
from .logging_config import setup_logging, log_error
from .exceptions import WeaviateError

logger = setup_logging()


def main(config_path: str = "config/weaviate_config.yaml", profile: str = "default") -> int:
    """
    Main function to handle ingestion initialization using the service layer.
    
    Args:
        config_path: Path to configuration file
        profile: Configuration profile to use
        
    Returns:
        int: Exit code (0: success, 1: in progress, 2: needs ingestion, 3: verification failed)
    """
    service = None
    try:
        # Create ingestion configuration for container mode
        ingestion_config = IngestionConfig(
            config_path=config_path,
            profile=profile,
            force_reingest=False,  # Container mode never forces reingest
            non_interactive=True,  # Container mode is always non-interactive
            docker_env=True  # This is running in container context
        )
        
        # Initialize service
        service = IngestionService(ingestion_config)
        
        # Get current state
        current_state = service.get_current_state()
        
        # Handle completed state
        if current_state == IngestionState.COMPLETED:
            logger.info("[init_container] Data already available, initialization complete")
            return 0
        
        # Handle in-progress state  
        elif current_state == IngestionState.IN_PROGRESS:
            logger.info("[init_container] Detected ongoing ingestion, monitoring progress...")
            return 1
        
        # Handle other states - need to run ingestion
        else:  # NOT_STARTED, FAILED, UNKNOWN
            logger.info(f"[init_container] State '{current_state.value}' requires new ingestion")
            
            # Check if we should run ingestion (handles non-interactive mode logic)
            decision = service.should_run_ingestion(force_reingest=False)
            
            if not decision.should_run:
                logger.warning(f"[init_container] Cannot proceed: {decision.reason}")
                if decision.force_required:
                    logger.info("[init_container] Manual intervention required (--force-reingest)")
                    return 2  # Needs manual intervention
                return 2  # Needs ingestion but can't proceed
            
            # Validate prerequisites before starting
            logger.info("[init_container] Validating prerequisites...")
            validation = service.validate_prerequisites()
            
            if not validation.is_valid:
                logger.error("[init_container] Prerequisites validation failed:")
                for error in validation.errors:
                    logger.error(f"  • {error}")
                return 2
            
            if validation.warnings:
                logger.warning("[init_container] Prerequisites validation warnings:")
                for warning in validation.warnings:
                    logger.warning(f"  • {warning}")
            
            # Run ingestion using the service layer
            logger.info("[init_container] Starting ingestion process...")
            result = service.run_ingestion()
            
            if result.success:
                logger.info(f"[init_container] Ingestion completed successfully in {result.duration:.1f}s")
                return 0
            else:
                logger.error(f"[init_container] Ingestion failed: {'; '.join(result.errors)}")
                return 3
                
    except Exception as e:
        error_msg = f"[init_container] Initialization error: {str(e)}"
        logger.error(error_msg)
        if service:
            try:
                service.close()
            except:
                pass
        return 3
    
    finally:
        # Clean up service resources
        if service:
            try:
                service.close()
            except Exception as cleanup_error:
                logger.warning(f"[init_container] Cleanup warning: {str(cleanup_error)}")


if __name__ == "__main__":
    # Parse command line arguments (same interface as before)
    config_path = "config/weaviate_config.yaml"
    profile = "default"
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    if len(sys.argv) > 2:
        profile = sys.argv[2]
    
    exit_code = main(config_path, profile)
    
    # Handle retry logic for in-progress ingestion with reduced sleep time
    if exit_code == 1:
        # Reduced sleep time and simplified message
        logger.info("[init_container] Deferring to ongoing ingestion process")
        time.sleep(30)  # Reduced from 3800 to 30 seconds
        sys.exit(1)  # Signal to retry
        
    sys.exit(exit_code) 