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
        logger.info(f"Initializing ingestion check with config: {config_path}, profile: {profile}")
        
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
        logger.info(f"Current ingestion state: {current_state.value}")
        
        # Handle completed state
        if current_state == IngestionState.COMPLETED:
            logger.info("Data already ingested, skipping...")
            return 0
        
        # Handle in-progress state  
        elif current_state == IngestionState.IN_PROGRESS:
            logger.info("Ingestion in progress, waiting...")
            return 1
        
        # Handle other states - need to run ingestion
        else:  # NOT_STARTED, FAILED, UNKNOWN
            logger.info(f"Initial state is '{current_state.value}'. Starting new ingestion...")
            
            # Check if we should run ingestion (handles non-interactive mode logic)
            decision = service.should_run_ingestion(force_reingest=False)
            
            if not decision.should_run:
                logger.warning(f"Ingestion decision: {decision.reason}")
                if decision.force_required:
                    logger.info("Use --force-reingest in CLI to override this decision.")
                    return 2  # Needs manual intervention
                return 2  # Needs ingestion but can't proceed
            
            # Validate prerequisites before starting
            logger.info("Validating prerequisites...")
            validation = service.validate_prerequisites()
            
            if not validation.is_valid:
                logger.error("Prerequisites validation failed:")
                for error in validation.errors:
                    logger.error(f"  • {error}")
                return 2
            
            if validation.warnings:
                logger.warning("Prerequisites validation warnings:")
                for warning in validation.warnings:
                    logger.warning(f"  • {warning}")
            
            # Run ingestion using the service layer
            logger.info("Running ingestion via service layer...")
            result = service.run_ingestion()
            
            # Check result
            if result.success:
                logger.info(f"Ingestion completed successfully in {result.duration:.1f} seconds")
                
                # Verify completion
                logger.info("Verifying ingestion completion...")
                verification = service.verify_completion()
                
                if verification.is_valid:
                    logger.info("Post-ingestion verification: Status is COMPLETED")
                    
                    # Log metrics
                    metrics = service.get_ingestion_metrics()
                    if metrics.get('entity_counts'):
                        logger.info("Entity counts:")
                        for class_name, count in metrics['entity_counts'].items():
                            if count > 0:
                                logger.info(f"  • {class_name}: {count}")
                    
                    return 0
                else:
                    logger.error("Post-ingestion verification failed:")
                    for error in verification.errors:
                        logger.error(f"  • {error}")
                    return 3  # Verification failed
            else:
                logger.error("Ingestion failed:")
                for error in result.errors:
                    logger.error(f"  • {error}")
                return 3  # Ingestion failed
            
    except Exception as e:
        log_error(logger, e, {'operation': 'main', 'config_path': config_path, 'profile': profile})
        return 2  # General error
    finally:
        if service is not None:
            service.close()


if __name__ == "__main__":
    # Parse command line arguments (same interface as before)
    config_path = "config/weaviate_config.yaml"
    profile = "default"
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    if len(sys.argv) > 2:
        profile = sys.argv[2]
    
    exit_code = main(config_path, profile)
    
    # Handle retry logic for in-progress ingestion (same logic as before)
    if exit_code == 1:
        logger.info("Waiting for in-progress ingestion...")
        time.sleep(3800)
        sys.exit(1)  # Signal to retry
        
    sys.exit(exit_code) 