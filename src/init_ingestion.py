#!/usr/bin/env python3
"""
Initialization script for ESCO data ingestion.
This script handles the initialization and monitoring of the ingestion process,
ensuring proper status tracking and error handling.
"""

import sys
import os
import time
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from .esco_weaviate_client import WeaviateClient
from .logging_config import setup_logging, log_error
from .exceptions import WeaviateError

logger = setup_logging()

def check_ingestion_status(client: WeaviateClient) -> Dict[str, Any]:
    """
    Check the current ingestion status.
    
    Args:
        client: Initialized WeaviateClient instance
        
    Returns:
        Dict containing status information
    """
    try:
        status = client.get_ingestion_status()
        logger.info(f"Current ingestion status: {status.get('status')}")
        return status
    except Exception as e:
        log_error(logger, e, {'operation': 'check_ingestion_status'})
        raise

def verify_post_ingestion(client: WeaviateClient) -> bool:
    """
    Verify the ingestion status after completion.
    
    Args:
        client: Initialized WeaviateClient instance
        
    Returns:
        bool: True if verification successful, False otherwise
    """
    try:
        final_status = client.get_ingestion_status()
        current_status = final_status.get('status')
        
        if current_status == 'completed':
            logger.info("Post-ingestion verification: Status is COMPLETED")
            return True
            
        logger.warning(f"Post-ingestion verification: Status is '{current_status}' (expected 'completed')")
        
        # Update status to failed if not already
        if current_status != 'failed':
            details = {
                'reason': 'Post-ingestion verification check failed',
                'final_status_observed': current_status
            }
            
            # Safely handle previous details
            previous_details = final_status.get('details')
            if isinstance(previous_details, str):
                try:
                    details['previous_status_details'] = json.loads(previous_details)
                except json.JSONDecodeError:
                    details['previous_status_details_raw'] = previous_details
            elif isinstance(previous_details, dict):
                details['previous_status_details'] = previous_details
                
            client.set_ingestion_metadata(status='failed', details=details)
            logger.info("Updated status to 'failed'")
            
        return False
        
    except Exception as e:
        log_error(logger, e, {'operation': 'verify_post_ingestion'})
        try:
            client.set_ingestion_metadata(
                status='failed',
                details={'reason': 'Exception during post-ingestion verification', 'error': str(e)}
            )
            logger.info("Attempted to set status to 'failed' after verification error")
        except Exception as set_status_e:
            log_error(logger, set_status_e, {'operation': 'set_failed_status'})
        return False

def main(config_path: str = "config/weaviate_config.yaml", profile: str = "default") -> int:
    """
    Main function to handle ingestion initialization.
    
    Args:
        config_path: Path to configuration file
        profile: Configuration profile to use
        
    Returns:
        int: Exit code (0: success, 1: in progress, 2: needs ingestion, 3: verification failed)
    """
    try:
        # Initialize client and ensure schema exists
        client = WeaviateClient(config_path, profile)
        client.ensure_schema()
        
        # Check current status
        status = check_ingestion_status(client)
        current_status = status.get('status')
        
        if current_status == 'completed':
            logger.info("Data already ingested, skipping...")
            return 0
        elif current_status == 'in_progress':
            logger.info("Ingestion in progress, waiting...")
            return 1
        else:  # 'not_started', 'failed', 'unknown'
            logger.info(f"Initial status is '{current_status}'. Starting new ingestion...")
            
            # Run ingestion
            logger.info("Running ingestion...")
            os.system(f"python -m src.esco_cli ingest --config {config_path} --profile {profile}")
            
            # Verify ingestion
            logger.info("Ingestion command finished. Verifying status...")
            if verify_post_ingestion(client):
                return 0
            return 3
            
    except Exception as e:
        log_error(logger, e, {'operation': 'main'})
        return 2

if __name__ == "__main__":
    # Parse command line arguments
    config_path = "config/weaviate_config.yaml"
    profile = "default"
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    if len(sys.argv) > 2:
        profile = sys.argv[2]
        
    exit_code = main(config_path, profile)
    
    # Handle retry logic for in-progress ingestion
    if exit_code == 1:
        logger.info("Waiting for in-progress ingestion...")
        time.sleep(30)
        sys.exit(1)  # Signal to retry
        
    sys.exit(exit_code) 