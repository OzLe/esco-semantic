#!/usr/bin/env python3
import logging
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from src.weaviate_semantic_search import ESCOSemanticSearch
from src.logging_config import setup_logging

# Setup logging
logger = setup_logging()

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
    """Run a simple HTTP server for health checks"""
    server = HTTPServer(('0.0.0.0', 8000), HealthCheckHandler)
    logger.info("Health check server started on port 8000")
    server.serve_forever()

def main():
    """Run the search service"""
    try:
        # Initialize the search client
        search_client = ESCOSemanticSearch()
        
        # Validate that data is indexed
        is_valid, validation_details = search_client.validate_data()
        if not is_valid:
            logger.error(f"Data validation failed. Please ensure all required data is indexed. Validation details: {validation_details}")
            return
        
        logger.info("Search service is ready. Data validation passed.")
        logger.info(f"Indexed data: {validation_details}")
        
        # Start health check server in a separate thread
        health_check_thread = threading.Thread(target=run_health_check_server, daemon=True)
        health_check_thread.start()
        
        # Keep the service running
        while True:
            pass
            
    except Exception as e:
        logger.error(f"Error in search service: {str(e)}")
        raise

if __name__ == "__main__":
    main() 