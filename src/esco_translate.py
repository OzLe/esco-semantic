#!/usr/bin/env python3
import argparse
import logging
from typing import List, Optional, Dict
from tqdm import tqdm
import torch
from concurrent.futures import ThreadPoolExecutor
import gc
import time
from functools import lru_cache
import platform
import importlib
import yaml
import os
import glob
import json
from datetime import datetime
from src.weaviate_client import WeaviateClient
from transformers import MarianMTModel, MarianTokenizer, AutoTokenizer, AutoModelForSeq2SeqLM
from src.logging_config import setup_logging, log_error
from pathlib import Path
from .exceptions import TranslationError, ModelError

# Setup logging
logger = setup_logging()

def verify_dependencies():
    """Verify that all required dependencies are installed."""
    try:
        import transformers
        import torch
    except ImportError as e:
        raise ModelError(f"Missing required dependency: {str(e)}")

def get_device():
    """Get the best available device for PyTorch operations."""
    if torch.backends.mps.is_available():
        return "mps"
    elif torch.cuda.is_available():
        return "cuda"
    return "cpu"

class ESCOTranslator:
    def __init__(self, config_path=None, profile='default', device: str = None):
        # Verify dependencies first
        verify_dependencies()
        
        # Load configuration
        if config_path is None:
            config_path = os.path.join('config', 'weaviate_config.yaml')
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Weaviate client
        self.client = WeaviateClient(config_path, profile)
        
        # Determine device
        if device is None:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)
        
        logger.info(f"Using device: {self.device}")
        
        # Clear any existing CUDA cache
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        # Set up model cache directory
        self.cache_dir = os.path.abspath("./model_cache")
        if not os.path.exists(self.cache_dir):
            raise RuntimeError(
                f"Model cache directory not found: {self.cache_dir}\n"
                "Please run 'python src/download_model.py' first to download the model."
            )
        
        # Find the actual model directory
        model_dirs = glob.glob(os.path.join(self.cache_dir, "**", "models--Helsinki-NLP--opus-mt-en-he"), recursive=True)
        if not model_dirs:
            raise RuntimeError(
                "Could not find model directory in cache.\n"
                "Please run 'python src/download_model.py' to download the model."
            )
        
        model_dir = model_dirs[0]
        logger.info(f"Found model directory: {model_dir}")
        
        # Find the latest snapshot directory
        snapshot_dirs = glob.glob(os.path.join(model_dir, "snapshots", "*"))
        if not snapshot_dirs:
            raise RuntimeError(
                "No snapshot directories found in model cache.\n"
                "Please run 'python src/download_model.py' to download the model."
            )
        
        # Use the first snapshot directory (they should be equivalent)
        self.model_dir = snapshot_dirs[0]
        logger.info(f"Using model directory: {self.model_dir}")
        
        # Load model and tokenizer with safe initialization
        try:
            # Use MarianMT model for English to Hebrew translation
            logger.info("Loading tokenizer from local cache...")
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(
                    self.model_dir,
                    use_fast=False,
                    local_files_only=True
                )
            except Exception as e:
                log_error(logger, e, {'operation': 'load_tokenizer'})
                raise ModelError(
                    "Failed to load tokenizer from cache. "
                    "Please run 'python src/download_model.py' to download the model."
                )
            
            logger.info("Loading model from local cache...")
            try:
                self.model = AutoModelForSeq2SeqLM.from_pretrained(
                    self.model_dir,
                    local_files_only=True
                ).to(self.device)
            except Exception as e:
                log_error(logger, e, {'operation': 'load_model'})
                raise ModelError(
                    "Failed to load model from cache. "
                    "Please run 'python src/download_model.py' to download the model."
                )
            
            # Set model to evaluation mode
            self.model.eval()
            
            # Run smoke test with a small batch
            logger.info("Running smoke test...")
            test_input = "Hello, how are you?"
            test_output = self.translate_text(test_input)
            if not test_output:
                raise ValueError("Smoke test failed - translation returned empty result")
            logger.info(f"Smoke test successful. Translated: {test_output}")
            
        except Exception as e:
            log_error(logger, e, {'operation': 'model_initialization'})
            raise ModelError(f"Failed to initialize translation model: {str(e)}")

    def close(self):
        """Clean up resources."""
        # Clear device cache
        if self.device == "cuda":
            torch.cuda.empty_cache()
        elif self.device == "mps":
            torch.mps.empty_cache()
        gc.collect()

    @lru_cache(maxsize=1000)
    def translate_text(self, text: str, max_retries: int = 3) -> str:
        """Translate English text to Hebrew using the MarianMT model with retries."""
        # Preprocess text to handle potential issues
        text = text.strip()
        if not text:
            return ""
            
        # More aggressive text cleaning
        # Remove any non-ASCII characters that might cause issues
        base_text = ''.join(char for char in text if ord(char) < 128)
        # Normalize whitespace
        base_text = ' '.join(base_text.split())
        
        # Log the text being processed for debugging
        logger.debug(f"Processing text: {base_text}")
        
        for attempt in range(max_retries):
            try:
                # Add prefix for MarianMT model
                prompt = f"translate English to Hebrew: {base_text}"
                
                # Try to tokenize with explicit error handling
                try:
                    inputs = self.tokenizer(
                        prompt,
                        return_tensors="pt",
                        max_length=512,
                        truncation=True,
                        padding=True,
                        add_special_tokens=True
                    )
                except Exception as e:
                    log_error(logger, e, {
                        'operation': 'tokenize',
                        'text': base_text,
                        'attempt': attempt + 1
                    })
                    if attempt == max_retries - 1:
                        return text  # Return original text if tokenization fails
                    continue
                
                # Move inputs to device
                inputs = {k: v.to(self.device) for k, v in inputs.items()}
                
                # Generate translation
                try:
                    outputs = self.model.generate(
                        **inputs,
                        max_length=512,
                        num_beams=5,
                        early_stopping=True
                    )
                except Exception as e:
                    log_error(logger, e, {
                        'operation': 'generate_translation',
                        'text': base_text,
                        'attempt': attempt + 1
                    })
                    if attempt == max_retries - 1:
                        return text
                    continue
                
                # Decode the output
                try:
                    translated = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
                    return translated
                except Exception as e:
                    log_error(logger, e, {
                        'operation': 'decode_translation',
                        'text': base_text,
                        'attempt': attempt + 1
                    })
                    if attempt == max_retries - 1:
                        return text
                    continue
                    
            except Exception as e:
                log_error(logger, e, {
                    'operation': 'translate_text',
                    'text': base_text,
                    'attempt': attempt + 1
                })
                if attempt == max_retries - 1:
                    return text
                continue
        
        return text  # Return original text if all retries fail

    def get_nodes_to_translate(self, node_type: str, property_name: str) -> List[dict]:
        """Get nodes that need translation from Weaviate."""
        query = {
            "class": node_type,
            "fields": [property_name, "id"],
            "where": {
                "path": [property_name + "_he"],
                "operator": "IsNull"
            }
        }
        
        results = self.client.client.query.get(node_type, [property_name, "id"]).do()
        return [{"text": node[property_name], "node_id": node["id"]} for node in results]

    def update_node_translation(self, node_id: str, property_name: str, translated_text: str):
        """Update node with translated text in Weaviate."""
        self.client.client.data_object.update(
            class_name=node_type,
            uuid=node_id,
            data_object={property_name + "_he": translated_text}
        )

    def process_batch(self, batch: List[dict], property_name: str) -> Dict[str, str]:
        """Process a batch of nodes in parallel."""
        results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_node = {
                executor.submit(self.translate_text, node["text"]): node["node_id"]
                for node in batch
            }
            
            for future in future_to_node:
                node_id = future_to_node[future]
                try:
                    translated_text = future.result()
                    results[node_id] = translated_text
                except Exception as e:
                    logger.error(f"Error translating node {node_id}: {str(e)}")
                    
        return results

    def translate_nodes(self, node_type: str, property_name: str, batch_size: int = 100):
        """Translate nodes in batches with improved error handling and performance."""
        nodes = self.get_nodes_to_translate(node_type, property_name)
        total_nodes = len(nodes)
        
        if total_nodes == 0:
            logger.info(f"No nodes found for translation (type: {node_type}, property: {property_name})")
            return

        logger.info(f"Found {total_nodes} nodes to translate")
        
        # Process in batches
        for i in tqdm(range(0, total_nodes, batch_size), 
                     desc="Translating batches",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'):
            batch = nodes[i:i + batch_size]
            
            # Process batch in parallel
            results = self.process_batch(batch, property_name)
            
            # Update nodes with translations
            for node_id, translated_text in results.items():
                try:
                    self.update_node_translation(node_id, property_name, translated_text)
                except Exception as e:
                    logger.error(f"Error updating node {node_id}: {str(e)}")
            
            # Clear cache periodically
            if i % (batch_size * 10) == 0:
                self.translate_text.cache_clear()
                if self.device == "cuda":
                    torch.cuda.empty_cache()
                elif self.device == "mps":
                    torch.mps.empty_cache()
                gc.collect()

def main():
    parser = argparse.ArgumentParser(description="Translate ESCO node properties to Hebrew")
    parser.add_argument("--config", type=str, help="Path to YAML config file")
    parser.add_argument("--profile", type=str, default='default',
                      help="Configuration profile to use")
    parser.add_argument("--property", required=True, help="Property to translate")
    parser.add_argument("--type", required=True, choices=["Skill", "Occupation", "SkillGroup", "ISCOGroup"],
                      help="Node type to translate")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--suffix", default="_he", help="Suffix for translated property")
    parser.add_argument("--device", choices=["cpu", "cuda", "mps"], help="Device to use for translation")

    args = parser.parse_args()

    translator = ESCOTranslator(config_path=args.config, profile=args.profile, device=args.device)
    try:
        translator.translate_nodes(args.type, args.property, args.batch_size)
    finally:
        translator.close()

if __name__ == "__main__":
    main() 