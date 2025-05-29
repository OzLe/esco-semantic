#!/usr/bin/env python3
import logging
import os
import yaml
import json
from datetime import datetime
import click
from pathlib import Path
from typing import Optional
import pandas as pd

# Local imports
from src.weaviate_semantic_search import ESCOSemanticSearch
from src.embedding_utils import ESCOEmbedding, generate_embeddings
from src.esco_ingest import create_ingestor
from src.esco_translate import ESCOTranslator
from src.download_model import download_model
from src.logging_config import setup_logging
from src.weaviate_client import WeaviateClient

# Setup logging
logger = setup_logging()

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def colorize(text, color):
    """Add color to text if terminal supports it"""
    if os.getenv('NO_COLOR') or not os.isatty(1):
        return text
    return f"{color}{text}{Colors.ENDC}"

def print_header(text):
    """Print a section header"""
    print("\n" + "=" * 80)
    print(colorize(f" {text} ".center(80, "="), Colors.HEADER))
    print("=" * 80 + "\n")

def print_section(text):
    """Print a subsection header"""
    print("\n" + "-" * 80)
    print(colorize(f" {text} ".center(80, "-"), Colors.BLUE))
    print("-" * 80 + "\n")

def print_result(result, index=None):
    """Print a single search result"""
    if index is not None:
        prefix = f"{index}. "
    else:
        prefix = "• "
    
    # Print the main label with type
    type_str = colorize(f"[{result['type']}]", Colors.YELLOW)
    score_str = colorize(f"(Score: {result['score']:.4f})", Colors.GREEN)
    print(f"{prefix}{type_str} {result['label']} {score_str}")
    
    # Print description if available
    if result.get('description'):
        desc = result['description']
        if len(desc) > 100:
            desc = desc[:97] + "..."
        print(f"   {colorize('Description:', Colors.BOLD)} {desc}")
    
    # Print additional fields based on type
    if result['type'] == 'Skill':
        if result.get('skillType'):
            print(f"   {colorize('Skill Type:', Colors.BOLD)} {result['skillType']}")
        
        if result.get('broaderSkills'):
            print(f"   {colorize('Broader Skills:', Colors.BOLD)}")
            for skill in result['broaderSkills']:
                print(f"     • {skill['label']}")
        
        if result.get('skillCollections'):
            print(f"   {colorize('Skill Collections:', Colors.BOLD)}")
            for collection in result['skillCollections']:
                print(f"     • {collection['label']}")
        
        if result.get('relatedSkills'):
            print(f"   {colorize('Related Skills:', Colors.BOLD)}")
            for skill in result['relatedSkills']:
                rel_type = f" ({skill['relationType']})" if skill.get('relationType') else ""
                print(f"     • {skill['label']}{rel_type}")
    
    elif result['type'] == 'Occupation':
        if result.get('iscoCode'):
            print(f"   {colorize('ISCO Code:', Colors.BOLD)} {result['iscoCode']}")
        
        if result.get('broaderOccupations'):
            print(f"   {colorize('Broader Occupations:', Colors.BOLD)}")
            for occ in result['broaderOccupations']:
                print(f"     • {occ['label']}")
        
        if result.get('essentialSkills'):
            print(f"   {colorize('Essential Skills:', Colors.BOLD)}")
            for skill in result['essentialSkills']:
                print(f"     • {skill['label']}")
        
        if result.get('optionalSkills'):
            print(f"   {colorize('Optional Skills:', Colors.BOLD)}")
            for skill in result['optionalSkills']:
                print(f"     • {skill['label']}")

def print_related_nodes(related_graph):
    """Print related nodes in a structured format"""
    if not related_graph:
        return
    
    node = related_graph['node']
    print_section(f"Related entities for '{node['label']}'")
    
    # Print ISCO information if available
    if node.get('iscoCode'):
        print(f"\n{colorize('ISCO Code:', Colors.BOLD)} {node['iscoCode']}")
    
    # Print broader occupations if available
    if node.get('broaderOccupations'):
        print(f"\n{colorize('Broader Occupations:', Colors.BOLD)}")
        for occ in node['broaderOccupations']:
            print(f"  • {occ['label']}")
            if occ.get('broaderOccupations'):
                for sub_occ in occ['broaderOccupations']:
                    print(f"    - {sub_occ['label']}")
    
    # Print skills information
    if node.get('essentialSkills'):
        print(f"\n{colorize('Essential Skills:', Colors.BOLD)}")
        for skill in node['essentialSkills']:
            print(f"  • {skill['label']}")
            if skill.get('broaderSkills'):
                for broader in skill['broaderSkills']:
                    print(f"    - {broader['label']}")
    
    if node.get('optionalSkills'):
        print(f"\n{colorize('Optional Skills:', Colors.BOLD)}")
        for skill in node['optionalSkills']:
            print(f"  • {skill['label']}")
            if skill.get('broaderSkills'):
                for broader in skill['broaderSkills']:
                    print(f"    - {broader['label']}")
    
    # Print skill collections if available
    if node.get('skillCollections'):
        print(f"\n{colorize('Skill Collections:', Colors.BOLD)}")
        for collection in node['skillCollections']:
            print(f"  • {collection['label']}")
    
    # Print related skills if available
    if node.get('relatedSkills'):
        print(f"\n{colorize('Related Skills:', Colors.BOLD)}")
        for skill in node['relatedSkills']:
            rel_type = f" ({skill['relationType']})" if skill.get('relationType') else ""
            print(f"  • {skill['label']}{rel_type}")

def format_json_output(data):
    """Format JSON output with consistent indentation"""
    return json.dumps(data, indent=2, ensure_ascii=False)

def load_config(config_path=None, profile='default'):
    """Load and validate configuration file"""
    if config_path is None:
        # Try to find config in default locations
        default_paths = [
            'config/weaviate_config.yaml',
            '../config/weaviate_config.yaml',
            os.path.expanduser('~/.esco/weaviate_config.yaml')
        ]
        for path in default_paths:
            if os.path.exists(path):
                config_path = path
                break
    
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(
            "Configuration file not found. Please specify --config or ensure "
            "config/weaviate_config.yaml exists."
        )
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        raise ValueError(f"Failed to load config file: {str(e)}")
    
    if not isinstance(config, dict):
        raise ValueError("Invalid config file format")
    
    if profile not in config:
        raise ValueError(f"Profile '{profile}' not found in config file")
    
    return config

@click.group()
def cli():
    """ESCO Data Management and Search Tool"""
    pass

@cli.command()
@click.option('--config', type=str, help='Path to configuration file')
@click.option('--profile', default='default', help='Configuration profile to use')
@click.option('--delete-all', is_flag=True, help='Delete all existing data before ingestion')
@click.option('--embeddings-only', is_flag=True, help='Run only the embedding generation and indexing')
@click.option('--classes', multiple=True, type=click.Choice(['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']),
              help='Specific classes to ingest (can be specified multiple times)')
@click.option('--skip-relations', is_flag=True, help='Skip creating relationships between entities')
@click.option('--force-reingest', is_flag=True, help='Force re-ingestion of all classes regardless of existing data')
def ingest(config: str, profile: str, delete_all: bool, embeddings_only: bool, classes: tuple, skip_relations: bool, force_reingest: bool):
    """Ingest ESCO data into Weaviate."""
    try:
        print_header("ESCO Data Ingestion")
        
        # Create ingestor instance
        ingestor = create_ingestor(config, profile)
        
        if delete_all:
            print_section("Deleting Existing Data")
            ingestor.delete_all_data()
            print(colorize("✓ All data deleted", Colors.GREEN))
        
        # Determine which classes to ingest
        classes_to_ingest = list(classes) if classes else ['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']
        
        # Run appropriate process
        if embeddings_only:
            print_section("Generating Embeddings")
            ingestor.run_embeddings_only()
        else:
            print_section("Starting Ingestion")
            print(f"Running ingestion for classes: {', '.join(classes_to_ingest)}")
            if skip_relations:
                print("Skipping relationship creation")
            
            # Run ingestion for each selected class
            for class_name in classes_to_ingest:
                print_section(f"Ingesting {class_name}")
                if class_name == 'Occupation':
                    ingestor.ingest_occupations()
                elif class_name == 'Skill':
                    ingestor.ingest_skills()
                elif class_name == 'ISCOGroup':
                    ingestor.ingest_isco_groups()
                elif class_name == 'SkillCollection':
                    ingestor.ingest_skill_collections()
            
            # Create relationships if not skipped
            if not skip_relations:
                print_section("Creating Relationships")
                ingestor.create_skill_relations()
                ingestor.create_hierarchical_relations()
                ingestor.create_isco_group_relations()
                ingestor.create_skill_collection_relations()
        
        print(colorize("\n✓ Ingestion completed successfully", Colors.GREEN))
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise click.ClickException(str(e))
    finally:
        ingestor.close()

@cli.command()
@click.option('--query', required=True, help='Search query')
@click.option('--limit', default=10, help='Maximum number of results')
@click.option('--certainty', default=0.75, help='Minimum similarity threshold (0-1)')
@click.option('--config', default='config/weaviate_config.yaml', help='Path to Weaviate configuration file')
@click.option('--profile', default='default', help='Configuration profile to use')
@click.option('--type', type=click.Choice(['Skill', 'Occupation', 'ISCOGroup', 'SkillCollection', 'All']),
              default='Skill', help='Type of nodes to search')
@click.option('--json', is_flag=True, help='Output results in JSON format')
@click.option('--profile-search', is_flag=True, help='Include complete occupation profiles in results')
def search(query: str, limit: int, certainty: float, config: str, profile: str, type: str, json: bool, profile_search: bool):
    """Search ESCO data using Weaviate."""
    try:
        print_header("ESCO Semantic Search")
        print(f"Query: {colorize(query, Colors.BOLD)}")
        print(f"Type: {colorize(type, Colors.BOLD)}")
        print(f"Threshold: {colorize(str(certainty), Colors.BOLD)}")
        
        # Initialize search engine
        engine = ESCOSemanticSearch(config, profile)
        
        # Perform search
        if profile_search:
            if type != 'Occupation':
                print(colorize("\nWarning: Profile search is only available for Occupation type. Switching to Occupation type.", Colors.YELLOW))
                type = 'Occupation'
            
            results = engine.semantic_search_with_profile(
                query=query,
                limit=limit,
                similarity_threshold=certainty
            )
            
            if not results:
                print(colorize("\nNo results found.", Colors.YELLOW))
                return
            
            if json:
                print("\n" + format_json_output({
                    "query": query,
                    "parameters": {
                        "limit": limit,
                        "similarity_threshold": certainty
                    },
                    "results": results
                }))
            else:
                print_section("Search Results with Profiles")
                for i, result in enumerate(results, 1):
                    print_result(result['search_result'], i)
                    print_related_nodes(result['profile'])
        else:
            results = engine.search(
                query=query,
                node_type=type,
                limit=limit,
                similarity_threshold=certainty
            )
            
            if not results:
                print(colorize("\nNo results found.", Colors.YELLOW))
                return
            
            if json:
                print("\n" + format_json_output({
                    "query": query,
                    "parameters": {
                        "type": type,
                        "limit": limit,
                        "similarity_threshold": certainty
                    },
                    "results": results
                }))
            else:
                print_section("Search Results")
                for i, result in enumerate(results, 1):
                    print_result(result, i)
            
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise click.ClickException(str(e))

@cli.command()
def download_model():
    """Download translation model."""
    try:
        print_header("Downloading Translation Model")
        download_model()
        print(colorize("\n✓ Model downloaded successfully", Colors.GREEN))
    except Exception as e:
        logger.error(f"Model download failed: {str(e)}")
        raise click.ClickException(str(e))

if __name__ == "__main__":
    cli() 