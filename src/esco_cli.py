#!/usr/bin/env python3
import argparse
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

def print_related_nodes(related_graph):
    """Print related nodes in a structured format"""
    if not related_graph:
        return
    
    node = related_graph['node']
    print_section(f"Related entities for '{node['label']}'")
    
    for rel_type, rel_nodes in related_graph['related'].items():
        if not rel_nodes:
            continue
            
        # Format the relationship type
        rel_type_display = rel_type.replace('_', ' ').title()
        count = len(rel_nodes)
        print(f"\n{colorize(rel_type_display, Colors.BOLD)} ({count}):")
        
        # Print first 5 nodes
        for node in rel_nodes[:5]:
            print(f"  • {node['label']}")
        
        # Indicate if there are more
        if count > 5:
            print(f"  ... and {count - 5} more")

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

def setup_logging(level=logging.INFO):
    """Setup logging configuration for all modules"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # Console handler
            logging.FileHandler('logs/esco.log')  # File handler
        ]
    )
    
    # Set specific logger levels
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('tqdm').setLevel(logging.WARNING)
    logging.getLogger('sentence_transformers').setLevel(logging.WARNING)
    logging.getLogger('transformers').setLevel(logging.WARNING)
    
    # Disable tqdm progress bars for specific modules
    import tqdm
    tqdm.tqdm.monitor_interval = 0
    
    return logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description='ESCO Data Management and Search CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download translation model
  python esco_cli.py download-model

  # Ingest ESCO data
  python esco_cli.py ingest --config config/weaviate_config.yaml

  # Search for skills
  python esco_cli.py search --query "python programming" --type Skill

  # Translate nodes
  python esco_cli.py translate --type Skill --property prefLabel

  # Get help for a specific command
  python esco_cli.py search --help
        """
    )

    # Common parameters
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('--config', type=str, help='Path to YAML config file')
    common_parser.add_argument('--profile', type=str, default='default',
                             help='Configuration profile to use')
    common_parser.add_argument('--quiet', action='store_true',
                             help='Reduce output verbosity')

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Download model command
    download_parser = subparsers.add_parser('download-model', help='Download translation model')

    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', parents=[common_parser], help='Ingest ESCO data')
    ingest_parser.add_argument('--embeddings-only', action='store_true', help='Only generate embeddings')
    ingest_parser.add_argument('--delete-all', action='store_true', help='Delete all data before ingestion')
    ingest_parser.add_argument('--classes', nargs='+', choices=['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection'],
                             help='Specific classes to ingest')
    ingest_parser.add_argument('--skip-relations', action='store_true',
                             help='Skip creating relationships between entities')

    # Search command
    search_parser = subparsers.add_parser('search', parents=[common_parser], help='Search ESCO data')
    search_parser.add_argument('--query', type=str, required=True, help='Text query for semantic search')
    search_parser.add_argument('--type', type=str, default='Skill',
                             choices=['Skill', 'Occupation', 'ISCOGroup', 'SkillCollection', 'All'],
                             help='Type of nodes to search')
    search_parser.add_argument('--limit', type=int, default=10, help='Maximum number of results')
    search_parser.add_argument('--threshold', type=float, default=0.5,
                             help='Minimum similarity threshold (0.0 to 1.0)')
    search_parser.add_argument('--profile-search', action='store_true',
                             help='Include complete occupation profiles in results')
    search_parser.add_argument('--json', action='store_true', help='Output results in JSON format')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == 'download-model':
            print_header("Downloading Translation Model")
            download_model()
            print(colorize("\n✓ Model downloaded successfully", Colors.GREEN))
            return

        # For commands that need configuration
        if args.command in ['ingest', 'search', 'translate']:
            # Load config
            config = load_config(args.config, args.profile)
            
            if args.command == 'ingest':
                print_header("ESCO Data Ingestion")
                ingestor = create_ingestor(args.config, args.profile)
                
                if args.delete_all:
                    print_section("Deleting Existing Data")
                    ingestor.delete_all_data()
                    print(colorize("✓ All data deleted", Colors.GREEN))
                
                print_section("Starting Ingestion")
                
                # Determine which classes to ingest
                classes_to_ingest = args.classes if args.classes else ['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']
                
                if args.embeddings_only:
                    print("Generating embeddings only...")
                    ingestor.run_embeddings_only()
                else:
                    print("Running ingestion for classes:", ", ".join(classes_to_ingest))
                    if args.skip_relations:
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
                    if not args.skip_relations:
                        print_section("Creating Relationships")
                        ingestor.create_skill_relations()
                        ingestor.create_hierarchical_relations()
                        ingestor.create_isco_group_relations()
                        ingestor.create_skill_collection_relations()
                
                ingestor.close()
                print(colorize("\n✓ Ingestion completed successfully", Colors.GREEN))

            elif args.command == 'search':
                print_header("ESCO Semantic Search")
                print(f"Query: {colorize(args.query, Colors.BOLD)}")
                print(f"Type: {colorize(args.type, Colors.BOLD)}")
                print(f"Threshold: {colorize(str(args.threshold), Colors.BOLD)}")
                
                search_engine = ESCOSemanticSearch(args.config, args.profile)
                
                print_section("Searching...")
                
                if args.profile_search:
                    if args.type != "Occupation":
                        print(colorize("\nWarning: Profile search is only available for Occupation type. Switching to Occupation type.", Colors.YELLOW))
                        args.type = "Occupation"
                    
                    results = search_engine.semantic_search_with_profile(
                        args.query,
                        args.limit,
                        args.threshold
                    )
                    
                    if not results:
                        print(colorize("\nNo results found.", Colors.YELLOW))
                        return
                    
                    print_section("Search Results with Profiles")
                    for i, result in enumerate(results, 1):
                        search_result = result['search_result']
                        print_result(search_result, i)
                        print_related_nodes(result['profile'])
                    
                    if args.json:
                        print("\n" + format_json_output({
                            "query": args.query,
                            "parameters": {
                                "limit": args.limit,
                                "similarity_threshold": args.threshold
                            },
                            "results": results
                        }))
                else:
                    results = search_engine.search(
                        args.query, 
                        args.type, 
                        args.limit, 
                        args.threshold
                    )

                    if not results:
                        print(colorize("\nNo results found.", Colors.YELLOW))
                        return

                    print_section("Search Results")
                    for i, result in enumerate(results, 1):
                        print_result(result, i)

                    if args.json:
                        print("\n" + format_json_output({
                            "query": args.query,
                            "results": results
                        }))

    except Exception as e:
        print(colorize(f"\nError: {str(e)}", Colors.RED))
        raise

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
        # Create ingestor instance
        ingestor = create_ingestor(config, profile)
        
        if delete_all:
            click.echo("Deleting all existing data...")
            ingestor.delete_all_data()
        
        # Determine which classes to ingest
        classes_to_ingest = list(classes) if classes else ['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']
        
        # Run appropriate process
        if embeddings_only:
            click.echo("Running embeddings-only mode...")
            ingestor.run_embeddings_only()
        else:
            click.echo(f"Running ingestion for classes: {', '.join(classes_to_ingest)}")
            if skip_relations:
                click.echo("Skipping relationship creation")
            
            # Run ingestion for each selected class
            for class_name in classes_to_ingest:
                click.echo(f"\nIngesting {class_name}...")
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
                click.echo("\nCreating relationships...")
                ingestor.create_skill_relations()
                ingestor.create_hierarchical_relations()
                ingestor.create_isco_group_relations()
                ingestor.create_skill_collection_relations()
        
        click.echo("Ingestion completed successfully!")
        
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
        # Initialize search engine
        engine = ESCOSemanticSearch(config, profile)
        
        # Perform search
        if profile_search and type == 'Occupation':
            results = engine.semantic_search_with_profile(
                query=query,
                limit=limit,
                certainty=certainty
            )
        else:
            results = engine.search(
                query=query,
                type=type,
                limit=limit,
                certainty=certainty
            )
        
        # Output results
        if json:
            click.echo(json.dumps(results, indent=2))
        else:
            click.echo(engine.format_results(results))
            
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise click.ClickException(str(e))

if __name__ == "__main__":
    main() 