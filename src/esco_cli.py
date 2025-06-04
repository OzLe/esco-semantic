#!/usr/bin/env python3
import os
import yaml
import json
import click

# Local imports
from src.download_model import download_model
from src.logging_config import setup_logging
from src.weaviate_semantic_search import ESCOSemanticSearch

# Service layer imports
from src.services.ingestion_service import IngestionService
from src.models.ingestion_models import (
    IngestionConfig,
    IngestionProgress,
    IngestionDecision,
    IngestionResult,
    ValidationResult
)

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

@click.group()
def cli():
    """ESCO Data Management and Search Tool"""
    pass

def cli_progress_callback(progress: IngestionProgress) -> None:
    """
    CLI progress callback for displaying ingestion progress.
    
    Args:
        progress: Progress information from the service layer
    """
    print_section(f"Step {progress.step_number}/{progress.total_steps}: {progress.step_description}")
    percentage = progress.progress_percentage
    print(f"Progress: {colorize(f'{percentage:.1f}%', Colors.GREEN)} ({progress.progress_display})")

def handle_ingestion_decision(decision: IngestionDecision, force_reingest: bool) -> bool:
    """
    Handle the ingestion decision with appropriate user interaction.
    
    Args:
        decision: Decision object from the service layer
        force_reingest: Whether force reingest was requested
        
    Returns:
        bool: True if ingestion should proceed, False otherwise
    """
    if not decision.should_run:
        print(colorize(f"\n⚠️  {decision.reason}", Colors.YELLOW))
        
        if decision.force_required and not force_reingest:
            print(colorize("Use --force-reingest to override this decision.", Colors.YELLOW))
            return False
        
        if decision.existing_classes:
            print(f"Existing data found for classes: {colorize(', '.join(decision.existing_classes), Colors.BOLD)}")
            
        return False
    
    # Handle cases where user confirmation is needed
    if decision.existing_classes and not force_reingest:
        print(f"\n{colorize('⚠️  Warning:', Colors.YELLOW)} {decision.reason}")
        print(f"Existing data found for classes: {colorize(', '.join(decision.existing_classes), Colors.BOLD)}")
        
        if not click.confirm("Do you want to proceed with re-ingestion?", default=False):
            print(colorize("Ingestion cancelled by user.", Colors.YELLOW))
            return False
        print(colorize("Proceeding with re-ingestion based on user confirmation.", Colors.GREEN))
    
    return True

def display_ingestion_result(result: IngestionResult) -> None:
    """
    Display the final ingestion result.
    
    Args:
        result: Result object from the service layer
    """
    if result.success:
        print(colorize(f"\n✓ Ingestion completed successfully", Colors.GREEN))
        if result.duration:
            print(f"Duration: {colorize(f'{result.duration:.1f} seconds', Colors.BOLD)}")
        print(f"Steps completed: {colorize(f'{result.steps_completed}/{result.total_steps}', Colors.BOLD)}")
    else:
        print(colorize(f"\n✗ Ingestion failed", Colors.RED))
        print(f"Steps completed: {colorize(f'{result.steps_completed}/{result.total_steps}', Colors.BOLD)}")
        if result.errors:
            print(colorize("\nErrors:", Colors.RED))
            for error in result.errors:
                print(f"  • {error}")
    
    if result.warnings:
        print(colorize(f"\nWarnings ({len(result.warnings)}):", Colors.YELLOW))
        for warning in result.warnings[:5]:  # Show first 5 warnings
            print(f"  • {warning}")
        if len(result.warnings) > 5:
            print(f"  ... and {len(result.warnings) - 5} more warnings")

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
    service = None
    try:
        print_header("ESCO Data Ingestion")
        
        # Validate and prepare configuration
        if not config:
            # Try to find config in default locations
            default_paths = [
                'config/weaviate_config.yaml',
                '../config/weaviate_config.yaml',
                os.path.expanduser('~/.esco/weaviate_config.yaml')
            ]
            for path in default_paths:
                if os.path.exists(path):
                    config = path
                    break
        
        if not config or not os.path.exists(config):
            raise click.ClickException(
                "Configuration file not found. Please specify --config or ensure "
                "config/weaviate_config.yaml exists."
            )
        
        # Create ingestion configuration
        ingestion_config = IngestionConfig(
            config_path=config,
            profile=profile,
            delete_all=delete_all,
            embeddings_only=embeddings_only,
            classes=list(classes) if classes else [],
            skip_relations=skip_relations,
            force_reingest=force_reingest
        )
        
        # Initialize service
        service = IngestionService(ingestion_config)
        
        # Validate prerequisites
        print_section("Validating Prerequisites")
        validation = service.validate_prerequisites()
        
        if not validation.is_valid:
            print(colorize("✗ Prerequisites validation failed:", Colors.RED))
            for error in validation.errors:
                print(f"  • {error}")
            raise click.ClickException("Prerequisites validation failed")
        
        print(colorize("✓ Prerequisites validation passed", Colors.GREEN))
        if validation.warnings:
            print(colorize("Warnings:", Colors.YELLOW))
            for warning in validation.warnings:
                print(f"  • {warning}")
        
        # Check if ingestion should run
        print_section("Checking Ingestion Status")
        decision = service.should_run_ingestion(force_reingest)
        
        # Handle decision and user interaction
        if not handle_ingestion_decision(decision, force_reingest):
            return
        
        # Handle embeddings-only mode
        if embeddings_only:
            print_section("Generating Embeddings")
            print(colorize("Note: Weaviate generates embeddings during ingestion automatically", Colors.BLUE))
            print(colorize("✓ Embeddings process completed", Colors.GREEN))
            return
        
        # Display ingestion plan
        print_section("Ingestion Plan")
        print(f"Classes to ingest: {colorize(', '.join(ingestion_config.classes_to_ingest), Colors.BOLD)}")
        if skip_relations:
            print(colorize("Relationships: Skipped", Colors.YELLOW))
        else:
            print(colorize("Relationships: Will be created", Colors.GREEN))
        
        if delete_all:
            print(colorize("⚠️  All existing data will be deleted", Colors.YELLOW))
        
        # Run ingestion
        print_section("Starting Ingestion")
        result = service.run_ingestion(progress_callback=cli_progress_callback)
        
        # Display results
        display_ingestion_result(result)
        
        # Verify completion if successful
        if result.success:
            print_section("Verifying Completion")
            verification = service.verify_completion()
            
            if verification.is_valid:
                print(colorize("✓ Ingestion verification passed", Colors.GREEN))
                
                # Display metrics
                metrics = service.get_ingestion_metrics()
                if metrics.get('entity_counts'):
                    print("\nEntity counts:")
                    for class_name, count in metrics['entity_counts'].items():
                        if count > 0:
                            print(f"  • {class_name}: {colorize(str(count), Colors.BOLD)}")
            else:
                print(colorize("⚠️  Ingestion verification failed:", Colors.YELLOW))
                for error in verification.errors:
                    print(f"  • {error}")
        
        # Exit with appropriate code
        if not result.success:
            raise click.ClickException("Ingestion failed")
            
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise click.ClickException(str(e))
    finally:
        if service is not None:
            service.close()

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

@cli.command()
@click.option('--title', required=True, help='Job title to enrich')
@click.option('--description', required=True, help='Job description to enrich')
@click.option('--max-occupations', default=5, help='Maximum number of occupations to return')
@click.option('--max-skills', default=15, help='Maximum number of skills to extract')
@click.option('--config', default='config/weaviate_config.yaml', help='Path to Weaviate configuration file')
@click.option('--profile', default='default', help='Configuration profile to use')
@click.option('--json', is_flag=True, help='Output results in JSON format')
def enrich(title: str, description: str, max_occupations: int, max_skills: int, config: str, profile: str, json: bool):
    """Enrich a job posting with ESCO taxonomy."""
    try:
        print_header("ESCO Job Posting Enrichment")
        print(f"Job Title: {colorize(title, Colors.BOLD)}")
        
        # Initialize the search engine
        search_engine = ESCOSemanticSearch(
            config_path=config,
            profile=profile
        )
        
        # Enrich the job posting
        print_section("Enriching job posting with ESCO taxonomy...")
        enrichment_result = search_engine.enrich_job_posting(
            job_title=title,
            job_description=description,
            max_occupations=max_occupations,
            max_skills=max_skills
        )
        
        if json:
            # Get summary and output as JSON
            summary = search_engine.get_enrichment_summary(enrichment_result)
            print("\n" + format_json_output(summary))
        else:
            # Display results in a formatted way
            print(f"\n📊 Job Title: {enrichment_result.job_title}")
            print(f"🎯 Overall Confidence: {enrichment_result.confidence_score:.2%}")
            
            print(f"\n🏢 Matched Occupations ({len(enrichment_result.matched_occupations)}):")
            for i, occupation in enumerate(enrichment_result.matched_occupations, 1):
                print(f"  {i}. {occupation['preferredLabel_en']} (Score: {occupation['similarity_score']:.2%})")
                if occupation.get('description_en'):
                    print(f"     {occupation['description_en'][:100]}...")
            
            print(f"\n🛠️ Extracted Skills ({len(enrichment_result.extracted_skills)}):")
            for i, skill in enumerate(enrichment_result.extracted_skills[:10], 1):
                print(f"  {i}. {skill['preferredLabel_en']} (Score: {skill['similarity_score']:.2%})")
                print(f"     Type: {skill.get('skillType', 'Unknown')}")
            
            if enrichment_result.skill_gaps:
                print(f"\n⚠️ Skill Gaps ({len(enrichment_result.skill_gaps)}):")
                for i, gap in enumerate(enrichment_result.skill_gaps[:5], 1):
                    print(f"  {i}. {gap['preferredLabel_en']} (Essential for matched occupations)")
            
            if enrichment_result.isco_groups:
                print(f"\n📋 ISCO Classifications:")
                for group in enrichment_result.isco_groups:
                    print(f"  • {group['preferredLabel_en']} (Code: {group.get('code', 'N/A')})")
        
    except Exception as e:
        logger.error(f"Enrichment failed: {str(e)}")
        raise click.ClickException(str(e))

if __name__ == "__main__":
    cli() 