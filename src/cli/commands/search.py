import click
from typing import Optional
from esco.search.semantic import SemanticSearchEngine
from esco.search.results import SearchResultFormatter
from esco.database.weaviate.client import WeaviateClient
from esco.embeddings.generator import EmbeddingGenerator
from ..utils import setup_logging

logger = setup_logging(__name__)

@click.group()
def search():
    """Search commands"""
    pass

@search.command()
@click.argument('query')
@click.option('--limit', '-l', default=10, help='Maximum number of results')
@click.option('--offset', '-o', default=0, help='Result offset')
@click.option('--format', '-f', type=click.Choice(['text', 'json', 'markdown', 'html']),
              default='text', help='Output format')
@click.option('--type', '-t', help='Filter by entity type')
@click.option('--min-score', '-s', type=float, default=0.0,
              help='Minimum similarity score')
@click.pass_context
def semantic(ctx, query: str, limit: int, offset: int, format: str,
            type: Optional[str], min_score: float):
    """Perform semantic search"""
    try:
        # Initialize components
        db_client = WeaviateClient()
        db_client.connect()
        
        embedding_generator = EmbeddingGenerator()
        search_engine = SemanticSearchEngine(db_client, embedding_generator)
        
        # Prepare filters
        filters = {}
        if type:
            filters['type'] = type
        
        # Perform search
        results = search_engine.search(
            query=query,
            filters=filters,
            limit=limit,
            offset=offset
        )
        
        # Filter by score
        results = [r for r in results if r.score >= min_score]
        
        # Format results
        formatter = SearchResultFormatter()
        if format == 'json':
            output = formatter.to_json(results, pretty=True)
        elif format == 'markdown':
            output = formatter.to_markdown(results)
        elif format == 'html':
            output = formatter.to_html(results)
        else:
            output = formatter.to_text(results)
        
        # Display results
        click.echo(output)
        
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise click.ClickException(str(e))
    finally:
        db_client.disconnect()

@search.command()
@click.argument('id')
@click.option('--limit', '-l', default=10, help='Maximum number of results')
@click.option('--format', '-f', type=click.Choice(['text', 'json', 'markdown', 'html']),
              default='text', help='Output format')
@click.pass_context
def similar(ctx, id: str, limit: int, format: str):
    """Find similar items"""
    try:
        # Initialize components
        db_client = WeaviateClient()
        db_client.connect()
        
        embedding_generator = EmbeddingGenerator()
        search_engine = SemanticSearchEngine(db_client, embedding_generator)
        
        # Get similar items
        results = search_engine.get_similar(id, limit=limit)
        
        # Format results
        formatter = SearchResultFormatter()
        if format == 'json':
            output = formatter.to_json(results, pretty=True)
        elif format == 'markdown':
            output = formatter.to_markdown(results)
        elif format == 'html':
            output = formatter.to_html(results)
        else:
            output = formatter.to_text(results)
        
        # Display results
        click.echo(output)
        
    except Exception as e:
        logger.error(f"Similar search failed: {e}")
        raise click.ClickException(str(e))
    finally:
        db_client.disconnect() 