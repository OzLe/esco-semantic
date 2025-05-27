import click
from pathlib import Path
from esco.translation.translator import TranslationEngine
from esco.translation.batch import BatchTranslator
from ..utils import setup_logging

logger = setup_logging(__name__)

@click.group()
def translate():
    """Translation commands"""
    pass

@translate.command()
@click.argument('text')
@click.option('--source', '-s', default='en', help='Source language')
@click.option('--target', '-t', default='es', help='Target language')
@click.option('--model', '-m', help='Translation model name')
@click.pass_context
def text(ctx, text: str, source: str, target: str, model: str):
    """Translate text"""
    try:
        # Initialize translator
        translator = TranslationEngine(
            source_lang=source,
            target_lang=target,
            model_name=model
        )
        
        # Translate
        result = translator.translate(text)
        click.echo(result)
        
    except Exception as e:
        logger.error(f"Translation failed: {e}")
        raise click.ClickException(str(e))

@translate.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_file', type=click.Path())
@click.option('--columns', '-c', required=True, help='Columns to translate (comma-separated)')
@click.option('--source', '-s', default='en', help='Source language')
@click.option('--target', '-t', default='es', help='Target language')
@click.option('--model', '-m', help='Translation model name')
@click.option('--batch-size', '-b', default=32, help='Batch size')
@click.option('--format', '-f', type=click.Choice(['csv', 'excel']),
              default='csv', help='File format')
@click.pass_context
def file(ctx, input_file: str, output_file: str, columns: str,
         source: str, target: str, model: str, batch_size: int, format: str):
    """Translate columns in a file"""
    try:
        # Initialize translator
        translator = TranslationEngine(
            source_lang=source,
            target_lang=target,
            model_name=model
        )
        
        batch_translator = BatchTranslator(
            translator=translator,
            batch_size=batch_size
        )
        
        # Parse columns
        columns_to_translate = [col.strip() for col in columns.split(',')]
        
        # Translate file
        batch_translator.translate_file(
            input_file=input_file,
            output_file=output_file,
            columns=columns_to_translate,
            file_format=format
        )
        
        click.echo(f"Translation completed. Results saved to {output_file}")
        
    except Exception as e:
        logger.error(f"File translation failed: {e}")
        raise click.ClickException(str(e))

@translate.command()
@click.option('--source', '-s', default='en', help='Source language')
@click.option('--target', '-t', default='es', help='Target language')
@click.pass_context
def languages(ctx, source: str, target: str):
    """List supported language pairs"""
    try:
        # Initialize translator
        translator = TranslationEngine(
            source_lang=source,
            target_lang=target
        )
        
        # Get supported languages
        languages = translator.get_supported_languages()
        
        # Display results
        click.echo("\nSupported language pairs:")
        for src, targets in languages.items():
            click.echo(f"\n{src.upper()} ->")
            for tgt in targets:
                click.echo(f"  - {tgt.upper()}")
        
    except Exception as e:
        logger.error(f"Failed to get supported languages: {e}")
        raise click.ClickException(str(e)) 