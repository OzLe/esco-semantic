# Migration Guide: From Current to Improved Structure

## Step 1: Prepare the New Structure

```bash
# Create new directory structure
mkdir -p src/esco/{core,models,database,ingestion,search,embeddings,translation,utils}
mkdir -p src/esco/database/weaviate
mkdir -p src/esco/ingestion/processors
mkdir -p src/cli/commands
mkdir -p tests/{unit,integration,fixtures}
mkdir -p config
mkdir -p docs/{api,guides}
mkdir -p scripts
mkdir -p requirements
```

## Step 2: Core Module Migration

### 2.1 Configuration Management
Create `src/esco/core/config.py`:

```python
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from ..core.exceptions import ConfigurationError

class Config:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.env = os.getenv('ESCO_ENV', 'development')
        self._config = self._load_config()
        self._initialized = True
    
    def _load_config(self) -> Dict[str, Any]:
        config_dir = Path(__file__).parent.parent.parent.parent / 'config'
        
        # Load base config
        base_config = self._load_yaml(config_dir / 'default.yaml')
        
        # Load environment-specific config
        env_config_file = config_dir / f'{self.env}.yaml'
        if env_config_file.exists():
            env_config = self._load_yaml(env_config_file)
            # Deep merge configurations
            return self._deep_merge(base_config, env_config)
        
        return base_config
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            raise ConfigurationError(f"Failed to load config from {path}: {e}")
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value

# Singleton accessor
def get_config() -> Config:
    return Config()
```

### 2.2 Logging Migration
Move `src/logging_config.py` to `src/esco/core/logging.py` and enhance:

```python
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from .config import get_config

class ColoredFormatter(logging.Formatter):
    """Colored output for terminal"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        if sys.stdout.isatty():
            levelname = record.levelname
            record.levelname = f"{self.COLORS.get(levelname, '')}{levelname}{self.RESET}"
        return super().format(record)

def setup_logging(name: str = None) -> logging.Logger:
    """Setup logging with proper configuration"""
    config = get_config()
    
    # Create logger
    logger = logging.getLogger(name or 'esco')
    logger.setLevel(config.get('logging.level', 'INFO'))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    )
    logger.addHandler(console_handler)
    
    # File handler
    log_dir = Path(config.get('logging.directory', 'logs'))
    log_dir.mkdir(exist_ok=True)
    
    file_handler = RotatingFileHandler(
        log_dir / 'esco.log',
        maxBytes=config.get('logging.max_bytes', 10 * 1024 * 1024),
        backupCount=config.get('logging.backup_count', 5),
        encoding='utf-8'
    )
    file_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    )
    logger.addHandler(file_handler)
    
    # Configure third-party loggers
    for lib in ['urllib3', 'transformers', 'sentence_transformers']:
        logging.getLogger(lib).setLevel(logging.WARNING)
    
    return logger
```

## Step 3: Model Classes

Create base model classes in `src/esco/models/base.py`:

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

@dataclass
class BaseEntity(ABC):
    """Base class for all ESCO entities"""
    uri: str
    preferred_label: str
    description: Optional[str] = None
    alt_labels: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary"""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate entity data"""
        pass

@dataclass
class Skill(BaseEntity):
    """Skill entity"""
    skill_type: Optional[str] = None
    reuse_level: Optional[str] = None
    definition: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'conceptUri': self.uri,
            'preferredLabel_en': self.preferred_label,
            'description_en': self.description,
            'altLabels_en': self.alt_labels,
            'skillType': self.skill_type,
            'reuseLevel': self.reuse_level,
            'definition_en': self.definition
        }
    
    def validate(self) -> bool:
        return bool(self.uri and self.preferred_label)

@dataclass
class Occupation(BaseEntity):
    """Occupation entity"""
    code: Optional[str] = None
    isco_group: Optional[str] = None
    definition: Optional[str] = None
    essential_skills: List[str] = field(default_factory=list)
    optional_skills: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'conceptUri': self.uri,
            'code': self.code,
            'preferredLabel_en': self.preferred_label,
            'description_en': self.description,
            'altLabels_en': self.alt_labels,
            'definition_en': self.definition,
            'iscoGroup': self.isco_group
        }
    
    def validate(self) -> bool:
        return bool(self.uri and self.preferred_label)
```

## Step 4: Database Abstraction

### 4.1 Create Database Interface
Create `src/esco/database/client.py`:

```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class DatabaseClient(ABC):
    """Abstract database client interface"""
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection"""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """Check if database is healthy"""
        pass
    
    @abstractmethod
    def create_collection(self, name: str, schema: Dict[str, Any]) -> None:
        """Create a collection/class"""
        pass
    
    @abstractmethod
    def delete_collection(self, name: str) -> None:
        """Delete a collection/class"""
        pass
    
    @abstractmethod
    def insert_batch(
        self, 
        collection: str, 
        objects: List[Dict[str, Any]], 
        vectors: Optional[List[List[float]]] = None
    ) -> None:
        """Insert batch of objects"""
        pass
    
    @abstractmethod
    def search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Vector search"""
        pass
    
    @abstractmethod
    def get_by_id(self, collection: str, object_id: str) -> Optional[Dict[str, Any]]:
        """Get object by ID"""
        pass
    
    @abstractmethod
    def create_reference(
        self,
        from_collection: str,
        from_id: str,
        to_collection: str,
        to_id: str,
        property_name: str
    ) -> None:
        """Create reference between objects"""
        pass
```

### 4.2 Migrate Weaviate Client
Move and refactor `src/weaviate_client.py` to `src/esco/database/weaviate/client.py`:

```python
import weaviate
from typing import List, Dict, Any, Optional
from ..client import DatabaseClient
from ...core.config import get_config
from ...core.exceptions import DatabaseError
import logging

logger = logging.getLogger(__name__)

class WeaviateClient(DatabaseClient):
    """Weaviate-specific database client implementation"""
    
    def __init__(self):
        self.config = get_config()
        self.client = None
        self._schema_manager = None
    
    def connect(self) -> None:
        """Establish connection to Weaviate"""
        try:
            self.client = weaviate.Client(
                url=self.config.get('weaviate.url'),
                timeout_config=(
                    self.config.get('weaviate.connect_timeout', 5),
                    self.config.get('weaviate.read_timeout', 60)
                )
            )
            self._schema_manager = SchemaManager(self.client)
            logger.info("Connected to Weaviate")
        except Exception as e:
            raise DatabaseError(f"Failed to connect to Weaviate: {e}")
    
    def disconnect(self) -> None:
        """Close Weaviate connection"""
        # Weaviate client doesn't require explicit disconnect
        self.client = None
        logger.info("Disconnected from Weaviate")
    
    def health_check(self) -> bool:
        """Check Weaviate health"""
        try:
            return self.client.is_ready()
        except Exception:
            return False
    
    # ... implement other abstract methods ...
```

## Step 5: Ingestion Pipeline Refactoring

### 5.1 Create Base Ingestor
Create `src/esco/ingestion/base.py`:

```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from ..database.client import DatabaseClient
from ..embeddings.generator import EmbeddingGenerator
import logging

logger = logging.getLogger(__name__)

class BaseIngestor(ABC):
    """Base class for all entity ingestors"""
    
    def __init__(
        self, 
        db_client: DatabaseClient,
        embedding_generator: EmbeddingGenerator,
        batch_size: int = 100
    ):
        self.db_client = db_client
        self.embedding_generator = embedding_generator
        self.batch_size = batch_size
    
    @abstractmethod
    def get_entity_type(self) -> str:
        """Get the entity type this ingestor handles"""
        pass
    
    @abstractmethod
    def validate_csv(self, df: pd.DataFrame) -> bool:
        """Validate CSV has required columns"""
        pass
    
    @abstractmethod
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        """Transform a CSV row to entity dict"""
        pass
    
    def ingest_file(self, file_path: str) -> int:
        """Ingest entities from CSV file"""
        logger.info(f"Starting ingestion of {file_path}")
        
        # Read CSV
        df = pd.read_csv(file_path, dtype=str)
        df = df.fillna('')
        
        # Validate
        if not self.validate_csv(df):
            raise ValueError(f"Invalid CSV format for {self.get_entity_type()}")
        
        # Process in batches
        total_processed = 0
        entities = []
        
        for idx, row in df.iterrows():
            entity = self.transform_row(row)
            if entity:
                entities.append(entity)
                
                if len(entities) >= self.batch_size:
                    self._process_batch(entities)
                    total_processed += len(entities)
                    entities = []
        
        # Process remaining
        if entities:
            self._process_batch(entities)
            total_processed += len(entities)
        
        logger.info(f"Completed ingestion: {total_processed} entities")
        return total_processed
    
    def _process_batch(self, entities: List[Dict[str, Any]]) -> None:
        """Process a batch of entities"""
        # Generate embeddings
        texts = [self._get_text_for_embedding(e) for e in entities]
        embeddings = self.embedding_generator.generate_batch(texts)
        
        # Insert to database
        self.db_client.insert_batch(
            collection=self.get_entity_type(),
            objects=entities,
            vectors=embeddings
        )
    
    @abstractmethod
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        """Get text representation for embedding generation"""
        pass
```

### 5.2 Create Specific Ingestors
Create `src/esco/ingestion/processors/skill.py`:

```python
from typing import Optional, Dict, Any
import pandas as pd
from ..base import BaseIngestor

class SkillIngestor(BaseIngestor):
    """Ingestor for Skill entities"""
    
    def get_entity_type(self) -> str:
        return "Skill"
    
    def validate_csv(self, df: pd.DataFrame) -> bool:
        required_columns = {'conceptUri', 'preferredLabel'}
        return required_columns.issubset(df.columns)
    
    def transform_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
            return None
        
        return {
            'conceptUri': row['conceptUri'].split('/')[-1],
            'preferredLabel_en': row['preferredLabel'],
            'description_en': row.get('description', ''),
            'definition_en': row.get('definition', ''),
            'skillType': row.get('skillType', ''),
            'reuseLevel': row.get('reuseLevel', ''),
            'altLabels_en': row.get('altLabels', '').split('|') if row.get('altLabels') else []
        }
    
    def _get_text_for_embedding(self, entity: Dict[str, Any]) -> str:
        parts = [
            entity.get('preferredLabel_en', ''),
            ' '.join(entity.get('altLabels_en', [])),
            entity.get('description_en', '')
        ]
        return '. '.join(filter(None, parts))
```

## Step 6: CLI Migration

### 6.1 Create New CLI Structure
Create `src/cli/main.py`:

```python
import click
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from esco.core.config import get_config
from esco.core.logging import setup_logging
from .commands import ingest, search, translate, model

@click.group()
@click.option('--config', envvar='ESCO_CONFIG', help='Configuration file')
@click.option('--env', envvar='ESCO_ENV', default='development', help='Environment')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, config, env, verbose):
    """ESCO Data Management and Search Tool"""
    # Setup context
    ctx.ensure_object(dict)
    ctx.obj['config_file'] = config
    ctx.obj['env'] = env
    ctx.obj['verbose'] = verbose
    
    # Setup logging
    logger = setup_logging()
    if verbose:
        logger.setLevel('DEBUG')

# Register commands
cli.add_command(ingest.ingest)
cli.add_command(search.search)
cli.add_command(translate.translate)
cli.add_command(model.download)

if __name__ == '__main__':
    cli()
```

### 6.2 Create Command Modules
Create `src/cli/commands/ingest.py`:

```python
import click
from esco.ingestion.orchestrator import IngestionOrchestrator
from esco.database.weaviate.client import WeaviateClient
from esco.embeddings.generator import EmbeddingGenerator

@click.command()
@click.option('--classes', '-c', multiple=True, 
              type=click.Choice(['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']),
              help='Classes to ingest')
@click.option('--delete-all', is_flag=True, help='Delete all data before ingestion')
@click.option('--skip-relations', is_flag=True, help='Skip relationship creation')
@click.option('--batch-size', default=100, help='Batch size for ingestion')
@click.pass_context
def ingest(ctx, classes, delete_all, skip_relations, batch_size):
    """Ingest ESCO data into the database"""
    # Initialize components
    db_client = WeaviateClient()
    db_client.connect()
    
    embedding_generator = EmbeddingGenerator()
    
    orchestrator = IngestionOrchestrator(
        db_client=db_client,
        embedding_generator=embedding_generator,
        batch_size=batch_size
    )
    
    try:
        if delete_all:
            click.echo("Deleting all existing data...")
            orchestrator.delete_all_data()
        
        # Run ingestion
        classes_to_ingest = list(classes) if classes else orchestrator.get_all_classes()
        click.echo(f"Ingesting classes: {', '.join(classes_to_ingest)}")
        
        results = orchestrator.ingest(
            classes=classes_to_ingest,
            skip_relations=skip_relations
        )
        
        # Display results
        click.echo("\nIngestion completed:")
        for class_name, count in results.items():
            click.echo(f"  {class_name}: {count} entities")
            
    finally:
        db_client.disconnect()
```

## Step 7: Requirements Management

### 7.1 Split Requirements
Create `requirements/base.txt`:

```txt
# Core dependencies
pandas>=2.0.0
numpy>=1.24.0
pyyaml>=6.0
click>=8.0.0
python-dotenv>=1.0.0

# Database
weaviate-client>=3.26.0

# Embeddings
sentence-transformers>=2.2.0
torch>=2.0.0
transformers>=4.36.0

# Utilities
tqdm>=4.66.0
```

Create `requirements/dev.txt`:

```txt
-r base.txt

# Testing
pytest>=7.0.0
pytest-cov>=4.0.0
pytest-mock>=3.10.0
pytest-asyncio>=0.20.0

# Code quality
black>=23.0.0
flake8>=6.0.0
mypy>=1.0.0
isort>=5.12.0

# Documentation
sphinx>=6.0.0
sphinx-rtd-theme>=1.2.0
```

## Step 8: Testing Setup

### 8.1 Create pytest Configuration
Create `pytest.ini`:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --cov=src/esco --cov-report=html --cov-report=term-missing
```

### 8.2 Create Sample Test
Create `tests/unit/test_models.py`:

```python
import pytest
from esco.models.skill import Skill

class TestSkillModel:
    def test_skill_creation(self):
        skill = Skill(
            uri="test-skill-001",
            preferred_label="Python Programming",
            description="Programming in Python",
            skill_type="technical"
        )
        
        assert skill.uri == "test-skill-001"
        assert skill.preferred_label == "Python Programming"
        assert skill.validate()
    
    def test_skill_to_dict(self):
        skill = Skill(
            uri="test-skill-001",
            preferred_label="Python Programming"
        )
        
        data = skill.to_dict()
        assert data['conceptUri'] == "test-skill-001"
        assert data['preferredLabel_en'] == "Python Programming"
```

## Step 9: Docker Updates

Update `Dockerfile`:

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements/base.txt requirements/prod.txt ./
RUN pip install --no-cache-dir -r prod.txt

# Copy application
COPY src/ ./src/
COPY config/ ./config/

# Set Python path
ENV PYTHONPATH=/app/src:$PYTHONPATH
ENV ESCO_ENV=production

# Run CLI
ENTRYPOINT ["python", "-m", "cli.main"]
```

## Step 10: Final Migration Steps

1. **Gradual Migration**: Don't try to migrate everything at once. Start with core modules.

2. **Maintain Compatibility**: Keep the old CLI working while developing the new one.

3. **Test Thoroughly**: Write tests for new modules as you create them.

4. **Document Changes**: Update documentation as you migrate.

5. **Version Control**: Use feature branches for the migration.

## Benefits of This Migration

1. **Better Organization**: Clear separation of concerns
2. **Easier Testing**: Modular design enables better unit testing
3. **Improved Maintainability**: Smaller, focused modules
4. **Better Performance**: Connection pooling and caching
5. **Enhanced Developer Experience**: Clear structure and interfaces
6. **Production Ready**: Proper configuration and deployment setup