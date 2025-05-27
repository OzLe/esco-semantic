# Improved Project Structure for ESCO Tool

```
esco-tool/
├── src/
│   ├── esco/
│   │   ├── __init__.py
│   │   ├── core/
│   │   │   ├── __init__.py
│   │   │   ├── config.py          # Centralized configuration management
│   │   │   ├── exceptions.py      # Custom exceptions
│   │   │   ├── constants.py       # Project constants
│   │   │   └── logging.py         # Logging configuration
│   │   │
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # Base classes and interfaces
│   │   │   ├── occupation.py      # Occupation model
│   │   │   ├── skill.py           # Skill model
│   │   │   ├── isco_group.py      # ISCO Group model
│   │   │   └── skill_collection.py # Skill Collection model
│   │   │
│   │   ├── database/
│   │   │   ├── __init__.py
│   │   │   ├── client.py          # Abstract database client
│   │   │   ├── weaviate/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── client.py      # Weaviate-specific client
│   │   │   │   ├── schema.py      # Schema management
│   │   │   │   └── operations.py  # CRUD operations
│   │   │   └── connection_pool.py # Connection pooling
│   │   │
│   │   ├── ingestion/
│   │   │   ├── __init__.py
│   │   │   ├── base.py            # Base ingestor interface
│   │   │   ├── csv_reader.py      # CSV reading utilities
│   │   │   ├── processors/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── occupation.py  # Occupation ingestion
│   │   │   │   ├── skill.py       # Skill ingestion
│   │   │   │   ├── isco_group.py  # ISCO group ingestion
│   │   │   │   └── relations.py   # Relationship ingestion
│   │   │   └── orchestrator.py    # Ingestion orchestration
│   │   │
│   │   ├── search/
│   │   │   ├── __init__.py
│   │   │   ├── engine.py          # Search engine interface
│   │   │   ├── semantic.py        # Semantic search implementation
│   │   │   ├── filters.py         # Search filters
│   │   │   └── results.py         # Result formatting
│   │   │
│   │   ├── embeddings/
│   │   │   ├── __init__.py
│   │   │   ├── generator.py       # Embedding generation
│   │   │   ├── models.py          # Model management
│   │   │   └── cache.py           # Embedding cache
│   │   │
│   │   ├── translation/
│   │   │   ├── __init__.py
│   │   │   ├── translator.py      # Translation engine
│   │   │   ├── models.py          # Translation models
│   │   │   └── batch.py           # Batch translation
│   │   │
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── validators.py      # Data validation
│   │       ├── formatters.py      # Output formatting
│   │       └── helpers.py         # General utilities
│   │
│   └── cli/
│       ├── __init__.py
│       ├── main.py               # CLI entry point
│       ├── commands/
│       │   ├── __init__.py
│       │   ├── ingest.py          # Ingestion commands
│       │   ├── search.py          # Search commands
│       │   ├── translate.py       # Translation commands
│       │   └── model.py           # Model management commands
│       └── utils.py               # CLI utilities
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_models.py
│   │   ├── test_ingestion.py
│   │   ├── test_search.py
│   │   └── test_embeddings.py
│   ├── integration/
│   │   ├── __init__.py
│   │   ├── test_weaviate.py
│   │   └── test_end_to_end.py
│   └── fixtures/
│       ├── __init__.py
│       └── sample_data.py
│
├── config/
│   ├── default.yaml              # Default configuration
│   ├── development.yaml          # Development overrides
│   ├── production.yaml           # Production overrides
│   └── schemas/                  # Keep existing schema files
│       ├── occupation.yaml
│       ├── skill.yaml
│       ├── isco_group.yaml
│       ├── skill_collection.yaml
│       └── skill_group.yaml
│
├── data/                         # Data directory
│   └── esco/                    # ESCO CSV files
│
├── logs/                        # Log files
│
├── docs/
│   ├── api/                     # API documentation
│   ├── architecture.md          # Architecture documentation
│   ├── development.md           # Development guide
│   └── deployment.md            # Deployment guide
│
├── scripts/
│   ├── setup.py                 # Setup script
│   ├── migrate.py               # Migration scripts
│   └── benchmark.py             # Performance benchmarking
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── .github/
│   └── workflows/
│       ├── tests.yml            # CI/CD pipeline
│       └── lint.yml             # Code quality checks
│
├── requirements/
│   ├── base.txt                 # Base requirements
│   ├── dev.txt                  # Development requirements
│   └── prod.txt                 # Production requirements
│
├── setup.py                     # Package setup
├── pyproject.toml              # Modern Python project config
├── .env.example                # Environment variables example
├── .gitignore
├── .dockerignore
├── README.md
└── LICENSE
```

## Key Improvements

### 1. **Modular Architecture**
- Separate concerns into distinct modules
- Clear interfaces between components
- Easy to extend with new features

### 2. **Configuration Management**
```python
# src/esco/core/config.py
from typing import Dict, Any, Optional
import yaml
import os
from pathlib import Path

class Config:
    """Centralized configuration management"""
    
    def __init__(self, env: Optional[str] = None):
        self.env = env or os.getenv('ESCO_ENV', 'development')
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration based on environment"""
        base_config = self._load_yaml('default.yaml')
        env_config = self._load_yaml(f'{self.env}.yaml')
        
        # Merge configurations
        return {**base_config, **env_config}
    
    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        config_path = Path(__file__).parent.parent.parent.parent / 'config' / filename
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)
```

### 3. **Database Abstraction**
```python
# src/esco/database/client.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

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
    def create_schema(self, schema: Dict[str, Any]) -> None:
        """Create database schema"""
        pass
    
    @abstractmethod
    def insert(self, collection: str, data: List[Dict[str, Any]]) -> None:
        """Insert data into collection"""
        pass
    
    @abstractmethod
    def search(self, query: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Search database"""
        pass
```

### 4. **Ingestion Pipeline**
```python
# src/esco/ingestion/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd

class BaseIngestor(ABC):
    """Base class for all data ingestors"""
    
    @abstractmethod
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate input data"""
        pass
    
    @abstractmethod
    def transform_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform data for ingestion"""
        pass
    
    @abstractmethod
    def ingest(self, data: List[Dict[str, Any]]) -> None:
        """Ingest transformed data"""
        pass
```

### 5. **Search Module**
```python
# src/esco/search/engine.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class SearchResult:
    """Search result container"""
    uri: str
    label: str
    description: str
    score: float
    type: str
    metadata: Optional[Dict[str, Any]] = None

class SearchEngine(ABC):
    """Abstract search engine interface"""
    
    @abstractmethod
    def search(
        self, 
        query: str, 
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10
    ) -> List[SearchResult]:
        """Perform search"""
        pass
```

### 6. **CLI Structure**
```python
# src/cli/main.py
import click
from .commands import ingest, search, translate, model

@click.group()
@click.option('--config', envvar='ESCO_CONFIG', help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, config, verbose):
    """ESCO Data Management and Search Tool"""
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['verbose'] = verbose

# Register command groups
cli.add_command(ingest.ingest_group)
cli.add_command(search.search_group)
cli.add_command(translate.translate_group)
cli.add_command(model.model_group)

if __name__ == '__main__':
    cli()
```

### 7. **Testing Structure**
```python
# tests/unit/test_search.py
import pytest
from esco.search.semantic import SemanticSearchEngine
from esco.models.skill import Skill

class TestSemanticSearch:
    @pytest.fixture
    def search_engine(self):
        return SemanticSearchEngine()
    
    def test_search_returns_results(self, search_engine):
        results = search_engine.search("python programming")
        assert len(results) > 0
        assert all(isinstance(r, SearchResult) for r in results)
```

### 8. **Error Handling**
```python
# src/esco/core/exceptions.py
class ESCOException(Exception):
    """Base exception for ESCO tool"""
    pass

class ConfigurationError(ESCOException):
    """Configuration related errors"""
    pass

class IngestionError(ESCOException):
    """Data ingestion errors"""
    pass

class SearchError(ESCOException):
    """Search related errors"""
    pass

class DatabaseError(ESCOException):
    """Database operation errors"""
    pass
```

## Implementation Plan

1. **Phase 1: Core Refactoring**
   - Extract configuration management
   - Create base classes and interfaces
   - Set up proper package structure

2. **Phase 2: Module Separation**
   - Split monolithic files into focused modules
   - Implement database abstraction layer
   - Refactor ingestion pipeline

3. **Phase 3: Testing & Documentation**
   - Add comprehensive unit tests
   - Create integration tests
   - Update documentation

4. **Phase 4: Performance & Optimization**
   - Implement connection pooling
   - Add caching layer
   - Optimize batch operations

5. **Phase 5: DevOps & Deployment**
   - Set up CI/CD pipeline
   - Create deployment scripts
   - Add monitoring and logging

## Benefits

1. **Maintainability**: Clear separation of concerns
2. **Testability**: Easy to mock and test components
3. **Extensibility**: Simple to add new features
4. **Performance**: Better resource management
5. **Developer Experience**: Clear structure and documentation