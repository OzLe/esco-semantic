# ESCO Data Management and Search Tool - Project Overview

## Executive Summary

The ESCO Data Management and Search Tool is a comprehensive Python application designed for managing, searching, and translating the ESCO (European Skills, Competences, Qualifications and Occupations) taxonomy using Weaviate vector database. The system provides a unified command-line interface for data ingestion, semantic search, and translation capabilities with robust containerized deployment.

## Architecture Overview

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Layer     │    │  Container Init │    │  Search Service │
│  (esco_cli.py)  │    │  (init_*.py)    │    │ (search_*.py)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Service Layer  │
                    │ (ingestion_*)   │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Data Access     │
                    │ (repositories)  │
                    └─────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Weaviate DB   │    │  File System    │    │  Translation    │
│   (Vector)      │    │  (CSV Data)     │    │   Models        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Service Layer Pattern

The application follows a clean Service Layer architecture that eliminates code duplication:

- **Service Layer**: Centralized business logic in `src/services/`
- **Data Models**: Structured data models in `src/models/`
- **Data Access Layer**: Pure data operations in `src/repositories/`
- **Multiple Interfaces**: CLI, Container Init, and Search Service all use the same service layer

## Domain Model

### Core Entities

The system models the ESCO taxonomy with the following primary entities:

#### 1. **Occupation**
- Represents ESCO occupation concepts
- Properties: conceptUri, preferredLabel_en, description_en, code, altLabels_en
- Relationships: memberOfISCOGroup, hasEssentialSkill, hasOptionalSkill, broaderOccupation

#### 2. **Skill**
- Represents skills, competences, and knowledge concepts
- Properties: conceptUri, preferredLabel_en, description_en, skillType, reuseLevel
- Relationships: broaderSkill, hasRelatedSkill, memberOfSkillCollection

#### 3. **ISCOGroup**
- Represents ISCO (International Standard Classification of Occupations) groups
- Properties: conceptUri, code, preferredLabel_en, description_en
- Relationships: hasOccupation, broaderISCOGroup

#### 4. **SkillCollection**
- Represents thematic collections of skills
- Properties: conceptUri, preferredLabel_en, description_en
- Relationships: hasSkill

#### 5. **SkillGroup**
- Represents hierarchical skill groupings
- Properties: conceptUri, preferredLabel_en, description_en
- Relationships: hasSkill, broaderSkillGroup

### Relationship Taxonomy

```
Occupation ↔ ISCOGroup (esco:memberOfGroup)
Occupation ↔ Skill (esco:hasEssentialSkill / hasOptionalSkill)
Occupation ↔ Occupation (skos:broader - hierarchical)
Skill ↔ Skill (skos:broader - hierarchical)
Skill ↔ Skill (esco:relatedSkill)
SkillCollection ↔ Skill (esco:hasSkill)
```

## Technology Stack

### Core Technologies

- **Python 3.10+**: Primary programming language
- **Weaviate**: Vector database for semantic search and data storage
- **Docker & Docker Compose**: Containerization and orchestration
- **YAML**: Configuration management

### Python Dependencies

#### Data Processing
- **pandas**: CSV data manipulation and processing
- **numpy**: Numerical computations
- **tqdm**: Progress tracking
- **PyYAML**: YAML configuration parsing

#### Machine Learning & NLP
- **sentence-transformers**: Semantic embeddings generation
- **torch**: PyTorch for model operations
- **transformers**: Hugging Face transformers library
- **accelerate**: Model acceleration

#### Vector Database
- **weaviate-client**: Weaviate database integration

#### CLI & Utilities
- **click**: Command-line interface framework
- **pyarrow**: Data serialization

### Infrastructure Components

#### Weaviate Configuration
- **Version**: 1.31.0
- **Vectorizer**: Sentence Transformers (all-MiniLM-L6-v2) or Contextuary
- **Index**: HNSW for fast similarity search
- **Batch Processing**: Configurable batch sizes

#### Docker Services
- **weaviate**: Main vector database
- **t2v-transformers**: Transformer inference service
- **esco-init**: Initialization container
- **esco-search**: Search service
- **esco-cli**: CLI container

## Key Features

### 1. **Service Layer Architecture**
- Centralized business logic eliminates duplication
- Single source of truth for ingestion operations
- Structured data models for consistent state management
- Comprehensive error handling and validation

### 2. **Intelligent Ingestion State Management**
- Automatic status detection before starting operations
- Stale process recovery (handles interrupted ingestion >1 hour old)
- Prerequisites validation (connectivity, schema, data files)
- Progress tracking with real-time updates and heartbeat monitoring
- Error recovery with structured error handling

### 3. **Vector Database Architecture**
- Weaviate for high-performance vector search
- HNSW indexing for fast similarity search
- Rich cross-references between entities
- Configurable vector index parameters
- Multiple vectorizer support

### 4. **Semantic Search Capabilities**
- Vector-based semantic search using HNSW index
- Support for multiple entity types
- Configurable similarity thresholds
- Rich result formatting with related entities
- Profile-based search with complete occupation details

### 5. **Data Management**
- Batch ingestion with automatic retries
- Cross-references between entities
- Support for multiple languages
- Efficient vector indexing
- Selective class ingestion
- Optional relationship creation

### 6. **Translation Support**
- Neural machine translation (English to Hebrew)
- Batch processing capabilities
- MarianMT model integration

## Project Structure

```
ESCO-Ingest/
├── config/                     # Configuration files
│   └── weaviate_config.yaml
├── src/                       # Source code
│   ├── models/               # Data models (Service Layer)
│   │   ├── __init__.py
│   │   └── ingestion_models.py
│   ├── services/             # Business logic (Service Layer)
│   │   ├── __init__.py
│   │   └── ingestion_service.py
│   ├── repositories/         # Data access layer
│   │   ├── base_repository.py
│   │   ├── weaviate_repository.py
│   │   ├── occupation_repository.py
│   │   ├── skill_repository.py
│   │   └── repository_factory.py
│   ├── esco_cli.py          # CLI interface
│   ├── init_ingestion.py    # Container initialization
│   ├── esco_ingest.py       # Data ingestion logic
│   ├── esco_weaviate_client.py # Weaviate client
│   ├── weaviate_semantic_search.py # Search functionality
│   ├── esco_translate.py    # Translation capabilities
│   └── logging_config.py    # Logging configuration
├── resources/               # Schema definitions
│   └── schemas/
├── scripts/                # Shell scripts
│   └── init_ingestion.sh
├── tests/                  # Test suites
├── docker-compose.yml      # Container orchestration
├── Dockerfile             # Container definition
└── requirements.txt       # Python dependencies
```

## High-Level Goals

### Primary Objectives

1. **Data Integration**: Seamlessly ingest and manage ESCO taxonomy data
2. **Semantic Search**: Provide powerful vector-based search capabilities
3. **Scalability**: Handle large-scale ESCO datasets efficiently
4. **Reliability**: Ensure robust, fault-tolerant operations
5. **Usability**: Offer intuitive CLI and programmatic interfaces

### Technical Goals

1. **Performance**: Sub-second semantic search responses
2. **Consistency**: Maintain data integrity across all operations
3. **Observability**: Comprehensive logging and monitoring
4. **Flexibility**: Support multiple deployment scenarios
5. **Maintainability**: Clean, well-documented, testable code

## Deployment Scenarios

### 1. **Development Environment**
- Local Docker Compose setup
- Interactive CLI for data exploration
- Hot-reloading for development

### 2. **Production Environment**
- Container-based deployment
- Non-interactive ingestion
- Health monitoring and alerting
- Horizontal scaling capabilities

### 3. **Research Environment**
- Jupyter notebook integration
- Batch processing capabilities
- Custom analysis workflows

## Data Flow

### Ingestion Pipeline

1. **Initialization**: Schema setup and validation
2. **Entity Ingestion**: Load core entities (Occupations, Skills, etc.)
3. **Relationship Creation**: Establish cross-references
4. **Vector Generation**: Create semantic embeddings
5. **Indexing**: Build search indices
6. **Validation**: Verify data integrity

### Search Pipeline

1. **Query Processing**: Parse and validate search queries
2. **Vector Generation**: Create query embeddings
3. **Similarity Search**: Execute vector search with HNSW
4. **Result Enrichment**: Add related entities and metadata
5. **Response Formatting**: Structure results for consumption

## Quality Assurance

### Testing Strategy
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end workflow testing
- **Performance Tests**: Load and stress testing
- **Container Tests**: Docker environment validation

### Code Quality
- **Type Hints**: Comprehensive type annotations
- **Documentation**: Google-style docstrings
- **Linting**: Code style enforcement
- **Error Handling**: Structured exception management

## Future Enhancements

### Planned Features
1. **Multi-language Support**: Extend beyond English and Hebrew
2. **Advanced Analytics**: Graph analysis and metrics
3. **API Gateway**: RESTful API for external integration
4. **Real-time Updates**: Streaming data ingestion
5. **Machine Learning**: Skill recommendation algorithms

### Technical Improvements
1. **Caching Layer**: Redis for frequently accessed data
2. **Message Queue**: Asynchronous processing with Celery
3. **Monitoring**: Prometheus and Grafana integration
4. **Security**: Authentication and authorization
5. **Backup**: Automated data backup and recovery

## Development Workflow

### Getting Started
1. Clone repository
2. Start Docker services: `docker-compose up -d`
3. Install dependencies: `pip install -r requirements.txt`
4. Run ingestion: `python src/esco_cli.py ingest`
5. Test search: `python src/esco_cli.py search --query "python programming"`

### Development Practices
- Feature branches for all changes
- Pull request reviews required
- Automated testing on commits
- Documentation updates with code changes
- Semantic versioning for releases