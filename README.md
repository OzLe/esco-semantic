# ESCO Data Management and Search Tool

This tool is designed for managing, searching, and translating the ESCO (European Skills, Competences, Qualifications and Occupations) taxonomy using Weaviate vector database. It provides a unified command-line interface for data ingestion, semantic search, and translation capabilities.

## Features

- **Vector Database Architecture**
  - Weaviate for high-performance vector search
  - HNSW indexing for fast similarity search
  - Rich cross-references between entities
  - Configurable vector index parameters
  - Support for multiple vectorizers (Sentence Transformers, Contextuary)

- **Semantic Search**
  - Vector-based semantic search using HNSW index
  - Support for multiple entity types (Skills, Occupations, ISCO Groups)
  - Configurable similarity thresholds
  - Rich result formatting with related entities
  - Profile-based search with complete occupation details

- **Data Management**
  - Batch ingestion with automatic retries
  - Cross-references between entities
  - Support for multiple languages
  - Efficient vector indexing
  - Selective class ingestion
  - Optional relationship creation

- **Translation**
  - Neural machine translation support
  - Batch processing capabilities
  - Multiple language pair support

## Prerequisites

- Python 3.8 or higher
- Weaviate Vector Database (version 1.25)
- ESCO CSV files (v1.2.0 or compatible)
- Docker and Docker Compose (for containerized deployment)

## Quick Start

1. Clone the repository:
```bash
git clone <repository-url>
cd ESCO-Ingest
```

2. Start the services:
```bash
docker-compose up -d
```

3. Switch vectorizer (optional):
```bash
# Use sentence transformers (default)
./switch_vectorizer.sh sentence-transformers

# Use contextuary
./switch_vectorizer.sh contextuary
```

4. Download the translation model:
```bash
python src/esco_cli.py download-model
```

5. Ingest data:
```bash
# Ingest all data
python src/esco_cli.py ingest --config config/weaviate_config.yaml

# Ingest specific classes
python src/esco_cli.py ingest --config config/weaviate_config.yaml --classes Skill Occupation

# Ingest with options
python src/esco_cli.py ingest --config config/weaviate_config.yaml --delete-all --skip-relations
```

6. Search the data:
```bash
# Basic search
python src/esco_cli.py search --query "python programming"

# Advanced search
python src/esco_cli.py search --query "python programming" --type Skill --limit 5 --certainty 0.7 --profile-search

# JSON output
python src/esco_cli.py search --query "python programming" --json
```

## Architecture

### Database Integration

The tool uses Weaviate as its primary database:

1. **Weaviate**
   - Stores vector embeddings
   - Provides semantic search
   - Manages cross-references
   - Uses HNSW indexing
   - Supports multiple languages
   - Handles complex relationships
   - Supports multiple vectorizers:
     - Sentence Transformers (all-MiniLM-L6-v2)
     - Contextuary

### Vectorizer Configuration

The system supports two vectorizers:

1. **Sentence Transformers**
   - Model: all-MiniLM-L6-v2
   - 384-dimensional embeddings
   - Optimized for semantic similarity
   - Default configuration

2. **Contextuary**
   - Specialized for contextual understanding
   - Alternative embedding model
   - Can be switched using the provided script

To switch between vectorizers:
```bash
# Switch to sentence transformers
./switch_vectorizer.sh sentence-transformers

# Switch to contextuary
./switch_vectorizer.sh contextuary
```

Note: Switching vectorizers requires re-ingesting the data as the embeddings will be different.

### Data Flow

```mermaid
graph TD
    A[ESCO CSV Files] --> B[Ingestion]
    B --> C[Weaviate (Vector)]
    C --> D[Search API]
    E[Translation Model] --> F[Translation API]
```

## Configuration

### Weaviate Configuration

Create `config/weaviate_config.yaml`:
```yaml
default:
  url: "http://localhost:8080"
  vector_index_config:
    distance: "cosine"
    efConstruction: 128
    maxConnections: 64
  batch_size: 100
  retry_attempts: 3
  retry_delay: 5
```

## Usage

### Data Ingestion

```bash
# Ingest all data
python src/esco_cli.py ingest --config config/weaviate_config.yaml

# Ingest specific classes
python src/esco_cli.py ingest --config config/weaviate_config.yaml --classes Skill Occupation

# Delete and re-ingest
python src/esco_cli.py ingest --config config/weaviate_config.yaml --delete-all

# Skip relationship creation
python src/esco_cli.py ingest --config config/weaviate_config.yaml --skip-relations
```

### Semantic Search

```bash
# Basic search
python src/esco_cli.py search --query "python programming"

# Search with options
python src/esco_cli.py search \
    --query "python programming" \
    --type Skill \
    --limit 5 \
    --certainty 0.7 \
    --profile-search

# JSON output
python src/esco_cli.py search --query "python programming" --json
```

### Translation

```bash
# Translate skills
python src/esco_cli.py translate --type Skill --property prefLabel

# Translate with options
python src/esco_cli.py translate \
    --type Skill \
    --property prefLabel \
    --batch-size 50 \
    --device mps
```

## Data Models

### Weaviate Vector Model

Collections:
1. **Occupation**
   - Properties:
     - `conceptUri` (string)
     - `code` (string)
     - `preferredLabel_en` (text)
     - `description_en` (text)
     - `definition_en` (text)
     - `iscoGroup` (string)
     - `altLabels_en` (text[])
   - Vector: 384-dimensional embedding
   - Cross-references:
     - `hasEssentialSkill` → Skill
     - `hasOptionalSkill` → Skill

2. **Skill**
   - Properties:
     - `conceptUri` (string)
     - `code` (string)
     - `preferredLabel_en` (text)
     - `description_en` (text)
     - `definition_en` (text)
     - `skillType` (string)
     - `reuseLevel` (string)
     - `altLabels_en` (text[])
   - Vector: 384-dimensional embedding

3. **ISCOGroup**
   - Properties:
     - `conceptUri` (string)
     - `code` (string)
     - `preferredLabel_en` (text)
     - `description_en` (text)
   - Vector: 384-dimensional embedding

4. **SkillCollection**
   - Properties:
     - `conceptUri` (string)
     - `preferredLabel_en` (text)
     - `description_en` (text)
   - Vector: 384-dimensional embedding

5. **SkillGroup**
   - Properties:
     - `conceptUri` (string)
     - `preferredLabel_en` (text)
     - `description_en` (text)
   - Vector: 384-dimensional embedding

## Performance

### Batch Processing
- Configurable batch sizes (default: 100)
- Automatic retries for failed operations
- Efficient vector storage and indexing

### Search Performance
- HNSW index for fast vector search
- Configurable index parameters
- Support for multiple devices (CPU, CUDA, MPS)

### Memory Usage
- Efficient vector storage
- Configurable batch sizes
- Automatic garbage collection

## Monitoring

### Health Checks
- Weaviate: Ready endpoint (8080)
- Automatic container restart

### Logging
- Structured logging to `logs/esco.log`
- Console output with color coding
- Error tracking and reporting

## Troubleshooting

### Common Issues

1. **Connection Problems**
   - Verify Weaviate URL
   - Check service health
   - Ensure ports are accessible

2. **Ingestion Failures**
   - Check CSV file formats
   - Verify file permissions
   - Monitor memory usage
   - Check batch size settings

3. **Search Issues**
   - Verify embeddings generation
   - Check index status
   - Monitor search latency
   - Adjust certainty threshold

### Debugging

1. Enable verbose logging:
```bash
export LOG_LEVEL=DEBUG
```

2. Check container logs:
```bash
docker-compose logs -f
```

3. Monitor database status:
```bash
curl http://localhost:8080/v1/.well-known/ready
```

## Development

### Project Structure
```
ESCO-Ingest/
├── config/
│   └── weaviate_config.yaml
├── src/
│   ├── esco_cli.py
│   ├── weaviate_client.py
│   ├── weaviate_search.py
│   ├── weaviate_semantic_search.py
│   ├── embedding_utils.py
│   ├── esco_translate.py
│   ├── esco_ingest.py
│   ├── download_model.py
│   └── logging_config.py
├── resources/
│   └── schemas/
│       ├── occupation.yaml
│       ├── skill.yaml
│       ├── isco_group.yaml
│       ├── skill_collection.yaml
│       ├── skill_group.yaml
│       └── references.yaml
└── docker-compose.yml
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- ESCO for the taxonomy data
- Weaviate for the vector database
- All contributors and users 