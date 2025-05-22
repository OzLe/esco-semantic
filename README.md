# ESCO Data Management and Search Tool

This tool is designed for managing, searching, and translating the ESCO (European Skills, Competences, Qualifications and Occupations) taxonomy using both Neo4j graph database and Weaviate vector database. It provides a unified command-line interface for data ingestion, semantic search, and translation capabilities.

## Features

- **Dual Database Architecture**
  - Neo4j for graph-based relationships and complex queries
  - Weaviate for high-performance vector search
  - Automatic synchronization between both databases

- **Semantic Search**
  - Vector-based semantic search using HNSW index
  - Support for both occupations and skills
  - Configurable similarity thresholds
  - Rich result formatting with related skills

- **Data Management**
  - Batch ingestion with automatic retries
  - Cross-references between entities
  - Support for multiple languages
  - Efficient vector indexing

- **Translation**
  - Neural machine translation support
  - Batch processing capabilities
  - Multiple language pair support

## Disclaimer

This tool is provided "AS IS", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages or other liability, whether in an action of contract, tort or otherwise, arising from, out of or in connection with the software or the use or other dealings in the software.

## Prerequisites

- Python 3.8 or higher
- Neo4j Database (version 5.x) or Neo4j AuraDB
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

3. Ingest data:
```bash
# Ingest into Neo4j
python src/esco_cli.py ingest --config config/neo4j_config.yaml

# Ingest into Weaviate
python src/esco_cli.py ingest-weaviate --config config/weaviate_config.yaml
```

4. Search the data:
```bash
# Search using Neo4j
python src/esco_cli.py search --query "python programming"

# Search using Weaviate
python src/esco_cli.py search-weaviate --query "python programming"
```

## Architecture

### Database Integration

The tool uses a dual-database architecture:

1. **Neo4j**
   - Stores graph relationships
   - Handles complex queries
   - Manages entity properties
   - Supports Cypher queries

2. **Weaviate**
   - Stores vector embeddings
   - Provides semantic search
   - Manages cross-references
   - Uses HNSW indexing

### Data Flow

```mermaid
graph TD
    A[ESCO CSV Files] --> B[Ingestion]
    B --> C[Neo4j (Graph)]
    C --> D[Weaviate (Vector)]
    D --> E[Search API]
```

## Configuration

### Neo4j Configuration

Create `config/neo4j_config.yaml`:
```yaml
default:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "your-password"
  max_retries: 3
  retry_delay: 5
  max_connection_lifetime: 3600
  max_connection_pool_size: 50
  connection_timeout: 30
```

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
# Ingest into Neo4j
python src/esco_cli.py ingest --config config/neo4j_config.yaml

# Ingest into Weaviate
python src/esco_cli.py ingest-weaviate --config config/weaviate_config.yaml

# Delete and re-ingest
python src/esco_cli.py ingest-weaviate --config config/weaviate_config.yaml --delete-all
```

### Semantic Search

```bash
# Search using Neo4j
python src/esco_cli.py search --query "python programming" --type Skill

# Search using Weaviate
python src/esco_cli.py search-weaviate --query "python programming" --limit 5

# Search with options
python src/esco_cli.py search-weaviate \
    --query "python programming" \
    --limit 5 \
    --certainty 0.7

# JSON output
python src/esco_cli.py search-weaviate --query "python programming" --json
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

### Neo4j Graph Model

Nodes:
- `Skill`: Individual skills
- `SkillGroup`: Groups of skills
- `Occupation`: Occupations
- `ISCOGroup`: ISCO classification groups

Relationships:
- `BROADER_THAN`: Hierarchical relationships
- `PART_OF_ISCOGROUP`: Occupation-ISCO links
- `ESSENTIAL_FOR`: Essential skill links
- `OPTIONAL_FOR`: Optional skill links
- `RELATED_SKILL`: Related skill links

### Weaviate Vector Model

Collections:
1. **Occupation**
   - Properties:
     - `conceptUri` (string)
     - `preferredLabel` (text)
     - `description` (text)
   - Vector: 384-dimensional embedding
   - Cross-references:
     - `hasEssentialSkill` → Skill
     - `hasOptionalSkill` → Skill

2. **Skill**
   - Properties:
     - `conceptUri` (string)
     - `preferredLabel` (text)
     - `description` (text)
   - Vector: 384-dimensional embedding

## Performance

### Batch Processing
- Neo4j: 50,000 rows per batch
- Weaviate: 100 rows per batch
- Automatic retries for failed operations

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
- Neo4j: HTTP endpoint (7474)
- Weaviate: Ready endpoint (8080)
- Automatic container restart

### Logging
- Structured logging to `logs/esco.log`
- Console output with color coding
- Error tracking and reporting

## Troubleshooting

### Common Issues

1. **Connection Problems**
   - Verify database URLs
   - Check credentials
   - Ensure ports are accessible

2. **Ingestion Failures**
   - Check CSV file formats
   - Verify file permissions
   - Monitor memory usage

3. **Search Issues**
   - Verify embeddings generation
   - Check index status
   - Monitor search latency

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
# Neo4j
curl http://localhost:7474

# Weaviate
curl http://localhost:8080/v1/.well-known/ready
```

## Development

### Project Structure
```
ESCO-Ingest/
├── config/
│   ├── neo4j_config.yaml
│   └── weaviate_config.yaml
├── src/
│   ├── esco_cli.py
│   ├── weaviate_client.py
│   ├── weaviate_search.py
│   └── ...
├── ESCO/
│   └── *.csv
└── docker-compose.yml
```

### Adding Features

1. **New Search Engine**
   - Implement search interface
   - Add configuration options
   - Update CLI commands

2. **Data Processing**
   - Add new data sources
   - Implement transformers
   - Update ingestion pipeline

3. **API Integration**
   - Add new endpoints
   - Implement authentication
   - Add rate limiting

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- ESCO for the taxonomy data
- Neo4j for the graph database
- Weaviate for the vector database
- All contributors and users 