# ESCO Data Ingestion Tool

This tool is designed for visualizing and testing the ESCO (European Skills, Competences, Qualifications and Occupations) taxonomy in a Neo4j graph database. It processes various CSV files containing skills, occupations, and their relationships, creating a comprehensive knowledge graph for exploration and analysis purposes.

## Disclaimer

This tool is provided "AS IS", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages or other liability, whether in an action of contract, tort or otherwise, arising from, out of or in connection with the software or the use or other dealings in the software.

## Prerequisites

- Python 3.8 or higher
- Neo4j Database (version 5.x)
- ESCO CSV files (v1.2.0 or compatible)

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd ESCO-Ingest
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Before running the ingestion tool, update the configuration variables in `esco_ingest.py`:

```python
NEO4J_URI = "bolt://localhost:7687"  # Update with your Neo4j URI
NEO4J_USER = "neo4j"                 # Update with your username
NEO4J_PASSWORD = "your-password"     # Update with your password
ESCO_DIR = "ESCO"                    # Directory containing ESCO CSV files
```

## Directory Structure

Place your ESCO CSV files in the `ESCO` directory. The tool expects the following files:
- `skillGroups_en.csv`
- `skills_en.csv`
- `occupations_en.csv`
- `ISCOGroups_en.csv`
- `broaderRelationsSkillPillar_en.csv`
- `broaderRelationsOccPillar_en.csv`
- `occupationSkillRelations_en.csv`
- `skillSkillRelations_en.csv`

## Usage

### Data Ingestion

Run the ingestion tool:
```bash
python esco_ingest.py
```

The tool will:
1. Delete all existing data from the Neo4j database
2. Create necessary constraints
3. Ingest skill groups
4. Ingest individual skills
5. Ingest occupations
6. Ingest ISCO groups
7. Create skill hierarchies
8. Create ISCO hierarchies
9. Create occupation-ISCO mappings
10. Create occupation-skill relations
11. Create skill-skill relations
12. Generate and store embeddings for semantic search

### Semantic Search

The tool includes a command-line interface for semantic search. You can use it in two modes:

1. Full pipeline (default):
```bash
python esco_search_cli.py --query "your search query" --password "your_password"
```

2. Search-only mode (when data is already indexed):
```bash
python esco_search_cli.py --query "your search query" --password "your_password" --search-only
```

Additional search options:
- `--type`: Specify node type to search (Skill, Occupation, or Both)
- `--limit`: Maximum number of results to return
- `--related`: Get related graph for the top result
- `--json`: Output results in JSON format

Example with all options:
```bash
python esco_search_cli.py \
    --query "machine learning" \
    --type Skill \
    --limit 5 \
    --related \
    --json \
    --search-only \
    --password "your_password"
```

The search functionality uses the `all-MiniLM-L6-v2` model for generating embeddings, which provides a good balance between performance and quality. The search results include:
- Semantic similarity scores
- Node descriptions
- Related entities (when using --related)
- Graph context for each result

## Data Model

The tool creates the following node types and relationships:

### Nodes
- `Skill`: Represents individual skills
- `SkillGroup`: Represents groups of skills
- `Occupation`: Represents occupations
- `ISCOGroup`: Represents ISCO classification groups

### Relationships
- `BROADER_THAN`: Hierarchical relationships between skills and ISCO groups
- `PART_OF_ISCOGROUP`: Links occupations to their ISCO groups
- `ESSENTIAL_FOR`: Links essential skills to occupations
- `OPTIONAL_FOR`: Links optional skills to occupations
- `RELATED_SKILL`: Links related skills with their relationship type

## Performance

The tool processes data in batches of 50,000 rows to optimize memory usage and performance. Progress is displayed using progress bars for each processing step.

For semantic search:
- Uses efficient vector indexes in Neo4j
- Implements cosine similarity for matching
- Supports batch processing for embedding generation
- Includes search-only mode for faster subsequent searches

## Error Handling

The tool includes comprehensive error handling and logging. Check the console output for any issues during the ingestion process.

## Logging

Logs are output to the console with timestamps and log levels. The tool logs:
- Start and completion of each ingestion step
- Any errors encountered during the process
- Overall completion status
- Search operations and results

## Troubleshooting

If you encounter issues:

1. Verify Neo4j connection details
2. Ensure all required CSV files are present in the ESCO directory
3. Check file permissions
4. Verify CSV file formats match the expected structure
5. Check Neo4j logs for any database-specific issues
6. For search issues, verify that embeddings are properly generated
7. Check if vector indexes are created in Neo4j

## License

This project is licensed under the MIT License - see the LICENSE file for details.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Contributing

This is a simple tool for testing and visualization purposes. While contributions are welcome, please note that this is not intended for production use. If you find any issues or have suggestions for improvements, feel free to open an issue or submit a pull request. 