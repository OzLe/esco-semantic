# ESCO Data Ingestion Tool

This tool ingests ESCO (European Skills, Competences, Qualifications and Occupations) data into a Neo4j graph database. It processes various CSV files containing skills, occupations, and their relationships, creating a comprehensive knowledge graph.

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

## Error Handling

The tool includes comprehensive error handling and logging. Check the console output for any issues during the ingestion process.

## Logging

Logs are output to the console with timestamps and log levels. The tool logs:
- Start and completion of each ingestion step
- Any errors encountered during the process
- Overall completion status

## Troubleshooting

If you encounter issues:

1. Verify Neo4j connection details
2. Ensure all required CSV files are present in the ESCO directory
3. Check file permissions
4. Verify CSV file formats match the expected structure
5. Check Neo4j logs for any database-specific issues

## License

[Add your license information here]

## Contributing

[Add contribution guidelines if applicable] 