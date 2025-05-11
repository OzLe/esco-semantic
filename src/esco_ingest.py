import os
from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import logging



# ESCO v1.2.0 (English) – CSV classification import for Neo4j 5.x
# Oz Levi
# 2025-05-11

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ESCOIngest:
    def __init__(self, uri, user, password, esco_dir):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.esco_dir = esco_dir
        self.batch_size = 50000

    def close(self):
        self.driver.close()

    def delete_all_data(self):
        """Delete all nodes and relationships from the database"""
        with self.driver.session() as session:
            # First drop all constraints
            constraints = [
                "DROP CONSTRAINT skill_uri IF EXISTS",
                "DROP CONSTRAINT skillgroup_uri IF EXISTS",
                "DROP CONSTRAINT occupation_uri IF EXISTS",
                "DROP CONSTRAINT iscogroup_uri IF EXISTS",
                "DROP CONSTRAINT iscogroup_code IF EXISTS"
            ]
            for constraint in constraints:
                session.run(constraint)
            
            # Then delete all nodes and relationships
            query = "MATCH (n) DETACH DELETE n"
            session.run(query)
            logger.info("Deleted all data from the database")

    def create_constraints(self):
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT skill_uri IF NOT EXISTS FOR (s:Skill) REQUIRE s.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT skillgroup_uri IF NOT EXISTS FOR (sg:SkillGroup) REQUIRE sg.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT occupation_uri IF NOT EXISTS FOR (o:Occupation) REQUIRE o.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT iscogroup_uri IF NOT EXISTS FOR (g:ISCOGroup) REQUIRE g.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT iscogroup_code IF NOT EXISTS FOR (g:ISCOGroup) REQUIRE g.code IS UNIQUE"
            ]
            for constraint in constraints:
                session.run(constraint)
            logger.info("Created constraints")

    def process_csv_in_batches(self, file_path, process_func):
        """Process a CSV file in batches"""
        df = pd.read_csv(file_path)
        total_rows = len(df)
        
        for start_idx in tqdm(range(0, total_rows, self.batch_size), desc=f"Processing {os.path.basename(file_path)}"):
            end_idx = min(start_idx + self.batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            process_func(batch)

    def ingest_skill_groups(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MERGE (sg:Skill:SkillGroup {conceptUri: $conceptUri})
                    SET sg += $properties
                    """
                    session.run(query, conceptUri=row['conceptUri'], properties=row.to_dict())

        file_path = os.path.join(self.esco_dir, 'skillGroups_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested skill groups")

    def ingest_skills(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MERGE (s:Skill {conceptUri: $conceptUri})
                    SET s += $properties
                    """
                    session.run(query, conceptUri=row['conceptUri'], properties=row.to_dict())

        file_path = os.path.join(self.esco_dir, 'skills_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested skills")

    def ingest_occupations(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MERGE (o:Occupation {conceptUri: $conceptUri})
                    SET o += $properties
                    """
                    session.run(query, conceptUri=row['conceptUri'], properties=row.to_dict())

        file_path = os.path.join(self.esco_dir, 'occupations_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested occupations")

    def ingest_isco_groups(self):
        def process_batch(batch):
            with self.driver.session() as session:
                # First, ensure we don't have duplicate codes
                batch = batch.drop_duplicates(subset=['code'], keep='first')
                
                for _, row in batch.iterrows():
                    # First create the node without the code
                    query = """
                    MERGE (g:ISCOGroup {conceptUri: $conceptUri})
                    SET g += $properties
                    """
                    properties = row.to_dict()
                    code = properties.pop('code', None)  # Remove code from properties
                    session.run(query, conceptUri=row['conceptUri'], properties=properties)
                    
                    # Then update the code if it exists
                    if code is not None:
                        update_query = """
                        MATCH (g:ISCOGroup {conceptUri: $conceptUri})
                        SET g.code = $code
                        """
                        session.run(update_query, conceptUri=row['conceptUri'], code=code)

        file_path = os.path.join(self.esco_dir, 'ISCOGroups_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested ISCO groups")

    def create_skill_hierarchy(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MATCH (child:Skill {conceptUri: $childUri})
                    MATCH (parent:Skill {conceptUri: $parentUri})
                    MERGE (parent)-[:BROADER_THAN]->(child)
                    """
                    session.run(query, childUri=row['conceptUri'], parentUri=row['broaderUri'])

        file_path = os.path.join(self.esco_dir, 'broaderRelationsSkillPillar_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created skill hierarchy")

    def create_isco_hierarchy(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MATCH (child:ISCOGroup {conceptUri: $childUri})
                    MATCH (parent:ISCOGroup {conceptUri: $parentUri})
                    MERGE (parent)-[:BROADER_THAN]->(child)
                    """
                    session.run(query, childUri=row['conceptUri'], parentUri=row['broaderUri'])

        file_path = os.path.join(self.esco_dir, 'broaderRelationsOccPillar_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created ISCO hierarchy")

    def create_occupation_isco_mapping(self):
        with self.driver.session() as session:
            query = """
            MATCH (o:Occupation)
            WITH o, o.iscoGroup as iscoCode
            MATCH (g:ISCOGroup {code: iscoCode})
            MERGE (o)-[:PART_OF_ISCOGROUP]->(g)
            """
            session.run(query)
            logger.info("Created occupation-ISCO mapping")

    def create_occupation_skill_relations(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    if row['relationType'] == 'essential':
                        query = """
                        MATCH (s:Skill {conceptUri: $skillUri})
                        MATCH (o:Occupation {conceptUri: $occupationUri})
                        MERGE (s)-[:ESSENTIAL_FOR]->(o)
                        """
                    else:
                        query = """
                        MATCH (s:Skill {conceptUri: $skillUri})
                        MATCH (o:Occupation {conceptUri: $occupationUri})
                        MERGE (s)-[:OPTIONAL_FOR]->(o)
                        """
                    session.run(query, 
                              skillUri=row['skillUri'],
                              occupationUri=row['occupationUri'])

        file_path = os.path.join(self.esco_dir, 'occupationSkillRelations_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created occupation-skill relations")

    def create_skill_skill_relations(self):
        def process_batch(batch):
            with self.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MATCH (a:Skill {conceptUri: $originalSkillUri})
                    MATCH (b:Skill {conceptUri: $relatedSkillUri})
                    MERGE (a)-[:RELATED_SKILL {type: $relationType}]->(b)
                    """
                    session.run(query, 
                              originalSkillUri=row['originalSkillUri'],
                              relatedSkillUri=row['relatedSkillUri'],
                              relationType=row['relationType'])

        file_path = os.path.join(self.esco_dir, 'skillSkillRelations_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created skill-skill relations")

    def create_vector_indexes(self):
        """Create vector indexes for Neo4j vector search"""
        with self.driver.session() as session:
            # Create vector indexes for skills
            session.run("""
                CREATE VECTOR INDEX skill_embedding IF NOT EXISTS
                FOR (s:Skill)
                ON s.embedding
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 384,  # For all-MiniLM-L6-v2
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            
            # Create vector indexes for occupations
            session.run("""
                CREATE VECTOR INDEX occupation_embedding IF NOT EXISTS
                FOR (o:Occupation)
                ON o.embedding
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 384,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
        logger.info("Created vector indexes for semantic search")

    def generate_and_store_embeddings(self, embedding_util):
        """Generate embeddings for all skills and occupations"""
        # Process skills
        logger.info("Generating embeddings for skills")
        with self.driver.session() as session:
            # Get skills in batches
            query = "MATCH (s:Skill) RETURN s.conceptUri as uri, s.preferredLabel as label, s.description as description, s.altLabels as altLabels"
            result = session.run(query)
            
            for record in tqdm(result, desc="Embedding skills"):
                node_data = {
                    'preferredLabel': record['label'],
                    'description': record['description'],
                    'altLabels': record['altLabels']
                }
                
                embedding = embedding_util.generate_node_embedding(node_data)
                if embedding:
                    # Store embedding back in Neo4j
                    session.run(
                        "MATCH (s:Skill {conceptUri: $uri}) SET s.embedding = $embedding",
                        uri=record['uri'], embedding=embedding
                    )
        
        # Process occupations (similar logic)
        logger.info("Generating embeddings for occupations")
        with self.driver.session() as session:
            query = "MATCH (o:Occupation) RETURN o.conceptUri as uri, o.preferredLabel as label, o.description as description, o.altLabels as altLabels"
            result = session.run(query)
            
            for record in tqdm(result, desc="Embedding occupations"):
                node_data = {
                    'preferredLabel': record['label'],
                    'description': record['description'],
                    'altLabels': record['altLabels']
                }
                
                embedding = embedding_util.generate_node_embedding(node_data)
                if embedding:
                    session.run(
                        "MATCH (o:Occupation {conceptUri: $uri}) SET o.embedding = $embedding",
                        uri=record['uri'], embedding=embedding
                    )
        
        logger.info("Completed embedding generation and storage")

    def run_ingest(self):
        """Run the complete ingestion process"""
        try:
            self.delete_all_data()  # Delete all data before starting
            self.create_constraints()
            self.ingest_skill_groups()
            self.ingest_skills()
            self.ingest_occupations()
            self.ingest_isco_groups()
            self.create_skill_hierarchy()
            self.create_isco_hierarchy()
            self.create_occupation_isco_mapping()
            self.create_occupation_skill_relations()
            self.create_skill_skill_relations()
            
            # Add vector indexes for semantic search
            self.create_vector_indexes()
            
            # Generate embeddings
            from embedding_utils import ESCOEmbedding
            embedding_util = ESCOEmbedding()
            self.generate_and_store_embeddings(embedding_util)
            
            logger.info("ESCO data ingestion completed successfully")
        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
            raise
        finally:
            self.close()

if __name__ == "__main__":
    # Configuration
    NEO4J_URI = "bolt://localhost:7687"  # Update with your Neo4j URI
    NEO4J_USER = "neo4j"                 # Update with your username
    NEO4J_PASSWORD = "Abcd1234@"          # Update with your password
    ESCO_DIR = "ESCO"                    # Directory containing ESCO CSV files

    # Create and run the ingestor
    ingestor = ESCOIngest(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, ESCO_DIR)
    ingestor.run_ingest() 