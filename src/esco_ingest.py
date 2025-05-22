import os
import pandas as pd
from tqdm import tqdm
import logging
import argparse
import json
import yaml
from datetime import datetime
from abc import ABC, abstractmethod
from neo4j import GraphDatabase
from neo4j_client import Neo4jClient
from weaviate_client import WeaviateClient
from embedding_utils import ESCOEmbedding, generate_embeddings
from logging_config import setup_logging

# ESCO v1.2.0 (English) – CSV classification import for Neo4j 5.x
# Oz Levi
# 2025-05-11

# Setup logging
logger = setup_logging()

class BaseIngestor(ABC):
    """Base class for ESCO data ingestion"""
    
    def __init__(self, config_path=None, profile='default'):
        """
        Initialize base ingestor
        
        Args:
            config_path (str): Path to YAML config file
            profile (str): Configuration profile to use
        """
        self.config = self._load_config(config_path, profile)
        self.esco_dir = self.config['esco']['data_dir']
        self.batch_size = self._get_batch_size(profile)
        logger.info(f"Using batch size of {self.batch_size} for {profile} profile")

    def _load_config(self, config_path, profile):
        """Load configuration from YAML file"""
        if not config_path:
            config_path = self._get_default_config_path()
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config[profile]

    @abstractmethod
    def _get_default_config_path(self):
        """Get default configuration file path"""
        pass

    def _get_batch_size(self, profile):
        """Get batch size based on profile"""
        if profile == 'aura':
            return 1000  # Smaller batch size for AuraDB
        return self.config['esco'].get('batch_size', 50000)

    @abstractmethod
    def close(self):
        """Close database connection"""
        pass

    @abstractmethod
    def delete_all_data(self):
        """Delete all data from the database"""
        pass

    def process_csv_in_batches(self, file_path, process_func):
        """Process a CSV file in batches"""
        df = pd.read_csv(file_path)
        total_rows = len(df)
        
        with tqdm(total=total_rows, desc=f"Processing {os.path.basename(file_path)}", unit="rows",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            for start_idx in range(0, total_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]
                process_func(batch)
                pbar.update(len(batch))

    @abstractmethod
    def run_ingest(self):
        """Run the complete ingestion process"""
        pass

    @abstractmethod
    def run_embeddings_only(self):
        """Run only the embedding generation and indexing"""
        pass

class Neo4jIngestor(BaseIngestor):
    """Neo4j-specific implementation of ESCO data ingestion"""
    
    def __init__(self, config_path=None, profile='default'):
        super().__init__(config_path, profile)
        self.client = Neo4jClient(config_path, profile)

    def _get_default_config_path(self):
        return 'config/neo4j_config.yaml'

    def close(self):
        """Close the Neo4j connection"""
        self.client.close()

    def delete_all_data(self):
        """Delete all nodes and relationships from Neo4j"""
        with self.client.driver.session() as session:
            # First drop all constraints
            constraints = [
                "DROP CONSTRAINT skill_uri IF EXISTS",
                "DROP CONSTRAINT skillgroup_uri IF EXISTS",
                "DROP CONSTRAINT occupation_uri IF EXISTS",
                "DROP CONSTRAINT iscogroup_uri IF EXISTS",
                "DROP CONSTRAINT iscogroup_code IF EXISTS"
            ]
            for constraint in constraints:
                self.client.execute_query(constraint, session=session)
            
            # Then delete all nodes and relationships
            query = "MATCH (n) DETACH DELETE n"
            self.client.execute_query(query, session=session)
            logger.info("Deleted all data from Neo4j")

    def create_constraints(self):
        """Create Neo4j constraints"""
        with self.client.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT skill_uri IF NOT EXISTS FOR (s:Skill) REQUIRE s.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT skillgroup_uri IF NOT EXISTS FOR (sg:SkillGroup) REQUIRE sg.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT occupation_uri IF NOT EXISTS FOR (o:Occupation) REQUIRE o.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT iscogroup_uri IF NOT EXISTS FOR (g:ISCOGroup) REQUIRE g.conceptUri IS UNIQUE",
                "CREATE CONSTRAINT iscogroup_code IF NOT EXISTS FOR (g:ISCOGroup) REQUIRE g.code IS UNIQUE"
            ]
            for constraint in constraints:
                self.client.execute_query(constraint, session=session)
            logger.info("Created Neo4j constraints")

    def ingest_skill_groups(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MERGE (sg:Skill:SkillGroup {conceptUri: $conceptUri})
                    SET sg += $properties
                    """
                    self.client.execute_query(query, 
                        parameters={'conceptUri': row['conceptUri'], 'properties': row.to_dict()},
                        session=session)

        file_path = os.path.join(self.esco_dir, 'skillGroups_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested skill groups")

    def ingest_skills(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MERGE (s:Skill {conceptUri: $conceptUri})
                    SET s += $properties
                    """
                    self.client.execute_query(query, 
                        parameters={'conceptUri': row['conceptUri'], 'properties': row.to_dict()},
                        session=session)

        file_path = os.path.join(self.esco_dir, 'skills_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested skills")

    def ingest_occupations(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                for _, row in batch.iterrows():
                    query = """
                    MERGE (o:Occupation {conceptUri: $conceptUri})
                    SET o += $properties
                    """
                    self.client.execute_query(query, 
                        parameters={'conceptUri': row['conceptUri'], 'properties': row.to_dict()},
                        session=session)

        file_path = os.path.join(self.esco_dir, 'occupations_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested occupations")

    def ingest_isco_groups(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
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
                    self.client.execute_query(query, 
                        parameters={'conceptUri': row['conceptUri'], 'properties': properties},
                        session=session)
                    
                    # Then update the code if it exists
                    if code is not None:
                        update_query = """
                        MATCH (g:ISCOGroup {conceptUri: $conceptUri})
                        SET g.code = $code
                        """
                        self.client.execute_query(update_query, 
                            parameters={'conceptUri': row['conceptUri'], 'code': code},
                            session=session)

        file_path = os.path.join(self.esco_dir, 'ISCOGroups_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Ingested ISCO groups")

    def create_skill_hierarchy(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                # Convert batch to list of dictionaries
                data = batch[['conceptUri', 'broaderUri']].to_dict('records')
                
                query = """
                UNWIND $data as row
                MATCH (child:Skill {conceptUri: row.conceptUri})
                MATCH (parent:Skill {conceptUri: row.broaderUri})
                MERGE (parent)-[:BROADER_THAN]->(child)
                """
                self.client.execute_query(query, data=data, session=session)

        file_path = os.path.join(self.esco_dir, 'broaderRelationsSkillPillar_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created skill hierarchy")

    def create_isco_hierarchy(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                # Convert batch to list of dictionaries
                data = batch[['conceptUri', 'broaderUri']].to_dict('records')
                
                query = """
                UNWIND $data as row
                MATCH (child:ISCOGroup {conceptUri: row.conceptUri})
                MATCH (parent:ISCOGroup {conceptUri: row.broaderUri})
                MERGE (parent)-[:BROADER_THAN]->(child)
                """
                self.client.execute_query(query, data=data, session=session)

        file_path = os.path.join(self.esco_dir, 'broaderRelationsOccPillar_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created ISCO hierarchy")

    def create_occupation_isco_mapping(self):
        with self.client.driver.session() as session:
            query = """
            MATCH (o:Occupation)
            WITH o, o.iscoGroup as iscoCode
            MATCH (g:ISCOGroup {code: iscoCode})
            MERGE (o)-[:PART_OF_ISCOGROUP]->(g)
            """
            self.client.execute_query(query, session=session)
            logger.info("Created occupation-ISCO mapping")

    def create_occupation_skill_relations(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                # Prepare batch data for essential relations
                essential_data = batch[batch['relationType'] == 'essential'][['skillUri', 'occupationUri']].to_dict('records')
                # Prepare batch data for optional relations
                optional_data = batch[batch['relationType'] == 'optional'][['skillUri', 'occupationUri']].to_dict('records')
                
                # Process essential relations in batch
                if essential_data:
                    query = """
                    UNWIND $data as row
                    MATCH (s:Skill {conceptUri: row.skillUri})
                    MATCH (o:Occupation {conceptUri: row.occupationUri})
                    MERGE (s)-[:ESSENTIAL_FOR]->(o)
                    """
                    self.client.execute_query(query, data=essential_data, session=session)
                
                # Process optional relations in batch
                if optional_data:
                    query = """
                    UNWIND $data as row
                    MATCH (s:Skill {conceptUri: row.skillUri})
                    MATCH (o:Occupation {conceptUri: row.occupationUri})
                    MERGE (s)-[:OPTIONAL_FOR]->(o)
                    """
                    self.client.execute_query(query, data=optional_data, session=session)

        file_path = os.path.join(self.esco_dir, 'occupationSkillRelations_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created occupation-skill relations")

    def create_skill_skill_relations(self):
        def process_batch(batch):
            with self.client.driver.session() as session:
                # Convert batch to list of dictionaries
                data = batch[['originalSkillUri', 'relatedSkillUri', 'relationType']].to_dict('records')
                
                query = """
                UNWIND $data as row
                MATCH (a:Skill {conceptUri: row.originalSkillUri})
                MATCH (b:Skill {conceptUri: row.relatedSkillUri})
                MERGE (a)-[:RELATED_SKILL {type: row.relationType}]->(b)
                """
                self.client.execute_query(query, data=data, session=session)

        file_path = os.path.join(self.esco_dir, 'skillSkillRelations_en.csv')
        self.process_csv_in_batches(file_path, process_batch)
        logger.info("Created skill-skill relations")

    def create_vector_indexes(self):
        """Create vector indexes for Neo4j vector search if supported"""
        try:
            with self.client.driver.session() as session:
                # Check Neo4j version
                version_result = session.run("CALL dbms.components() YIELD versions").single()
                version = version_result['versions'][0]
                major, minor, patch = map(int, version.split('.')[:3])
                
                if major > 5 or (major == 5 and minor >= 15):
                    # New vector index syntax for Neo4j 5.15+
                    session.run("""
                        CREATE VECTOR INDEX skill_embedding IF NOT EXISTS
                        FOR (s:Skill)
                        ON (s.embedding)
                        OPTIONS {
                            indexConfig: {
                                `vector.dimensions`: 384,
                                `vector.similarity_function`: 'cosine'
                            }
                        }
                    """)
                    
                    session.run("""
                        CREATE VECTOR INDEX occupation_embedding IF NOT EXISTS
                        FOR (o:Occupation)
                        ON (o.embedding)
                        OPTIONS {
                            indexConfig: {
                                `vector.dimensions`: 384,
                                `vector.similarity_function`: 'cosine'
                            }
                        }
                    """)
                elif major == 5 and minor >= 11:
                    # Old vector index syntax for Neo4j 5.11-5.14
                    session.run("""
                        CALL db.index.vector.createNodeIndex(
                            'skill_embedding',
                            'Skill',
                            'embedding',
                            384,
                            'cosine'
                        )
                    """)
                    
                    session.run("""
                        CALL db.index.vector.createNodeIndex(
                            'occupation_embedding',
                            'Occupation',
                            'embedding',
                            384,
                            'cosine'
                        )
                    """)
                else:
                    logger.warning(f"Vector indexes are not supported in Neo4j version {version}. Skipping vector index creation.")
                
                logger.info("Created vector indexes for semantic search")
        except Exception as e:
            logger.warning(f"Could not create vector indexes: {str(e)}. Continuing without vector indexes.")

    def generate_and_store_embeddings(self, embedding_util):
        """Generate embeddings for all skills and occupations"""
        # Get total count of nodes to process
        with self.client.driver.session() as session:
            # Get total count of skills and occupations
            count_query = """
                MATCH (n)
                WHERE n:Skill OR n:Occupation
                RETURN count(n) as count
            """
            total_nodes = session.run(count_query).single()["count"]
            
            # Create a single progress bar for all nodes
            with tqdm(total=total_nodes, desc="Generating embeddings", unit="nodes",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                
                # Process skills
                logger.info("Generating embeddings for skills")
                query = "MATCH (s:Skill) RETURN s.conceptUri as uri, s.preferredLabel as label, s.description as description, s.altLabels as altLabels"
                result = session.run(query)
                
                for record in result:
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
                    pbar.update(1)
                
                # Process occupations
                logger.info("Generating embeddings for occupations")
                query = "MATCH (o:Occupation) RETURN o.conceptUri as uri, o.preferredLabel as label, o.description as description, o.altLabels as altLabels"
                result = session.run(query)
                
                for record in result:
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
                    pbar.update(1)
        
        logger.info("Completed embedding generation and storage")

    def run_ingest(self):
        """Run the complete Neo4j ingestion process"""
        try:
            self.delete_all_data()
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
            
            # Add vector indexes for semantic search (if supported)
            self.create_vector_indexes()
            
            # Generate embeddings
            embedding_util = ESCOEmbedding()
            self.generate_and_store_embeddings(embedding_util)
            
            logger.info("ESCO data ingestion into Neo4j completed successfully")
        except Exception as e:
            logger.error(f"Error during Neo4j ingestion: {str(e)}")
            raise
        finally:
            self.close()

    def run_embeddings_only(self):
        """Run only the Neo4j embedding generation and indexing"""
        try:
            self.create_vector_indexes()
            embedding_util = ESCOEmbedding()
            self.generate_and_store_embeddings(embedding_util)
            logger.info("Neo4j embedding generation and indexing completed successfully")
        except Exception as e:
            logger.error(f"Error during Neo4j embedding generation: {str(e)}")
            raise
        finally:
            self.close()

class WeaviateIngestor(BaseIngestor):
    """Weaviate-specific implementation of ESCO data ingestion"""
    
    def __init__(self, config_path=None, profile='default'):
        super().__init__(config_path, profile)
        self.client = WeaviateClient(config_path, profile)

    def _get_default_config_path(self):
        return 'config/weaviate_config.yaml'

    def close(self):
        """Close the Weaviate connection"""
        # Weaviate client doesn't need explicit closing
        pass

    def delete_all_data(self):
        """Delete all data from Weaviate"""
        try:
            # Delete Occupation collection
            if self.client.client.schema.exists("Occupation"):
                self.client.client.schema.delete_class("Occupation")
            
            # Delete Skill collection
            if self.client.client.schema.exists("Skill"):
                self.client.client.schema.delete_class("Skill")
            
            logger.info("Deleted all data from Weaviate")
        except Exception as e:
            logger.error(f"Error deleting Weaviate data: {str(e)}")
            raise

    def ingest_occupations(self):
        """Ingest occupations into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'occupations_en.csv')
        df = pd.read_csv(file_path)
        
        # Generate embeddings
        occupation_vectors = generate_embeddings(
            texts=df['prefLabel'].tolist(),
            descriptions=df['description'].tolist()
        )
        
        # Prepare occupation data
        occupations = []
        for _, row in df.iterrows():
            occupations.append({
                "conceptUri": row['conceptUri'],
                "preferredLabel": row['prefLabel'],
                "description": row.get('description', '')
            })
        
        # Import occupations
        self.client.batch_import_occupations(occupations, occupation_vectors)
        logger.info("Ingested occupations into Weaviate")

    def ingest_skills(self):
        """Ingest skills into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'skills_en.csv')
        df = pd.read_csv(file_path)
        
        # Generate embeddings
        skill_vectors = generate_embeddings(
            texts=df['prefLabel'].tolist(),
            descriptions=df['description'].tolist()
        )
        
        # Prepare skill data
        skills = []
        for _, row in df.iterrows():
            skills.append({
                "conceptUri": row['conceptUri'],
                "preferredLabel": row['prefLabel'],
                "description": row.get('description', '')
            })
        
        # Import skills
        self.client.batch_import_skills(skills, skill_vectors)
        logger.info("Ingested skills into Weaviate")

    def create_skill_relations(self):
        """Create skill relations in Weaviate"""
        file_path = os.path.join(self.esco_dir, 'occupationSkillRelations_en.csv')
        df = pd.read_csv(file_path)
        
        # Group relations by occupation
        for occupation_uri, group in df.groupby('occupationUri'):
            essential_skills = group[group['relationType'] == 'essential']['skillUri'].tolist()
            optional_skills = group[group['relationType'] == 'optional']['skillUri'].tolist()
            
            # Add relations
            self.client.add_skill_relations(
                occupation_uri=occupation_uri,
                essential_skills=essential_skills,
                optional_skills=optional_skills
            )
        
        logger.info("Created skill relations in Weaviate")

    def run_ingest(self):
        """Run the complete Weaviate ingestion process"""
        try:
            self.delete_all_data()
            self.ingest_occupations()
            self.ingest_skills()
            self.create_skill_relations()
            logger.info("ESCO data ingestion into Weaviate completed successfully")
        except Exception as e:
            logger.error(f"Error during Weaviate ingestion: {str(e)}")
            raise

    def run_embeddings_only(self):
        """Run only the Weaviate embedding generation and indexing"""
        try:
            # Weaviate handles embeddings during ingestion
            logger.info("Weaviate embeddings are generated during ingestion")
        except Exception as e:
            logger.error(f"Error during Weaviate embedding generation: {str(e)}")
            raise

def create_ingestor(db_type, config_path=None, profile='default'):
    """
    Factory function to create the appropriate ingestor
    
    Args:
        db_type (str): Type of database ('neo4j' or 'weaviate')
        config_path (str): Path to configuration file
        profile (str): Configuration profile to use
        
    Returns:
        BaseIngestor: Appropriate ingestor instance
    """
    if db_type.lower() == 'neo4j':
        return Neo4jIngestor(config_path, profile)
    elif db_type.lower() == 'weaviate':
        return WeaviateIngestor(config_path, profile)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def main():
    parser = argparse.ArgumentParser(description='ESCO Data Ingestion Tool')
    
    # Configuration parameters
    parser.add_argument('--db-type', type=str, required=True,
                      choices=['neo4j', 'weaviate'],
                      help='Type of database to ingest into')
    parser.add_argument('--config', type=str,
                      help='Path to YAML config file')
    parser.add_argument('--profile', type=str, default='default',
                      choices=['default', 'aura'],
                      help='Configuration profile to use')
    
    # Execution mode
    parser.add_argument('--embeddings-only', action='store_true',
                      help='Run only the embedding generation and indexing')
    
    args = parser.parse_args()
    
    # Create ingestor instance
    ingestor = create_ingestor(args.db_type, args.config, args.profile)
    
    try:
        # Run appropriate process
        if args.embeddings_only:
            ingestor.run_embeddings_only()
        else:
            ingestor.run_ingest()
    finally:
        ingestor.close()

if __name__ == "__main__":
    main() 