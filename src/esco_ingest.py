import os
import pandas as pd
from tqdm import tqdm
import logging
import argparse
import yaml
from datetime import datetime
from abc import ABC, abstractmethod
from weaviate_client import WeaviateClient
from embedding_utils import ESCOEmbedding
from logging_config import setup_logging
import numpy as np

# ESCO v1.2.0 (English) – CSV classification import for Weaviate
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
        self.batch_size = self.config['esco'].get('batch_size', 100)
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

class WeaviateIngestor(BaseIngestor):
    """Weaviate-specific implementation of ESCO data ingestion"""
    
    def __init__(self, config_path=None, profile='default'):
        super().__init__(config_path, profile)
        self.client = WeaviateClient(config_path, profile)
        self.embedding_util = ESCOEmbedding()

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
        logger.info(f"Starting occupation ingestion from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            total_occupations = len(df)
            logger.info(f"Found {total_occupations} occupations to process")
            
            # Generate embeddings and prepare data
            occupation_vectors = []
            occupations_to_import = []
            failed_embeddings = 0
            failed_imports = 0
            
            logger.info("Generating embeddings for occupations...")
            with tqdm(total=total_occupations, desc="Embedding Occupations", unit="occupation") as pbar:
                for idx, row in df.iterrows():
                    try:
                        node_data = {
                            'preferredLabel': row['preferredLabel'],
                            'description': row.get('description', ''),
                            'altLabels': row.get('altLabels', '')
                        }
                        embedding = self.embedding_util.generate_node_embedding(node_data)
                        
                        if embedding is not None:
                            # Convert embedding to list format
                            if isinstance(embedding, np.ndarray):
                                embedding = embedding.tolist()
                            elif isinstance(embedding, list):
                                # Ensure it's a flat list of floats
                                embedding = [float(x) for x in embedding]
                            else:
                                raise ValueError(f"Unexpected embedding type: {type(embedding)}")
                                
                            # Extract UUID from the full URI
                            concept_uri = row['conceptUri']
                            uuid = concept_uri.split('/')[-1]
                            
                            occupation_vectors.append(embedding)
                            occupations_to_import.append({
                                "conceptUri": uuid,  # Use just the UUID part
                                "preferredLabel": row['preferredLabel'],
                                "description": row.get('description', ''),
                                "iscoGroup": row.get('iscoGroup', ''),
                                "altLabels": row.get('altLabels', '')
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for occupation: {row.get('conceptUri', 'Unknown URI')} - {row.get('preferredLabel', 'Unknown Label')}")
                    except Exception as e:
                        failed_embeddings += 1
                        logger.error(f"Error processing occupation at index {idx}: {str(e)}")
                    
                    pbar.update(1)
            
            # Import occupations in batches
            if occupations_to_import and occupation_vectors:
                logger.info(f"Starting batch import of {len(occupations_to_import)} occupations")
                try:
                    self.client.batch_import_occupations(occupations_to_import, occupation_vectors)
                    logger.info(f"Successfully ingested {len(occupations_to_import)} occupations into Weaviate")
                except Exception as e:
                    failed_imports = len(occupations_to_import)
                    logger.error(f"Failed to import occupations batch: {str(e)}")
                    raise
            else:
                logger.warning("No occupations were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"Occupation ingestion completed:")
            logger.info(f"- Total occupations processed: {total_occupations}")
            logger.info(f"- Successfully embedded: {len(occupations_to_import)}")
            logger.info(f"- Failed embeddings: {failed_embeddings}")
            logger.info(f"- Failed imports: {failed_imports}")
            
        except Exception as e:
            logger.error(f"Error during occupation ingestion: {str(e)}")
            raise

    def ingest_skills(self):
        """Ingest skills into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'skills_en.csv')
        logger.info(f"Starting skills ingestion from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            total_skills = len(df)
            logger.info(f"Found {total_skills} skills to process")
            
            # Generate embeddings and prepare data
            skill_vectors = []
            skills_to_import = []
            failed_embeddings = 0
            failed_imports = 0
            
            logger.info("Generating embeddings for skills...")
            with tqdm(total=total_skills, desc="Embedding Skills", unit="skill") as pbar:
                for idx, row in df.iterrows():
                    try:
                        node_data = {
                            'preferredLabel': row['preferredLabel'],
                            'description': row.get('description', ''),
                            'altLabels': row.get('altLabels', '')
                        }
                        embedding = self.embedding_util.generate_node_embedding(node_data)
                        
                        if embedding is not None:
                            # Convert embedding to list format
                            if isinstance(embedding, np.ndarray):
                                embedding = embedding.tolist()
                            elif isinstance(embedding, list):
                                # Ensure it's a flat list of floats
                                embedding = [float(x) for x in embedding]
                            else:
                                raise ValueError(f"Unexpected embedding type: {type(embedding)}")
                                
                            # Extract UUID from the full URI
                            concept_uri = row['conceptUri']
                            uuid = concept_uri.split('/')[-1]
                            
                            skill_vectors.append(embedding)
                            skills_to_import.append({
                                "conceptUri": uuid,  # Use just the UUID part
                                "preferredLabel": row['preferredLabel'],
                                "description": row.get('description', ''),
                                "altLabels": row.get('altLabels', '')
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for skill: {row.get('conceptUri', 'Unknown URI')} - {row.get('preferredLabel', 'Unknown Label')}")
                    except Exception as e:
                        failed_embeddings += 1
                        logger.error(f"Error processing skill at index {idx}: {str(e)}")
                    
                    pbar.update(1)
            
            # Import skills in batches
            if skills_to_import and skill_vectors:
                logger.info(f"Starting batch import of {len(skills_to_import)} skills")
                try:
                    self.client.batch_import_skills(skills_to_import, skill_vectors)
                    logger.info(f"Successfully ingested {len(skills_to_import)} skills into Weaviate")
                except Exception as e:
                    failed_imports = len(skills_to_import)
                    logger.error(f"Failed to import skills batch: {str(e)}")
                    raise
            else:
                logger.warning("No skills were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"Skills ingestion completed:")
            logger.info(f"- Total skills processed: {total_skills}")
            logger.info(f"- Successfully embedded: {len(skills_to_import)}")
            logger.info(f"- Failed embeddings: {failed_embeddings}")
            logger.info(f"- Failed imports: {failed_imports}")
            
        except Exception as e:
            logger.error(f"Error during skills ingestion: {str(e)}")
            raise

    def create_skill_relations(self):
        """Create skill relations in Weaviate"""
        file_path = os.path.join(self.esco_dir, 'occupationSkillRelations_en.csv')
        df = pd.read_csv(file_path)
        
        # Group relations by occupation
        total_occupations = len(df['occupationUri'].unique())
        logger.info(f"Creating skill relations for {total_occupations} occupations...")
        
        with tqdm(total=total_occupations, desc="Creating Skill Relations", unit="occupation") as pbar:
            for occupation_uri, group in df.groupby('occupationUri'):
                try:
                    # Extract UUID from the full URI
                    occupation_uuid = occupation_uri.split('/')[-1]
                    essential_skills = [uri.split('/')[-1] for uri in group[group['relationType'] == 'essential']['skillUri'].tolist()]
                    optional_skills = [uri.split('/')[-1] for uri in group[group['relationType'] == 'optional']['skillUri'].tolist()]
                    
                    # Add relations
                    self.client.add_skill_relations(
                        occupation_uri=occupation_uuid,
                        essential_skills=essential_skills,
                        optional_skills=optional_skills
                    )
                except Exception as e:
                    logger.error(f"Failed to add relations for occupation {occupation_uri}: {str(e)}")
                    continue
                pbar.update(1)
        
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

def create_ingestor(config_path=None, profile='default'):
    """
    Factory function to create the Weaviate ingestor
    
    Args:
        config_path (str): Path to configuration file
        profile (str): Configuration profile to use
        
    Returns:
        WeaviateIngestor: Weaviate ingestor instance
    """
    return WeaviateIngestor(config_path, profile)

def main():
    parser = argparse.ArgumentParser(description='ESCO Data Ingestion Tool for Weaviate')
    
    # Configuration parameters
    parser.add_argument('--config', type=str,
                      help='Path to YAML config file')
    parser.add_argument('--profile', type=str, default='default',
                      help='Configuration profile to use')
    
    # Execution mode
    parser.add_argument('--embeddings-only', action='store_true',
                      help='Run only the embedding generation and indexing')
    
    args = parser.parse_args()
    
    # Create ingestor instance
    ingestor = create_ingestor(args.config, args.profile)
    
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