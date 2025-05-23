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
            # Delete all collections
            for collection in ["Occupation", "Skill", "ISCOGroup", "SkillCollection"]:
                if self.client.client.schema.exists(collection):
                    self.client.client.schema.delete_class(collection)
            
            logger.info("Deleted all data from Weaviate")
        except Exception as e:
            logger.error(f"Error deleting Weaviate data: {str(e)}")
            raise

    def ingest_isco_groups(self):
        """Ingest ISCO groups into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'ISCOGroups_en.csv')
        logger.info(f"Starting ISCO groups ingestion from {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            total_groups = len(df)
            logger.info(f"Found {total_groups} ISCO groups to process")
            
            # Generate embeddings and prepare data
            group_vectors = []
            groups_to_import = []
            failed_embeddings = 0
            failed_imports = 0
            
            logger.info("Generating embeddings for ISCO groups...")
            with tqdm(total=total_groups, desc="Embedding ISCO Groups", unit="group") as pbar:
                for idx, row in df.iterrows():
                    try:
                        node_data = {
                            'preferredLabel': row['preferredLabel'],
                            'description': row.get('description', '')
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
                            
                            group_vectors.append(embedding)
                            groups_to_import.append({
                                "conceptUri": uuid,  # Use just the UUID part
                                "code": row['code'],
                                "preferredLabel_en": row['preferredLabel'],
                                "description_en": row.get('description', '')
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for ISCO group: {row.get('conceptUri', 'Unknown URI')} - {row.get('preferredLabel', 'Unknown Label')}")
                    except Exception as e:
                        failed_embeddings += 1
                        logger.error(f"Error processing ISCO group at index {idx}: {str(e)}")
                    
                    pbar.update(1)
            
            # Import ISCO groups in batches
            if groups_to_import and group_vectors:
                logger.info(f"Starting batch import of {len(groups_to_import)} ISCO groups")
                try:
                    self.client.batch_import_isco_groups(groups_to_import, group_vectors)
                    logger.info(f"Successfully ingested {len(groups_to_import)} ISCO groups into Weaviate")
                except Exception as e:
                    failed_imports = len(groups_to_import)
                    logger.error(f"Failed to import ISCO groups batch: {str(e)}")
                    raise
            else:
                logger.warning("No ISCO groups were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"ISCO groups ingestion completed:")
            logger.info(f"- Total groups processed: {total_groups}")
            logger.info(f"- Successfully embedded: {len(groups_to_import)}")
            logger.info(f"- Failed embeddings: {failed_embeddings}")
            logger.info(f"- Failed imports: {failed_imports}")
            
        except Exception as e:
            logger.error(f"Error during ISCO groups ingestion: {str(e)}")
            raise

    def ingest_occupations(self):
        """Ingest occupations into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'occupations_en.csv')
        logger.info(f"Starting occupation ingestion from {file_path}")
        
        try:
            # Read CSV with explicit string dtypes for critical fields
            df = pd.read_csv(file_path, dtype={
                'conceptUri': str,
                'altLabels': str,
                'preferredLabel': str,
                'description': str,
                'definition': str,
                'code': str,
                'iscoGroup': str
            })
            
            # Convert NaN to empty string for string columns
            string_columns = ['conceptUri', 'altLabels', 'preferredLabel', 'description', 'definition', 'code', 'iscoGroup']
            df[string_columns] = df[string_columns].fillna('')
            
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
                        # Skip rows with missing conceptUri
                        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
                            logger.warning(f"Skipping row {idx} - missing conceptUri")
                            failed_embeddings += 1
                            continue
                            
                        node_data = {
                            'preferredLabel': row['preferredLabel'],
                            'description': row['description'],
                            'altLabels': row['altLabels']
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
                            
                            # Handle altLabels - split only if not empty
                            alt_labels = row['altLabels'].split('|') if row['altLabels'] else []
                            
                            occupation_vectors.append(embedding)
                            occupations_to_import.append({
                                "conceptUri": uuid,  # Use just the UUID part
                                "code": row['code'],
                                "preferredLabel_en": row['preferredLabel'],
                                "description_en": row['description'],
                                "definition_en": row['definition'],
                                "iscoGroup": row['iscoGroup'],
                                "altLabels_en": alt_labels
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for occupation: {row['conceptUri']} - {row['preferredLabel']}")
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
            # Define required columns
            required_columns = {"conceptUri", "preferredLabel", "altLabels"}
            
            # Read CSV with explicit string dtypes for critical fields
            df = pd.read_csv(file_path, dtype={
                'conceptUri': str,
                'altLabels': str,
                'preferredLabel': str,
                'description': str,
                'definition': str,
                'skillType': str,
                'reuseLevel': str
            })
            
            # Log detected columns for debugging
            logger.debug(f"Skills CSV columns detected: {list(df.columns)}")
            
            # Validate required columns
            missing = required_columns - set(df.columns)
            if missing:
                raise ValueError(f"Skills file is missing required columns: {missing}")
            
            # Define all possible string columns and get intersection with actual columns
            string_columns = ['conceptUri', 'altLabels', 'preferredLabel', 'description', 
                            'definition', 'skillType', 'reuseLevel']
            present_cols = list(set(string_columns).intersection(df.columns))
            df[present_cols] = df[present_cols].fillna('')
            
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
                    # Skip rows with missing conceptUri
                    if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
                        logger.warning(f"Skipping row {idx} – missing conceptUri")
                        failed_embeddings += 1
                        pbar.update(1)
                        continue
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
                                "preferredLabel_en": row['preferredLabel'],
                                "description_en": row.get('description', ''),
                                "definition_en": row.get('definition', ''),
                                "skillType": row.get('skillType', ''),
                                "reuseLevel": row.get('reuseLevel', ''),
                                "altLabels_en": row.get('altLabels', '').split('|') if row.get('altLabels') else []
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

    def create_hierarchical_relations(self):
        """Create hierarchical relations in Weaviate"""
        # Create occupation hierarchy
        occupation_hierarchy_path = os.path.join(self.esco_dir, 'broaderRelationsOccPillar_en.csv')
        if os.path.exists(occupation_hierarchy_path):
            df = pd.read_csv(occupation_hierarchy_path)
            total_relations = len(df)
            logger.info(f"Creating occupation hierarchy relations for {total_relations} relations...")
            
            with tqdm(total=total_relations, desc="Creating Occupation Hierarchy", unit="relation") as pbar:
                for _, row in df.iterrows():
                    try:
                        # Extract UUIDs from the full URIs
                        broader_uuid = row['broaderUri'].split('/')[-1]
                        narrower_uuid = row['narrowerUri'].split('/')[-1]
                        
                        # Add relations
                        self.client.add_hierarchical_relation(
                            broader_uri=broader_uuid,
                            narrower_uri=narrower_uuid,
                            relation_type="Occupation"
                        )
                    except Exception as e:
                        logger.error(f"Failed to add occupation hierarchy relation: {str(e)}")
                        continue
                    pbar.update(1)
        
        # Create skill hierarchy
        skill_hierarchy_path = os.path.join(self.esco_dir, 'skillsHierarchy_en.csv')
        if os.path.exists(skill_hierarchy_path):
            # Read CSV with explicit string dtypes for critical fields
            df = pd.read_csv(skill_hierarchy_path, dtype={
                'broaderUri': str,
                'narrowerUri': str
            })
            
            # Convert NaN to empty string for string columns
            string_columns = ['broaderUri', 'narrowerUri']
            df[string_columns] = df[string_columns].fillna('')
            
            # Filter out rows with missing URIs
            df = df[df['broaderUri'].notna() & df['narrowerUri'].notna()]
            df = df[(df['broaderUri'] != '') & (df['narrowerUri'] != '')]
            
            total_relations = len(df)
            logger.info(f"Creating skill hierarchy relations for {total_relations} relations...")
            
            with tqdm(total=total_relations, desc="Creating Skill Hierarchy", unit="relation") as pbar:
                for _, row in df.iterrows():
                    try:
                        # Skip rows with missing URIs
                        if not row['broaderUri'] or not row['narrowerUri']:
                            continue
                            
                        # Extract UUIDs from the full URIs
                        broader_uuid = row['broaderUri'].split('/')[-1]
                        narrower_uuid = row['narrowerUri'].split('/')[-1]
                        
                        # Add relations
                        self.client.add_hierarchical_relation(
                            broader_uri=broader_uuid,
                            narrower_uri=narrower_uuid,
                            relation_type="Skill"
                        )
                    except Exception as e:
                        logger.error(f"Failed to add skill hierarchy relation: {str(e)}")
                        continue
                    pbar.update(1)

    def ingest_skill_collections(self):
        """Ingest skill collections into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'conceptSchemes_en.csv')
        logger.info(f"Starting skill collections ingestion from {file_path}")
        
        try:
            # Define required columns
            required_columns = {"conceptUri", "preferredLabel"}
            
            # Read CSV with explicit string dtypes for critical fields
            df = pd.read_csv(file_path, dtype={
                'conceptUri': str,
                'preferredLabel': str,
                'description': str
            })
            
            # Log detected columns for debugging
            logger.debug(f"Skill collections CSV columns detected: {list(df.columns)}")
            
            # Validate required columns
            missing = required_columns - set(df.columns)
            if missing:
                raise ValueError(f"Skill collections file is missing required columns: {missing}")
            
            # Convert NaN to empty string for string columns
            string_columns = ['conceptUri', 'preferredLabel', 'description']
            df[string_columns] = df[string_columns].fillna('')
            
            total_collections = len(df)
            logger.info(f"Found {total_collections} skill collections to process")
            
            # Generate embeddings and prepare data
            collection_vectors = []
            collections_to_import = []
            failed_embeddings = 0
            failed_imports = 0
            
            logger.info("Generating embeddings for skill collections...")
            with tqdm(total=total_collections, desc="Embedding Skill Collections", unit="collection") as pbar:
                for idx, row in df.iterrows():
                    try:
                        # Skip rows with missing conceptUri
                        if not row['conceptUri'] or row['conceptUri'].lower() == 'nan':
                            logger.warning(f"Skipping row {idx} - missing conceptUri")
                            failed_embeddings += 1
                            continue
                            
                        node_data = {
                            'preferredLabel': row['preferredLabel'],
                            'description': row.get('description', '')
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
                            
                            collection_vectors.append(embedding)
                            collections_to_import.append({
                                "conceptUri": uuid,  # Use just the UUID part
                                "preferredLabel_en": row['preferredLabel'],
                                "description_en": row.get('description', '')
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for skill collection: {row.get('conceptUri', 'Unknown URI')} - {row.get('preferredLabel', 'Unknown Label')}")
                    except Exception as e:
                        failed_embeddings += 1
                        logger.error(f"Error processing skill collection at index {idx}: {str(e)}")
                    
                    pbar.update(1)
            
            # Import skill collections in batches
            if collections_to_import and collection_vectors:
                logger.info(f"Starting batch import of {len(collections_to_import)} skill collections")
                try:
                    self.client.batch_import_skill_collections(collections_to_import, collection_vectors)
                    logger.info(f"Successfully ingested {len(collections_to_import)} skill collections into Weaviate")
                except Exception as e:
                    failed_imports = len(collections_to_import)
                    logger.error(f"Failed to import skill collections batch: {str(e)}")
                    raise
            else:
                logger.warning("No skill collections were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"Skill collections ingestion completed:")
            logger.info(f"- Total collections processed: {total_collections}")
            logger.info(f"- Successfully embedded: {len(collections_to_import)}")
            logger.info(f"- Failed embeddings: {failed_embeddings}")
            logger.info(f"- Failed imports: {failed_imports}")
            
        except Exception as e:
            logger.error(f"Error during skill collections ingestion: {str(e)}")
            raise

    def create_skill_collection_relations(self):
        """Create skill collection relations in Weaviate"""
        # Process each collection file
        collection_files = [
            'digCompSkillsCollection_en.csv',
            'digitalSkillsCollection_en.csv',
            'greenSkillsCollection_en.csv',
            'languageSkillsCollection_en.csv',
            'researchSkillsCollection_en.csv',
            'transversalSkillsCollection_en.csv'
        ]
        
        for collection_file in collection_files:
            file_path = os.path.join(self.esco_dir, collection_file)
            if not os.path.exists(file_path):
                logger.warning(f"Collection file not found: {file_path}")
                continue
                
            logger.info(f"Processing skill collection relations from {file_path}")
            df = pd.read_csv(file_path)
            total_relations = len(df)
            
            with tqdm(total=total_relations, desc=f"Creating {os.path.basename(collection_file)} Relations", unit="relation") as pbar:
                for _, row in df.iterrows():
                    try:
                        # Extract UUIDs from the full URIs
                        collection_uuid = row['conceptSchemeUri'].split('/')[-1]
                        skill_uuid = row['skillUri'].split('/')[-1]
                        
                        # Add relation
                        self.client.add_skill_collection_relation(
                            collection_uri=collection_uuid,
                            skill_uri=skill_uuid
                        )
                    except Exception as e:
                        logger.error(f"Failed to add skill collection relation: {str(e)}")
                        continue
                    pbar.update(1)

    def run_ingest(self):
        """Run the complete Weaviate ingestion process"""
        try:
            # Create a progress bar for the overall process
            with tqdm(total=6, desc="ESCO Ingestion Progress", unit="step",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                # Step 1: Delete existing data
                self.delete_all_data()
                pbar.update(1)
                
                # Step 2: Ingest ISCO groups
                self.ingest_isco_groups()
                pbar.update(1)
                
                # Step 3: Ingest occupations
                self.ingest_occupations()
                pbar.update(1)
                
                # Step 4: Ingest skills
                self.ingest_skills()
                pbar.update(1)
                
                # Step 5: Ingest skill collections
                self.ingest_skill_collections()
                pbar.update(1)
                
                # Step 6: Create skill relations
                self.create_skill_relations()
                pbar.update(1)
                
                # Step 7: Create hierarchical relations
                self.create_hierarchical_relations()
                pbar.update(1)
                
                # Step 8: Create skill collection relations
                self.create_skill_collection_relations()
                pbar.update(1)
                
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