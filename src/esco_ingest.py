import os
import pandas as pd
from tqdm import tqdm
import argparse
import yaml
from abc import ABC, abstractmethod
import numpy as np
import click

# Local imports
from src.esco_weaviate_client import WeaviateClient
from src.embedding_utils import ESCOEmbedding
from src.logging_config import setup_logging

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
        self.esco_dir = self.config['app']['data_dir']
        self.batch_size = self.config['weaviate'].get('batch_size', 100)
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
    
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize the Weaviate ingestor."""
        super().__init__(config_path, profile)
        self.client = WeaviateClient(config_path, profile)
        
        # Initialize repositories
        self.skill_repo = self.client.get_repository("Skill")
        self.occupation_repo = self.client.get_repository("Occupation")
        self.isco_group_repo = self.client.get_repository("ISCOGroup")
        self.skill_collection_repo = self.client.get_repository("SkillCollection")
        self.skill_group_repo = self.client.get_repository("SkillGroup")
        self.embedding_util = ESCOEmbedding()

    def _get_default_config_path(self):
        return 'config/weaviate_config.yaml'

    def initialize_schema(self):
        """Initialize the Weaviate schema if not already initialized."""
        try:
            if not self.client.is_schema_initialized():
                logger.info("Initializing Weaviate schema...")
                self.client.ensure_schema()
                logger.info("Schema initialization completed")
            else:
                logger.info("Schema already initialized")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {str(e)}")
            raise

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _standardize_hierarchy_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename alternative hierarchy column names to the expected
        `broaderUri` / `narrowerUri`.

        Handles variants found in ESCO CSVs such as:
        - broaderConceptUri / narrowerConceptUri
        - parentUri / childUri
        - broaderSkillUri / skillUri   (skill hierarchy)
        - conceptUri / targetUri       (child/narrower)
        - Level X URI format           (skills hierarchy)
        """
        rename_map = {}
        
        # Handle Level X URI format
        if 'Level 0 URI' in df.columns:
            # For each row, find the highest non-empty Level URI
            def get_broader_narrower(row):
                levels = [f'Level {i} URI' for i in range(4)]  # ESCO uses up to Level 3
                non_empty_levels = [level for level in levels if level in df.columns and pd.notna(row[level]) and row[level] != '']
                if len(non_empty_levels) >= 2:
                    # The broader URI is the second-to-last non-empty level
                    broader = row[non_empty_levels[-2]]
                    # The narrower URI is the last non-empty level
                    narrower = row[non_empty_levels[-1]]
                    return pd.Series([broader, narrower])
                return pd.Series([None, None])
            
            # Apply the function to create broader/narrower columns
            df[['broaderUri', 'narrowerUri']] = df.apply(get_broader_narrower, axis=1)
            # Drop rows where we couldn't determine the relationship
            df = df.dropna(subset=['broaderUri', 'narrowerUri'])
            # Drop rows where broader and narrower are the same
            df = df[df['broaderUri'] != df['narrowerUri']]
            return df
            
        # Handle other formats
        if 'broaderUri' not in df.columns:
            if 'broaderConceptUri' in df.columns:
                rename_map['broaderConceptUri'] = 'broaderUri'
            elif 'parentUri' in df.columns:
                rename_map['parentUri'] = 'broaderUri'
            elif 'broaderSkillUri' in df.columns:
                rename_map['broaderSkillUri'] = 'broaderUri'

        if 'narrowerUri' not in df.columns:
            if 'narrowerConceptUri' in df.columns:
                rename_map['narrowerConceptUri'] = 'narrowerUri'
            elif 'childUri' in df.columns:
                rename_map['childUri'] = 'narrowerUri'
            elif 'conceptUri' in df.columns:
                rename_map['conceptUri'] = 'narrowerUri'
            elif 'targetUri' in df.columns:
                rename_map['targetUri'] = 'narrowerUri'
            elif 'skillUri' in df.columns:
                rename_map['skillUri'] = 'narrowerUri'

        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _standardize_collection_relation_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename alternative column names for skill‑collection relation CSVs so the rest
        of the code can safely assume `conceptSchemeUri` and `skillUri`.

        Handles variants such as:
        - collectionUri / conceptScheme / schemeUri  → conceptSchemeUri
        - conceptUri / targetUri / skillID           → skillUri
        """
        rename_map = {}
        if 'conceptSchemeUri' not in df.columns:
            if 'collectionUri' in df.columns:
                rename_map['collectionUri'] = 'conceptSchemeUri'
            elif 'conceptScheme' in df.columns:
                rename_map['conceptScheme'] = 'conceptSchemeUri'
            elif 'schemeUri' in df.columns:
                rename_map['schemeUri'] = 'conceptSchemeUri'
        if 'skillUri' not in df.columns:
            if 'conceptUri' in df.columns:
                rename_map['conceptUri'] = 'skillUri'
            elif 'targetUri' in df.columns:
                rename_map['targetUri'] = 'skillUri'
            elif 'skillID' in df.columns:
                rename_map['skillID'] = 'skillUri'
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def close(self):
        """Close the Weaviate connection"""
        # Weaviate client doesn't need explicit closing
        pass

    def delete_all_data(self):
        """Delete all data from Weaviate"""
        try:
            # Delete all collections
            for collection in ["Occupation", "Skill", "ISCOGroup", "SkillCollection", "SkillGroup"]:
                if self.occupation_repo.client.client.schema.exists(collection):
                    self.occupation_repo.client.client.schema.delete_class(collection)
            
            # Reset schema initialization flag
            self.client._schema_initialized = False
            
            logger.info("Deleted all data from Weaviate")
        except Exception as e:
            logger.error(f"Error deleting Weaviate data: {str(e)}")
            raise

    def check_class_exists(self, class_name: str) -> bool:
        """Check if a class exists in Weaviate and has data"""
        try:
            repo = self.client.get_repository(class_name)
            # Use repo to check if class has any objects
            # We'll use the check_object_exists method with a dummy UUID (since we don't know a specific one)
            # Instead, use the aggregate query to check count
            if not repo.client.client.schema.exists(class_name):
                return False
            result = repo.client.client.query.aggregate(class_name).with_meta_count().do()
            count = result.get('data', {}).get('Aggregate', {}).get(class_name, [{}])[0].get('meta', {}).get('count', 0)
            return count > 0
        except Exception as e:
            logger.error(f"Error checking existence of {class_name}: {str(e)}")
            return False

    def ingest_isco_groups(self):
        """Ingest ISCO groups into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'ISCOGroups_en.csv')
        logger.info(f"Starting ISCO groups ingestion from {file_path}")
        
        try:
            # Read CSV with explicit string dtypes for critical fields
            df = pd.read_csv(file_path, dtype={
                'conceptUri': str,
                'code': str,
                'preferredLabel': str,
                'description': str
            })
            
            # Convert NaN to empty string for string columns
            string_columns = ['conceptUri', 'code', 'preferredLabel', 'description']
            df[string_columns] = df[string_columns].fillna('')
            
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
                                "code": str(row['code']),  # Ensure code is string
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
                    self.isco_group_repo.batch_import(groups_to_import, group_vectors)
                    logger.info(f"Successfully ingested {len(groups_to_import)} ISCO groups into Weaviate")
                except Exception as e:
                    failed_imports = len(groups_to_import)
                    logger.error(f"Failed to import ISCO groups batch: {str(e)}")
                    raise
            else:
                logger.warning("No ISCO groups were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"ISCO groups ingestion completed:")
            logger.info(f">> Total groups processed: {total_groups}")
            logger.info(f">> Successfully embedded: {len(groups_to_import)}")
            logger.info(f">> Failed embeddings: {failed_embeddings}")
            logger.info(f">> Failed imports: {failed_imports}")
            
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
                    self.occupation_repo.batch_import(occupations_to_import, occupation_vectors)
                    logger.info(f"Successfully ingested {len(occupations_to_import)} occupations into Weaviate")
                except Exception as e:
                    failed_imports = len(occupations_to_import)
                    logger.error(f"Failed to import occupations batch: {str(e)}")
                    raise
            else:
                logger.warning("No occupations were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"Occupation ingestion completed:")
            logger.info(f">> Total occupations processed: {total_occupations}")
            logger.info(f">> Successfully embedded: {len(occupations_to_import)}")
            logger.info(f">> Failed embeddings: {failed_embeddings}")
            logger.info(f">> Failed imports: {failed_imports}")
            
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
                    self.skill_repo.batch_import(skills_to_import, skill_vectors)
                    logger.info(f"Successfully ingested {len(skills_to_import)} skills into Weaviate")
                except Exception as e:
                    failed_imports = len(skills_to_import)
                    logger.error(f"Failed to import skills batch: {str(e)}")
                    raise
            else:
                logger.warning("No skills were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"Skills ingestion completed:")
            logger.info(f">> Total skills processed: {total_skills}")
            logger.info(f">> Successfully embedded: {len(skills_to_import)}")
            logger.info(f">> Failed embeddings: {failed_embeddings}")
            logger.info(f">> Failed imports: {failed_imports}")
            
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
                    self.occupation_repo.add_skill_relations(
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
        """Create occupation and skill hierarchical relations in Weaviate."""
        # ----------------------------------------------------------- #
        # Occupation hierarchy
        # ----------------------------------------------------------- #
        occupation_hierarchy_path = os.path.join(self.esco_dir, 'broaderRelationsOccPillar_en.csv')
        if os.path.exists(occupation_hierarchy_path):
            df = pd.read_csv(occupation_hierarchy_path)
            df = self._standardize_hierarchy_columns(df)

            required_cols = {'broaderUri', 'narrowerUri'}
            missing_cols = required_cols - set(df.columns)
            if missing_cols:
                logger.error(f"Occupation hierarchy file missing columns: {missing_cols}. Skipping occupation hierarchy creation.")
            else:
                df[list(required_cols)] = df[list(required_cols)].fillna('')
                total_relations = len(df)
                logger.info(f"Creating occupation hierarchy relations for {total_relations} relations...")

                with tqdm(total=total_relations, desc="Creating Occupation Hierarchy", unit="relation") as pbar:
                    for _, row in df.iterrows():
                        try:
                            broader_uuid = row['broaderUri'].split('/')[-1]
                            narrower_uuid = row['narrowerUri'].split('/')[-1]

                            # Check if both occupations exist before creating relation
                            broader_exists = self.skill_repo.check_object_exists("Occupation", broader_uuid)
                            narrower_exists = self.skill_repo.check_object_exists("Occupation", narrower_uuid)
                            
                            if not broader_exists:
                                logger.warning(f"Broader occupation {broader_uuid} not found - skipping relation")
                                continue
                            if not narrower_exists:
                                logger.warning(f"Narrower occupation {narrower_uuid} not found - skipping relation")
                                continue

                            self.skill_repo.add_hierarchical_relation(
                                broader_uri=broader_uuid,
                                narrower_uri=narrower_uuid,
                                relation_type="Occupation"
                            )
                        except Exception as e:
                            logger.error(f"Failed to add occupation hierarchy relation: {str(e)}")
                            continue
                        pbar.update(1)

        # ----------------------------------------------------------- #
        # Skill hierarchy
        # ----------------------------------------------------------- #
        skill_hierarchy_path = os.path.join(self.esco_dir, 'skillsHierarchy_en.csv')
        if os.path.exists(skill_hierarchy_path):
            df = pd.read_csv(skill_hierarchy_path)
            df = self._standardize_hierarchy_columns(df)

            required_cols = {'broaderUri', 'narrowerUri'}
            missing_cols = required_cols - set(df.columns)
            if missing_cols:
                logger.error(f"Skill hierarchy file missing columns: {missing_cols}. Skipping skill hierarchy creation.")
            else:
                df[list(required_cols)] = df[list(required_cols)].fillna('')
                df = df[(df['broaderUri'] != '') & (df['narrowerUri'] != '')]

                total_relations = len(df)
                logger.info(f"Creating skill hierarchy relations for {total_relations} relations...")

                with tqdm(total=total_relations, desc="Creating Skill Hierarchy", unit="relation") as pbar:
                    for _, row in df.iterrows():
                        try:
                            broader_uuid = row['broaderUri'].split('/')[-1]
                            narrower_uuid = row['narrowerUri'].split('/')[-1]

                            self.skill_repo.add_hierarchical_relation(
                                broader_uri=broader_uuid,
                                narrower_uri=narrower_uuid,
                                relation_type="Skill"
                            )
                        except Exception as e:
                            logger.error(f"Failed to add skill hierarchy relation: {str(e)}")
                            continue
                        pbar.update(1)

    def create_isco_group_relations(self):
        """
        Link each Occupation to its parent ISCOGroup using the iscoGroup URI column
        in the occupations CSV. If the Weaviate client exposes
        `add_occupation_group_relation`, it will be used; otherwise the method
        exits with a warning so the CLI does not crash.
        """
        file_path = os.path.join(self.esco_dir, 'occupations_en.csv')
        if not os.path.exists(file_path):
            logger.error(f"Occupations file not found at {file_path}; cannot create ISCO‑group relations.")
            return

        df = pd.read_csv(file_path, dtype={'conceptUri': str, 'iscoGroup': str})
        df[['conceptUri', 'iscoGroup']] = df[['conceptUri', 'iscoGroup']].fillna('')
        df = df[(df['conceptUri'] != '') & (df['iscoGroup'] != '')]  # keep only valid rows

        total_links = len(df)
        logger.info(f"Creating {total_links} Occupation → ISCOGroup relations...")

        with tqdm(total=total_links, desc="Linking ISCO Groups", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    occupation_uuid = row['conceptUri'].split('/')[-1]
                    group_uuid = row['iscoGroup'].split('/')[-1]
                    self.occupation_repo.add_occupation_group_relation(
                        occupation_uri=occupation_uuid,
                        group_uri=group_uuid
                    )
                except Exception as e:
                    logger.error(f"Failed to link occupation to ISCO group: {str(e)}")
                    continue
                pbar.update(1)

    def ingest_skill_groups(self):
        """Ingest skill groups into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'skillGroups_en.csv')
        logger.info(f"Starting skill groups ingestion from {file_path}")
        
        try:
            required_columns = {"conceptUri", "preferredLabel"}
            df = pd.read_csv(file_path, dtype={
                'conceptUri': str,
                'preferredLabel': str,
                'altLabels': str,
                'description': str,
                'code': str
            })
            
            logger.debug(f"Skill groups CSV columns detected: {list(df.columns)}")
            missing = required_columns - set(df.columns)
            if missing:
                raise ValueError(f"Skill groups file is missing required columns: {missing}")

            string_columns = ['conceptUri', 'preferredLabel', 'altLabels', 'description', 'code']
            present_cols = list(set(string_columns).intersection(df.columns))
            df[present_cols] = df[present_cols].fillna('')
            
            total_groups = len(df)
            logger.info(f"Found {total_groups} skill groups to process")
            
            group_vectors = []
            groups_to_import = []
            failed_embeddings = 0
            
            logger.info("Generating embeddings for skill groups...")
            with tqdm(total=total_groups, desc="Embedding Skill Groups", unit="group") as pbar:
                for idx, row in df.iterrows():
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
                            if isinstance(embedding, np.ndarray):
                                embedding = embedding.tolist()
                            elif isinstance(embedding, list):
                                embedding = [float(x) for x in embedding]
                            else:
                                raise ValueError(f"Unexpected embedding type: {type(embedding)}")
                                
                            uuid = row['conceptUri'].split('/')[-1]
                            alt_labels = row.get('altLabels', '').split('|') if row.get('altLabels') else []
                            
                            group_vectors.append(embedding)
                            groups_to_import.append({
                                "conceptUri": uuid,
                                "code": row.get('code', ''),
                                "preferredLabel_en": row['preferredLabel'],
                                "altLabels_en": alt_labels,
                                "description_en": row.get('description', '')
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for skill group: {row.get('conceptUri', 'Unknown URI')} - {row.get('preferredLabel', 'Unknown Label')}")
                    except Exception as e:
                        failed_embeddings += 1
                        logger.error(f"Error processing skill group at index {idx}: {str(e)}")
                    pbar.update(1)
            
            failed_imports = 0
            if groups_to_import and group_vectors:
                logger.info(f"Starting batch import of {len(groups_to_import)} skill groups")
                try:
                    self.skill_group_repo.batch_import(groups_to_import, group_vectors)
                    logger.info(f"Successfully ingested {len(groups_to_import)} skill groups into Weaviate")
                except Exception as e:
                    failed_imports = len(groups_to_import)
                    logger.error(f"Failed to import skill groups batch: {str(e)}")
                    # raise # Decide if this should halt execution
            else:
                logger.warning("No skill groups were imported into Weaviate.")
            
            logger.info(f"Skill groups ingestion completed:")
            logger.info(f">> Total groups processed: {total_groups}")
            logger.info(f">> Successfully embedded: {len(groups_to_import)}")
            logger.info(f">> Failed embeddings: {failed_embeddings}")
            logger.info(f">> Failed imports: {failed_imports}")
            
        except Exception as e:
            logger.error(f"Error during skill groups ingestion: {str(e)}")
            # raise # Decide if this should halt execution

    def ingest_skill_collections(self):
        """Ingest skill collections into Weaviate"""
        file_path = os.path.join(self.esco_dir, 'conceptSchemes_en.csv')
        logger.info(f"Starting skill collections ingestion from {file_path}")
        
        try:
            # Define required columns
            required_columns = {"conceptSchemeUri", "preferredLabel"}
            
            # Read CSV with explicit string dtypes for critical fields
            df = pd.read_csv(file_path, dtype={
                'conceptSchemeUri': str,
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
            string_columns = ['conceptSchemeUri', 'preferredLabel', 'description']
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
                        # Skip rows with missing conceptSchemeUri
                        if not row['conceptSchemeUri'] or row['conceptSchemeUri'].lower() == 'nan':
                            logger.warning(f"Skipping row {idx} - missing conceptSchemeUri")
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
                            source_uri_value = row['conceptSchemeUri']
                            uuid = source_uri_value.split('/')[-1]
                            
                            collection_vectors.append(embedding)
                            collections_to_import.append({
                                "conceptUri": uuid,  # Use just the UUID part
                                "preferredLabel_en": row['preferredLabel'],
                                "description_en": row.get('description', '')
                            })
                        else:
                            failed_embeddings += 1
                            logger.warning(f"Could not generate embedding for skill collection: {row.get('conceptSchemeUri', 'Unknown URI')} - {row.get('preferredLabel', 'Unknown Label')}")
                    except Exception as e:
                        failed_embeddings += 1
                        logger.error(f"Error processing skill collection at index {idx}: {str(e)}")
                    
                    pbar.update(1)
            
            # Import skill collections in batches
            if collections_to_import and collection_vectors:
                logger.info(f"Starting batch import of {len(collections_to_import)} skill collections")
                try:
                    self.skill_collection_repo.batch_import(collections_to_import, collection_vectors)
                    logger.info(f"Successfully ingested {len(collections_to_import)} skill collections into Weaviate")
                except Exception as e:
                    failed_imports = len(collections_to_import)
                    logger.error(f"Failed to import skill collections batch: {str(e)}")
                    raise
            else:
                logger.warning("No skill collections were imported into Weaviate, possibly due to embedding failures or empty input.")
            
            # Log final statistics
            logger.info(f"Skill collections ingestion completed:")
            logger.info(f">> Total collections processed: {total_collections}")
            logger.info(f">> Successfully embedded: {len(collections_to_import)}")
            logger.info(f">> Failed embeddings: {failed_embeddings}")
            logger.info(f">> Failed imports: {failed_imports}")
            
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
            
            # **FIX 1: Standardize column names**
            df = self._standardize_collection_relation_columns(df)
            
            # **FIX 2: Use standardized column names**
            if 'conceptSchemeUri' not in df.columns or 'skillUri' not in df.columns:
                logger.error(f"Collection file {collection_file} missing required columns after standardization. "
                            f"Available columns: {list(df.columns)}. Skipping.")
                continue

            # Clean NaNs and drop invalid rows
            df[['conceptSchemeUri', 'skillUri']] = df[['conceptSchemeUri', 'skillUri']].fillna('')
            df = df[(df['conceptSchemeUri'] != '') & (df['skillUri'] != '')]

            total_relations = len(df)
            if total_relations == 0:
                logger.warning(f"No valid relations found in {collection_file} – skipping.")
                continue

            logger.info(f"Found {total_relations} relations in {collection_file}")

            with tqdm(total=total_relations, desc=f"Creating {os.path.basename(collection_file)} Relations", unit="relation") as pbar:
                for _, row in df.iterrows():
                    try:
                        # **FIX 3: Handle collection URIs properly**
                        # Some files might have multiple collection URIs separated by ' | '
                        collection_uris_raw = str(row['conceptSchemeUri'])
                        if ' | ' in collection_uris_raw:
                            collection_uris = collection_uris_raw.split(' | ')
                        else:
                            collection_uris = [collection_uris_raw]
                        
                        skill_uri_raw = str(row['skillUri'])
                        skill_uuid = skill_uri_raw.split('/')[-1]

                        # Check if skill exists
                        skill_exists = self.skill_repo.check_object_exists(skill_uuid)
                        if not skill_exists:
                            logger.warning(f"Skill {skill_uuid} not found - skipping relation")
                            continue

                        # Add relation for each collection URI
                        relations_added = 0
                        for collection_uri in collection_uris:
                            collection_uri = collection_uri.strip()  # Remove any whitespace
                            if not collection_uri:
                                continue
                                
                            collection_uuid = collection_uri.split('/')[-1]
                            
                            # Check if collection exists
                            collection_exists = self.skill_collection_repo.check_object_exists(collection_uuid)
                            if not collection_exists:
                                logger.debug(f"Skill collection {collection_uuid} (from URI: {collection_uri}) not found - skipping relation")
                                continue

                            # Add relation
                            success = self.skill_collection_repo.add_skill_collection_relation(
                                collection_uri=collection_uuid,
                                skill_uri=skill_uuid
                            )
                            if success:
                                relations_added += 1
                            else:
                                logger.warning(f"Failed to add relation between collection {collection_uuid} and skill {skill_uuid}")
                        
                        if relations_added == 0:
                            logger.warning(f"No relations added for skill {skill_uuid} - none of the collection URIs were found: {collection_uris}")
                    
                    except Exception as e:
                        logger.error(f"Failed to add skill collection relation for row: {str(e)}")
                        continue
                    pbar.update(1)

            logger.info(f"Completed processing {collection_file}")

    def create_skill_skill_relations(self):
        """Create skill-to-skill relations in Weaviate from skillSkillRelations_en.csv."""
        file_path = os.path.join(self.esco_dir, 'skillSkillRelations_en.csv')
        if not os.path.exists(file_path):
            logger.warning(f"Skill-skill relations file not found: {file_path}, skipping this step.")
            return

        logger.info(f"Processing skill-to-skill relations from {file_path}")
        df = pd.read_csv(file_path, dtype=str) # Ensure all URI columns are read as strings
        df = df.fillna('') # Fill NaN with empty strings to avoid errors

        required_cols = {'originalSkillUri', 'relatedSkillUri', 'relationType'}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            logger.error(f"Skill-skill relations file {file_path} is missing columns: {missing_cols}. Skipping.")
            return

        # Filter out rows with missing essential URIs
        df = df[(df['originalSkillUri'] != '') & (df['relatedSkillUri'] != '')]
        if df.empty:
            logger.info(f"No valid skill-skill relations found in {file_path}.")
            return
            
        total_relations = len(df)
        logger.info(f"Creating {total_relations} skill-to-skill relations...")

        with tqdm(total=total_relations, desc="Creating Skill-Skill Relations", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    from_skill_uuid = row['originalSkillUri'].split('/')[-1]
                    to_skill_uuid = row['relatedSkillUri'].split('/')[-1]
                    relation_type = row['relationType']

                    # Check existence before creating relation
                    from_skill_exists = self.skill_repo.check_object_exists(from_skill_uuid)
                    to_skill_exists = self.skill_repo.check_object_exists(to_skill_uuid)

                    if not from_skill_exists:
                        logger.warning(f"Original skill {from_skill_uuid} not found. Skipping relation to {to_skill_uuid}.")
                        pbar.update(1)
                        continue
                    if not to_skill_exists:
                        logger.warning(f"Related skill {to_skill_uuid} not found. Skipping relation from {from_skill_uuid}.")
                        pbar.update(1)
                        continue
                    
                    self.skill_repo.add_skill_to_skill_relation(
                        from_skill_uri=from_skill_uuid,
                        to_skill_uri=to_skill_uuid,
                        relation_type=relation_type
                    )
                except Exception as e:
                    logger.error(f"Failed to add skill-to-skill relation ({row.get('originalSkillUri')} -> {row.get('relatedSkillUri')}): {str(e)}")
                pbar.update(1)
        logger.info(f"Finished creating skill-to-skill relations.")

    def create_broader_skill_relations(self):
        """Create broader skill relations in Weaviate"""
        file_path = os.path.join(self.esco_dir, 'broaderRelationsSkillPillar_en-small.csv')
        if not os.path.exists(file_path):
            logger.warning(f"Broader relations file not found: {file_path}")
            return

        logger.info("Processing broader skill relations")
        df = pd.read_csv(file_path)

        # Clean NaNs and drop invalid rows
        df[['conceptUri', 'broaderUri']] = df[['conceptUri', 'broaderUri']].fillna('')
        df = df[(df['conceptUri'] != '') & (df['broaderUri'] != '')]

        total_relations = len(df)
        if total_relations == 0:
            logger.warning("No valid relations found in broader relations file – skipping.")
            return

        with tqdm(total=total_relations, desc="Creating Broader Skill Relations", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    # Extract UUIDs from the full URIs
                    skill_uuid = row['conceptUri'].split('/')[-1]
                    broader_uuid = row['broaderUri'].split('/')[-1]

                    # Check if both skills exist before creating relation
                    skill_exists = self.skill_repo.check_object_exists(skill_uuid)
                    broader_exists = self.skill_repo.check_object_exists(broader_uuid)

                    if not skill_exists:
                        logger.warning(f"Skill {skill_uuid} not found - skipping relation")
                        continue
                    if not broader_exists:
                        logger.warning(f"Broader skill {broader_uuid} not found - skipping relation")
                        continue

                    # Add relation
                    self.skill_repo.add_broader_skill_relation(
                        skill_uri=skill_uuid,
                        broader_uri=broader_uuid
                    )
                except Exception as e:
                    logger.error(f"Failed to add broader skill relation: {str(e)}")
                    continue
                pbar.update(1)

    def run_ingest(self, force_reingest: bool = False):
        """Run the complete Weaviate ingestion process"""
        try:
            # Create a progress bar for the overall process
            with tqdm(total=11, desc="ESCO Ingestion Progress", unit="step",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                
                # Step 1: Initialize schema or delete all data if force_reingest
                if force_reingest:
                    self.delete_all_data()
                else:
                    self.initialize_schema()
                pbar.update(1)
                
                # Check for existing data and handle re-ingestion
                if not force_reingest:
                    existing_classes = []
                    for class_name in ["ISCOGroup", "Occupation", "Skill", "SkillCollection", "SkillGroup"]:
                        if self.check_class_exists(class_name):
                            existing_classes.append(class_name)
                    
                    if existing_classes:
                        logger.warning(f"Found existing data for classes: {', '.join(existing_classes)}")
                        if not click.confirm("Do you want to re-ingest these classes?", default=False):
                            logger.info("Skipping re-ingestion of existing classes")
                            # Skip the ingestion steps for existing classes
                            for class_name in existing_classes:
                                if class_name in ["ISCOGroup", "Occupation", "Skill", "SkillCollection", "SkillGroup"]:
                                     pbar.update(1) # only update for main entity types
                            return
                
                # Step 2: Ingest ISCO groups if not exists or force_reingest
                if force_reingest or not self.check_class_exists("ISCOGroup"):
                    self.ingest_isco_groups()
                pbar.update(1)
                
                # Step 3: Ingest occupations if not exists or force_reingest
                if force_reingest or not self.check_class_exists("Occupation"):
                    self.ingest_occupations()
                pbar.update(1)
                
                # Step 4: Ingest skills if not exists or force_reingest
                if force_reingest or not self.check_class_exists("Skill"):
                    self.ingest_skills()
                pbar.update(1)
                
                # Step 5: Ingest skill collections if not exists or force_reingest
                if force_reingest or not self.check_class_exists("SkillCollection"):
                    self.ingest_skill_collections()
                pbar.update(1)

                # Step 6: Ingest skill groups if not exists or force_reingest
                if force_reingest or not self.check_class_exists("SkillGroup"):
                    self.ingest_skill_groups()
                pbar.update(1)
                
                # Step 7: Create occupation skill relations
                self.create_skill_relations() # This links Occupations to Skills
                pbar.update(1)
                
                # Step 8: Create hierarchical relations (Occupations and Skills)
                self.create_hierarchical_relations()
                pbar.update(1)

                # Step 9: Create ISCO group relations (Occupation to ISCOGroup)
                self.create_isco_group_relations()
                pbar.update(1)
                
                # Step 10: Create skill collection relations (SkillCollection to Skill)
                self.create_skill_collection_relations()
                pbar.update(1)

                # Step 11: Create skill-to-skill relations
                self.create_skill_skill_relations()
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