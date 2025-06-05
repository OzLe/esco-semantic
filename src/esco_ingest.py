import os
import pandas as pd
from tqdm import tqdm
import argparse
import yaml
from abc import ABC, abstractmethod
import numpy as np
from datetime import datetime

# Local imports
from src.esco_weaviate_client import WeaviateClient
from src.embedding_utils import ESCOEmbedding
from src.logging_config import setup_logging
from src.weaviate_semantic_search import ESCOSemanticSearch

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

    def process_csv_in_batches(self, file_path, process_func, heartbeat_callback=None):
        """
        Process a CSV file in batches with optional heartbeat updates.
        
        Args:
            file_path: Path to the CSV file
            process_func: Function to process each batch
            heartbeat_callback: Optional callback function for heartbeat updates
        """
        df = pd.read_csv(file_path)
        total_rows = len(df)
        rows_processed = 0
        
        with tqdm(total=total_rows, desc=f"Processing {os.path.basename(file_path)}", unit="rows",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            for start_idx in range(0, total_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]
                process_func(batch)
                rows_processed += len(batch)
                pbar.update(len(batch))
                
                # Send heartbeat every 1000 rows
                if heartbeat_callback and rows_processed % 1000 == 0:
                    heartbeat_callback(rows_processed, total_rows)

    @abstractmethod
    def run_simple_ingestion(self):
        """Run a simplified ingestion process without business logic"""
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
        """Close database connection"""
        if self.client:
            # Weaviate client doesn't need explicit closing
            pass

    def delete_all_data(self):
        """Delete all data from the database"""
        try:
            logger.info("Deleting all data from Weaviate...")
            
            # Delete schema which removes all data
            self.client.delete_schema()
            logger.info("All data deleted successfully")
            
            # Recreate schema
            self.client.ensure_schema()
            logger.info("Schema recreated")
            
        except Exception as e:
            logger.error(f"Failed to delete all data: {str(e)}")
            raise

    def check_class_exists(self, class_name: str) -> bool:
        """
        Check if a class exists and has data.
        
        Args:
            class_name: Name of the class to check
            
        Returns:
            bool: True if class exists and has data, False otherwise
        """
        try:
            repo = self.client.get_repository(class_name)
            count = repo.count_objects()
            return count > 0
        except Exception as e:
            logger.warning(f"Error checking if class {class_name} exists: {str(e)}")
            return False

    def ingest_isco_groups(self):
        """Ingest ISCO groups into Weaviate"""
        file_path = os.path.join(self.esco_dir, "ISCOGroups_en.csv")
        if not os.path.exists(file_path):
            logger.warning(f"ISCO groups file not found: {file_path} – skipping.")
            return

        logger.info(f"Ingesting ISCO groups from {file_path}")

        def process_batch(batch):
            for _, row in batch.iterrows():
                try:
                    isco_group_data = {
                        "uri": row["conceptUri"],
                        "code": row.get("code", ""),
                        "preferredLabel_en": row.get("preferredLabel", ""),
                        "description_en": row.get("description", ""),
                        "iscoLevel": row.get("iscoLevel", ""),
                    }

                    # Clean empty values
                    isco_group_data = {k: v for k, v in isco_group_data.items() if v is not None and v != ""}

                    # Create UUID from URI
                    uuid = isco_group_data["uri"].split("/")[-1]

                    self.isco_group_repo.create_object(
                        properties=isco_group_data,
                        uuid=uuid
                    )

                except Exception as e:
                    logger.error(f"Failed to ingest ISCO group {row.get('conceptUri', 'unknown')}: {str(e)}")
                    continue

        self.process_csv_in_batches(file_path, process_batch)
        logger.info("ISCO group ingestion completed")

    def ingest_occupations(self):
        """Ingest occupations from CSV file."""
        logger.info("Starting occupation ingestion...")
        
        def process_batch(batch):
            # Process each occupation
            for _, row in batch.iterrows():
                try:
                    # Create occupation object
                    occupation = {
                        "conceptUri": row["conceptUri"],
                        "preferredLabel_en": row["preferredLabel_en"],
                        "description_en": row.get("description_en", ""),
                        "definition_en": row.get("definition_en", ""),
                        "code": row.get("code", ""),
                        "altLabels_en": row.get("altLabels_en", "").split("|") if pd.notna(row.get("altLabels_en")) else []
                    }
                    
                    # Add to repository
                    self.occupation_repo.add(occupation)
                    
                except Exception as e:
                    logger.error(f"Error processing occupation {row.get('conceptUri', 'unknown')}: {str(e)}")
        
        def update_heartbeat(processed, total):
            self.client.set_ingestion_metadata(
                status="in_progress",
                details={
                    "step": "ingest_occupations",
                    "progress": f"{processed}/{total}",
                    "last_heartbeat": datetime.utcnow().isoformat()
                }
            )
        
        # Process occupations CSV
        occupations_file = os.path.join(self.esco_dir, "occupations_en.csv")
        self.process_csv_in_batches(occupations_file, process_batch, update_heartbeat)
        logger.info("Occupation ingestion completed")

    def ingest_skills(self):
        """Ingest skills from CSV file."""
        logger.info("Starting skill ingestion...")
        
        def process_batch(batch):
            # Process each skill
            for _, row in batch.iterrows():
                try:
                    # Create skill object
                    skill = {
                        "conceptUri": row["conceptUri"],
                        "preferredLabel_en": row["preferredLabel_en"],
                        "description_en": row.get("description_en", ""),
                        "skillType": row.get("skillType", ""),
                        "reuseLevel": row.get("reuseLevel", ""),
                        "altLabels_en": row.get("altLabels_en", "").split("|") if pd.notna(row.get("altLabels_en")) else []
                    }
                    
                    # Add to repository
                    self.skill_repo.add(skill)
                    
                except Exception as e:
                    logger.error(f"Error processing skill {row.get('conceptUri', 'unknown')}: {str(e)}")
        
        def update_heartbeat(processed, total):
            self.client.set_ingestion_metadata(
                status="in_progress",
                details={
                    "step": "ingest_skills",
                    "progress": f"{processed}/{total}",
                    "last_heartbeat": datetime.utcnow().isoformat()
                }
            )
        
        # Process skills CSV
        skills_file = os.path.join(self.esco_dir, "skills_en.csv")
        self.process_csv_in_batches(skills_file, process_batch, update_heartbeat)
        logger.info("Skill ingestion completed")

    def create_skill_relations(self):
        """Create occupation-skill relations"""
        file_path = os.path.join(self.esco_dir, "occupationSkillRelations.csv")
        if not os.path.exists(file_path):
            logger.warning(f"Occupation-skill relations file not found: {file_path} – skipping.")
            return

        logger.info(f"Creating occupation-skill relations from {file_path}")

        df = pd.read_csv(file_path)
        total_relations = len(df)

        if total_relations == 0:
            logger.warning("No occupation-skill relations found – skipping.")
            return

        with tqdm(total=total_relations, desc="Creating Occupation-Skill Relations", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    # Extract UUIDs from the full URIs
                    occupation_uuid = row['occupationUri'].split('/')[-1]
                    skill_uuid = row['skillUri'].split('/')[-1]
                    relation_type = row.get('relationType', 'related')

                    # Check if both objects exist before creating relation
                    occupation_exists = self.occupation_repo.check_object_exists(occupation_uuid)
                    skill_exists = self.skill_repo.check_object_exists(skill_uuid)

                    if not occupation_exists:
                        logger.warning(f"Occupation {occupation_uuid} not found - skipping relation")
                        continue
                    if not skill_exists:
                        logger.warning(f"Skill {skill_uuid} not found - skipping relation")
                        continue

                    # Add relation based on type
                    if relation_type == 'essential':
                        self.occupation_repo.add_essential_skill_relation(
                            occupation_uri=occupation_uuid,
                            skill_uri=skill_uuid
                        )
                    elif relation_type == 'optional':
                        self.occupation_repo.add_optional_skill_relation(
                            occupation_uri=occupation_uuid,
                            skill_uri=skill_uuid
                        )
                    else:
                        # Default to essential if type is unclear
                        self.occupation_repo.add_essential_skill_relation(
                            occupation_uri=occupation_uuid,
                            skill_uri=skill_uuid
                        )
                except Exception as e:
                    logger.error(f"Failed to create occupation-skill relation: {str(e)}")
                    continue
                pbar.update(1)

    def create_hierarchical_relations(self):
        """Create hierarchical relations between occupations"""
        file_path = os.path.join(self.esco_dir, "broaderRelationsOccupationPillar.csv")
        if not os.path.exists(file_path):
            logger.warning(f"Hierarchical relations file not found: {file_path} – skipping.")
            return

        logger.info(f"Creating hierarchical relations from {file_path}")

        df = pd.read_csv(file_path)
        df = self._standardize_hierarchy_columns(df)
        
        if 'broaderUri' not in df.columns or 'narrowerUri' not in df.columns:
            logger.warning("Required columns 'broaderUri' and 'narrowerUri' not found – skipping hierarchical relations.")
            return

        total_relations = len(df)

        if total_relations == 0:
            logger.warning("No hierarchical relations found – skipping.")
            return

        with tqdm(total=total_relations, desc="Creating Hierarchical Relations", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    # Extract UUIDs from the full URIs
                    broader_uuid = row['broaderUri'].split('/')[-1]
                    narrower_uuid = row['narrowerUri'].split('/')[-1]

                    # Check if both occupations exist before creating relation
                    broader_exists = self.occupation_repo.check_object_exists(broader_uuid)
                    narrower_exists = self.occupation_repo.check_object_exists(narrower_uuid)

                    if not broader_exists:
                        logger.warning(f"Broader occupation {broader_uuid} not found - skipping relation")
                        continue
                    if not narrower_exists:
                        logger.warning(f"Narrower occupation {narrower_uuid} not found - skipping relation")
                        continue

                    # Add broader occupation relation
                    self.occupation_repo.add_broader_occupation_relation(
                        occupation_uri=narrower_uuid,
                        broader_uri=broader_uuid
                    )
                except Exception as e:
                    logger.error(f"Failed to create hierarchical relation: {str(e)}")
                    continue
                pbar.update(1)

    def create_isco_group_relations(self):
        """Create relations between occupations and ISCO groups"""
        logger.info("Creating ISCO group relations...")

        # Get all occupations and update their ISCO group relations
        try:
            # We'll iterate through all occupations and link them to their ISCO groups
            occupations = self.occupation_repo.get_all_objects()
            
            with tqdm(total=len(occupations), desc="Creating ISCO Group Relations", unit="occupation") as pbar:
                for occupation in occupations:
                    try:
                        isco_code = occupation.get('iscoCode')
                        if not isco_code:
                            continue

                        # Find the ISCO group with this code
                        isco_groups = self.isco_group_repo.get_objects_by_property('code', isco_code)
                        if isco_groups:
                            isco_group_uuid = isco_groups[0]['_id']
                            occupation_uuid = occupation['_id']
                            
                            # Add relation
                            self.occupation_repo.add_isco_group_relation(
                                occupation_uri=occupation_uuid,
                                isco_group_uri=isco_group_uuid
                            )
                    except Exception as e:
                        logger.error(f"Failed to create ISCO group relation: {str(e)}")
                        continue
                    pbar.update(1)
                    
        except Exception as e:
            logger.error(f"Error creating ISCO group relations: {str(e)}")

    def ingest_skill_groups(self):
        """Ingest skill groups from CSV file."""
        logger.info("Starting skill group ingestion...")
        
        def process_batch(batch):
            # Process each skill group
            for _, row in batch.iterrows():
                try:
                    # Create skill group object
                    skill_group = {
                        "conceptUri": row["conceptUri"],
                        "preferredLabel_en": row["preferredLabel_en"],
                        "description_en": row.get("description_en", ""),
                        "altLabels_en": row.get("altLabels_en", "").split("|") if pd.notna(row.get("altLabels_en")) else []
                    }
                    
                    # Add to repository
                    self.skill_group_repo.add(skill_group)
                    
                except Exception as e:
                    logger.error(f"Error processing skill group {row.get('conceptUri', 'unknown')}: {str(e)}")
        
        def update_heartbeat(processed, total):
            self.client.set_ingestion_metadata(
                status="in_progress",
                details={
                    "step": "ingest_skill_groups",
                    "progress": f"{processed}/{total}",
                    "last_heartbeat": datetime.utcnow().isoformat()
                }
            )
        
        # Process skill groups CSV
        skill_groups_file = os.path.join(self.esco_dir, "skill_groups_en.csv")
        self.process_csv_in_batches(skill_groups_file, process_batch, update_heartbeat)
        logger.info("Skill group ingestion completed")

    def ingest_skill_collections(self):
        """Ingest skill collections from CSV file."""
        logger.info("Starting skill collection ingestion...")
        
        def process_batch(batch):
            # Process each skill collection
            for _, row in batch.iterrows():
                try:
                    # Create skill collection object
                    collection = {
                        "conceptUri": row["conceptUri"],
                        "preferredLabel_en": row["preferredLabel_en"],
                        "description_en": row.get("description_en", ""),
                        "altLabels_en": row.get("altLabels_en", "").split("|") if pd.notna(row.get("altLabels_en")) else []
                    }
                    
                    # Add to repository
                    self.skill_collection_repo.add(collection)
                    
                except Exception as e:
                    logger.error(f"Error processing skill collection {row.get('conceptUri', 'unknown')}: {str(e)}")
        
        def update_heartbeat(processed, total):
            self.client.set_ingestion_metadata(
                status="in_progress",
                details={
                    "step": "ingest_skill_collections",
                    "progress": f"{processed}/{total}",
                    "last_heartbeat": datetime.utcnow().isoformat()
                }
            )
        
        # Process skill collections CSV
        collections_file = os.path.join(self.esco_dir, "skill_collections_en.csv")
        self.process_csv_in_batches(collections_file, process_batch, update_heartbeat)
        logger.info("Skill collection ingestion completed")

    def create_skill_collection_relations(self):
        """Create relations between skills and skill collections"""
        file_path = os.path.join(self.esco_dir, "skillCollectionMemberSkills.csv")
        if not os.path.exists(file_path):
            logger.warning(f"Skill collection relations file not found: {file_path} – skipping.")
            return

        logger.info(f"Creating skill collection relations from {file_path}")

        df = pd.read_csv(file_path)
        df = self._standardize_collection_relation_columns(df)

        if 'conceptSchemeUri' not in df.columns or 'skillUri' not in df.columns:
            logger.warning("Required columns not found in skill collection relations file – skipping.")
            return

        total_relations = len(df)

        if total_relations == 0:
            logger.warning("No skill collection relations found – skipping.")
            return

        with tqdm(total=total_relations, desc="Creating Skill Collection Relations", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    # Extract UUIDs from the full URIs
                    collection_uuid = row['conceptSchemeUri'].split('/')[-1]
                    skill_uuid = row['skillUri'].split('/')[-1]

                    # Check if both objects exist before creating relation
                    collection_exists = self.skill_collection_repo.check_object_exists(collection_uuid)
                    skill_exists = self.skill_repo.check_object_exists(skill_uuid)

                    if not collection_exists:
                        logger.warning(f"Skill collection {collection_uuid} not found - skipping relation")
                        continue
                    if not skill_exists:
                        logger.warning(f"Skill {skill_uuid} not found - skipping relation")
                        continue

                    # Add relation
                    self.skill_repo.add_skill_collection_relation(
                        skill_uri=skill_uuid,
                        collection_uri=collection_uuid
                    )
                except Exception as e:
                    logger.error(f"Failed to create skill collection relation: {str(e)}")
                    continue
                pbar.update(1)

    def create_skill_skill_relations(self):
        """Create skill-to-skill relations"""
        file_path = os.path.join(self.esco_dir, "skillSkillRelations.csv")
        if not os.path.exists(file_path):
            logger.warning(f"Skill-skill relations file not found: {file_path} – skipping.")
            return

        logger.info(f"Creating skill-skill relations from {file_path}")

        df = pd.read_csv(file_path)
        total_relations = len(df)

        if total_relations == 0:
            logger.warning("No skill-skill relations found – skipping.")
            return

        with tqdm(total=total_relations, desc="Creating Skill-Skill Relations", unit="relation") as pbar:
            for _, row in df.iterrows():
                try:
                    # Extract UUIDs from the full URIs
                    skill_uuid = row['skillUri'].split('/')[-1]
                    related_uuid = row['relatedSkillUri'].split('/')[-1]
                    relation_type = row.get('relationType', 'related')

                    # Check if both skills exist before creating relation
                    skill_exists = self.skill_repo.check_object_exists(skill_uuid)
                    related_exists = self.skill_repo.check_object_exists(related_uuid)

                    if not skill_exists:
                        logger.warning(f"Skill {skill_uuid} not found - skipping relation")
                        continue
                    if not related_exists:
                        logger.warning(f"Related skill {related_uuid} not found - skipping relation")
                        continue

                    # Add relation
                    self.skill_repo.add_related_skill_relation(
                        skill_uri=skill_uuid,
                        related_uri=related_uuid,
                        relation_type=relation_type
                    )
                except Exception as e:
                    logger.error(f"Failed to create skill-skill relation: {str(e)}")
                    continue
                pbar.update(1)

    def create_broader_skill_relations(self):
        """Create broader skill relations"""
        file_path = os.path.join(self.esco_dir, "broaderRelationsSkillPillar.csv")
        if not os.path.exists(file_path):
            logger.warning(f"Broader skill relations file not found: {file_path} – skipping.")
            return

        logger.info(f"Creating broader skill relations from {file_path}")

        df = pd.read_csv(file_path)
        df = self._standardize_hierarchy_columns(df)

        if 'broaderUri' not in df.columns or 'conceptUri' not in df.columns:
            logger.warning("Required columns not found in broader skill relations file – skipping.")
            return

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

    def run_simple_ingestion(self):
        """
        Run a simplified ingestion process for all entities and relationships.
        
        This method contains only the data access operations without any
        business logic, status management, or user interaction.
        """
        try:
            logger.info("Starting simple ingestion process")
            
            # Initialize schema
            self.initialize_schema()
            
            # Ingest all entities
            self.ingest_isco_groups()
            self.ingest_occupations()
            self.ingest_skills()
            self.ingest_skill_groups()
            self.ingest_skill_collections()
            
            # Create all relationships
            self.create_skill_relations()
            self.create_hierarchical_relations()
            self.create_isco_group_relations()
            self.create_skill_collection_relations()
            self.create_skill_skill_relations()
            self.create_broader_skill_relations()
            
            logger.info("Simple ingestion process completed")
            
        except Exception as e:
            logger.error(f"Simple ingestion process failed: {str(e)}")
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
            # Use simple ingestion instead of the business logic heavy run_ingest
            ingestor.run_simple_ingestion()
    finally:
        ingestor.close()

if __name__ == "__main__":
    main()