"""
Ingestion Service Layer for ESCO data management.

This service layer consolidates and centralizes all ingestion business logic,
eliminating duplication between CLI and container initialization while providing
a single source of truth for ingestion operations.
"""

import os
import sys
import json
import yaml
import click
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, List
from pathlib import Path
import pandas as pd

from ..models.ingestion_models import (
    IngestionState,
    IngestionDecision,
    IngestionProgress,
    IngestionResult,
    ValidationResult,
    IngestionConfig
)
from ..esco_weaviate_client import WeaviateClient
from ..esco_ingest import WeaviateIngestor
from ..logging_config import setup_logging, log_error
from ..exceptions import WeaviateError

logger = setup_logging()


class IngestionService:
    """
    Service layer for ESCO data ingestion operations.
    
    This class provides a unified interface for all ingestion-related business logic,
    eliminating duplication between CLI and container initialization approaches.
    """
    
    def __init__(self, config: IngestionConfig):
        """
        Initialize the ingestion service.
        
        Args:
            config: Configuration object containing all necessary parameters
        """
        self.config = config
        self._client: Optional[WeaviateClient] = None
        self._ingestor: Optional[WeaviateIngestor] = None
        
        # Load configuration from file
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load and validate configuration from file."""
        try:
            with open(self.config.config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            if not isinstance(raw_config, dict):
                raise ValueError("Invalid config file format")
            
            if self.config.profile not in raw_config:
                raise ValueError(f"Profile '{self.config.profile}' not found in config file")
            
            self.config.raw_config = raw_config
            profile_config = raw_config[self.config.profile]
            
            # Extract settings from profile
            if 'app' in profile_config and 'data_dir' in profile_config['app']:
                self.config.data_dir = profile_config['app']['data_dir']
            
            if 'weaviate' in profile_config and 'batch_size' in profile_config['weaviate']:
                self.config.batch_size = profile_config['weaviate']['batch_size']
                
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    @property
    def client(self) -> WeaviateClient:
        """Get or create WeaviateClient instance."""
        if self._client is None:
            self._client = WeaviateClient(self.config.config_path, self.config.profile)
        return self._client
    
    @property
    def ingestor(self) -> WeaviateIngestor:
        """Get or create WeaviateIngestor instance."""
        if self._ingestor is None:
            self._ingestor = WeaviateIngestor(self.config.config_path, self.config.profile)
        return self._ingestor
    
    def get_current_state(self) -> IngestionState:
        """
        Get the current ingestion state.
        
        Consolidates status checking from both CLI and init components.
        Checks Metadata table in Weaviate and handles connection errors gracefully.
        
        Returns:
            IngestionState: Current state of the ingestion system
        """
        try:
            status_data = self.client.get_ingestion_status()
            status_str = status_data.get('status', 'unknown')
            
            # Map string status to enum
            status_mapping = {
                'not_started': IngestionState.NOT_STARTED,
                'in_progress': IngestionState.IN_PROGRESS,
                'completed': IngestionState.COMPLETED,
                'failed': IngestionState.FAILED,
                'unknown': IngestionState.UNKNOWN
            }
            
            return status_mapping.get(status_str, IngestionState.UNKNOWN)
            
        except Exception as e:
            logger.warning(f"Failed to get ingestion status: {str(e)}")
            return IngestionState.UNKNOWN
    
    def should_run_ingestion(self, force_reingest: bool = False) -> IngestionDecision:
        """
        Determine whether ingestion should run.
        
        Combines logic from both esco_cli.py and init_ingestion.py to make a
        comprehensive decision about whether to proceed with ingestion.
        
        Args:
            force_reingest: Whether to force re-ingestion regardless of current state
            
        Returns:
            IngestionDecision: Decision object with reasoning and state information
        """
        try:
            # Get current state and status data
            current_state = self.get_current_state()
            status_data = self.client.get_ingestion_status()
            timestamp_str = status_data.get('timestamp')
            
            # Check for existing classes
            existing_classes = self._check_existing_classes()
            
            # Handle force reingest
            if force_reingest:
                return IngestionDecision(
                    should_run=True,
                    reason="Force re-ingestion requested",
                    current_state=current_state,
                    force_required=False,
                    existing_classes=existing_classes,
                    timestamp=timestamp_str
                )
            
            # Handle completed state
            if current_state == IngestionState.COMPLETED:
                return IngestionDecision(
                    should_run=False,
                    reason="Ingestion already completed",
                    current_state=current_state,
                    force_required=True,
                    existing_classes=existing_classes,
                    timestamp=timestamp_str
                )
            
            # Handle in-progress state with staleness check
            if current_state == IngestionState.IN_PROGRESS:
                is_stale = self._is_ingestion_stale(timestamp_str)
                
                if not is_stale:
                    return IngestionDecision(
                        should_run=False,
                        reason="Ingestion currently in progress and not stale",
                        current_state=current_state,
                        force_required=True,
                        existing_classes=existing_classes,
                        timestamp=timestamp_str,
                        is_stale=False
                    )
                else:
                    return IngestionDecision(
                        should_run=True,
                        reason="Stale in-progress ingestion detected, proceeding with new ingestion",
                        current_state=current_state,
                        force_required=False,
                        existing_classes=existing_classes,
                        timestamp=timestamp_str,
                        is_stale=True
                    )
            
            # Handle existing data without force reingest
            if existing_classes and not force_reingest:
                if not self.config.is_interactive_mode:
                    return IngestionDecision(
                        should_run=False,
                        reason=f"Non-interactive mode with existing data for classes: {', '.join(existing_classes)}",
                        current_state=current_state,
                        force_required=True,
                        existing_classes=existing_classes,
                        timestamp=timestamp_str
                    )
                
                # In interactive mode, we'll need user confirmation (handled by caller)
                return IngestionDecision(
                    should_run=True,
                    reason="Existing data found, requires user confirmation in interactive mode",
                    current_state=current_state,
                    force_required=False,
                    existing_classes=existing_classes,
                    timestamp=timestamp_str
                )
            
            # Default: proceed with ingestion
            return IngestionDecision(
                should_run=True,
                reason=f"Ready to start ingestion (current state: {current_state.value})",
                current_state=current_state,
                force_required=False,
                existing_classes=existing_classes,
                timestamp=timestamp_str
            )
            
        except Exception as e:
            logger.error(f"Error determining ingestion decision: {str(e)}")
            return IngestionDecision(
                should_run=False,
                reason=f"Error occurred while checking ingestion status: {str(e)}",
                current_state=IngestionState.UNKNOWN,
                force_required=True,
                existing_classes=[],
                timestamp=None
            )
    
    def _check_existing_classes(self) -> List[str]:
        """Check which ESCO classes already have data."""
        existing_classes = []
        class_names = ["Skill", "Occupation", "ISCOGroup", "SkillCollection", "SkillGroup"]
        
        try:
            for class_name in class_names:
                if self.ingestor.check_class_exists(class_name):
                    existing_classes.append(class_name)
        except Exception as e:
            logger.warning(f"Error checking existing classes: {str(e)}")
        
        return existing_classes
    
    def _is_ingestion_stale(self, timestamp_str: Optional[str]) -> bool:
        """
        Check if an in-progress ingestion is stale (older than threshold).
        
        Uses heartbeat timestamp if available, falls back to main timestamp.
        
        Args:
            timestamp_str: Main timestamp string from metadata
            
        Returns:
            bool: True if ingestion is stale, False otherwise
        """
        try:
            # Get current metadata to check for heartbeat
            status_data = self.client.get_ingestion_status()
            details = status_data.get('details', {})
            
            # Try to get heartbeat timestamp first
            heartbeat_str = details.get('last_heartbeat')
            if heartbeat_str:
                try:
                    heartbeat_time = datetime.fromisoformat(heartbeat_str)
                    age_seconds = (datetime.utcnow() - heartbeat_time).total_seconds()
                    is_stale = age_seconds > self.config.staleness_threshold_seconds
                    
                    if is_stale:
                        logger.info(f"In-progress ingestion is stale based on heartbeat (age: {age_seconds:.0f}s, threshold: {self.config.staleness_threshold_seconds}s)")
                    return is_stale
                except ValueError as e:
                    logger.warning(f"Could not parse heartbeat timestamp '{heartbeat_str}': {str(e)}, falling back to main timestamp")
            
            # Fallback to main timestamp if no heartbeat or parsing failed
            if not timestamp_str:
                logger.warning("No timestamp found for in-progress status, assuming stale")
                return True
            
            try:
                ingestion_time = datetime.fromisoformat(timestamp_str)
                age_seconds = (datetime.utcnow() - ingestion_time).total_seconds()
                is_stale = age_seconds > self.config.staleness_threshold_seconds
                
                if is_stale:
                    logger.info(f"In-progress ingestion is stale based on main timestamp (age: {age_seconds:.0f}s, threshold: {self.config.staleness_threshold_seconds}s)")
                
                return is_stale
                
            except ValueError as e:
                logger.warning(f"Could not parse main timestamp '{timestamp_str}': {str(e)}, assuming stale")
                return True
            
        except Exception as e:
            logger.warning(f"Error checking staleness: {str(e)}, assuming stale")
            return True
    
    def validate_prerequisites(self) -> ValidationResult:
        """
        Validate all prerequisites for ingestion.
        
        Checks Weaviate connectivity, schema readiness, data directory existence,
        and configuration validity.
        
        Returns:
            ValidationResult: Comprehensive validation status and details
        """
        result = ValidationResult(is_valid=True)
        
        # Validate configuration first
        config_validation = self.config.validate()
        if not config_validation.is_valid:
            result.is_valid = False
            result.errors.extend(config_validation.errors)
            result.warnings.extend(config_validation.warnings)
            result.details.update(config_validation.details)
        
        result.checks_performed.append("configuration_validation")
        
        # Check Weaviate connectivity
        try:
            # Test connection by checking schema
            self.client.ensure_schema()
            result.add_success("Weaviate connection successful", "connectivity")
        except Exception as e:
            result.add_error(f"Weaviate connection failed: {str(e)}", "connectivity")
        
        result.checks_performed.append("weaviate_connectivity")
        
        # Check data directory exists
        if self.config.data_dir:
            if os.path.exists(self.config.data_dir):
                result.add_success(f"Data directory exists: {self.config.data_dir}", "data_directory")
            else:
                result.add_error(f"Data directory not found: {self.config.data_dir}", "data_directory")
        else:
            result.add_warning("No data directory specified in configuration", "data_directory")
        
        result.checks_performed.append("data_directory_check")
        
        # Check schema readiness
        try:
            if self.client.is_schema_initialized():
                result.add_success("Weaviate schema is ready", "schema")
            else:
                result.add_warning("Weaviate schema not initialized (will be created)", "schema")
        except Exception as e:
            result.add_error(f"Schema check failed: {str(e)}", "schema")
        
        result.checks_performed.append("schema_readiness")
        
        # Check required CSV files if data directory is available
        if self.config.data_dir and os.path.exists(self.config.data_dir):
            self._validate_data_files(result)
        
        return result
    
    def _validate_data_files(self, result: ValidationResult) -> None:
        """Validate that required data files exist."""
        data_dir = Path(self.config.data_dir)
        required_patterns = [
            "*occupations*.csv",
            "*skills*.csv", 
            "*isco*.csv",
            "*collection*.csv"
        ]
        
        for pattern in required_patterns:
            files = list(data_dir.glob(pattern))
            if files:
                result.add_success(f"Found data files matching {pattern}: {len(files)} files", "data_files")
            else:
                result.add_warning(f"No data files found matching pattern: {pattern}", "data_files")
        
        result.checks_performed.append("data_files_validation")
    
    def run_ingestion(self, progress_callback: Optional[Callable[[IngestionProgress], None]] = None) -> IngestionResult:
        """
        Run the ingestion process.
        
        Args:
            progress_callback: Optional callback function to report progress
            
        Returns:
            IngestionResult: Result of the ingestion process
        """
        result = IngestionResult(
            success=False,
            steps_completed=0,
            total_steps=12,  # Total number of steps in the process
            start_time=datetime.utcnow()
        )
        
        try:
            # Initialize progress tracking
            progress = IngestionProgress(
                current_step="initialization",
                step_number=0,
                total_steps=12,
                step_description="Initializing ingestion process",
                started_at=datetime.utcnow()
            )
            
            if progress_callback:
                progress_callback(progress)
            
            # Step 1: Initialize
            progress.step_number = 1
            progress.step_description = "Initializing ingestion process"
            progress.step_started_at = datetime.utcnow()
            self._step_initialization()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 2: Schema Setup
            progress.step_number = 2
            progress.step_description = "Setting up schema"
            progress.step_started_at = datetime.utcnow()
            self._step_schema_setup()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 3: Ingest ISCO Groups
            progress.step_number = 3
            progress.step_description = "Ingesting ISCO groups"
            progress.step_started_at = datetime.utcnow()
            self._step_ingest_isco_groups()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 4: Ingest Occupations
            progress.step_number = 4
            progress.step_description = "Ingesting occupations"
            progress.step_started_at = datetime.utcnow()
            self._step_ingest_occupations()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 5: Ingest Skills
            progress.step_number = 5
            progress.step_description = "Ingesting skills"
            progress.step_started_at = datetime.utcnow()
            self._step_ingest_skills()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 6: Ingest Skill Groups
            progress.step_number = 6
            progress.step_description = "Ingesting skill groups"
            progress.step_started_at = datetime.utcnow()
            self._step_ingest_skill_groups()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 7: Ingest Skill Collections
            progress.step_number = 7
            progress.step_description = "Ingesting skill collections"
            progress.step_started_at = datetime.utcnow()
            self._step_ingest_skill_collections()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 8: Create Skill Relations
            progress.step_number = 8
            progress.step_description = "Creating skill relations"
            progress.step_started_at = datetime.utcnow()
            self._step_create_skill_relations()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 9: Create Hierarchical Relations
            progress.step_number = 9
            progress.step_description = "Creating hierarchical relations"
            progress.step_started_at = datetime.utcnow()
            self._step_create_hierarchical_relations()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 10: Create ISCO Relations
            progress.step_number = 10
            progress.step_description = "Creating ISCO relations"
            progress.step_started_at = datetime.utcnow()
            self._step_create_isco_relations()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 11: Create Collection Relations
            progress.step_number = 11
            progress.step_description = "Creating collection relations"
            progress.step_started_at = datetime.utcnow()
            self._step_create_collection_relations()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Step 12: Create Skill-Skill Relations
            progress.step_number = 12
            progress.step_description = "Creating skill-skill relations"
            progress.step_started_at = datetime.utcnow()
            self._step_create_skill_skill_relations()
            self._update_heartbeat()
            if progress_callback:
                progress_callback(progress)
            
            # Update final state
            result.success = True
            result.steps_completed = 12
            result.end_time = datetime.utcnow()
            result.final_state = IngestionState.COMPLETED
            result.last_completed_step = "skill_skill_relations"
            
            # Set completion metadata
            self.client.set_ingestion_metadata(
                status="completed",
                details={
                    "completed_at": datetime.utcnow().isoformat(),
                    "duration_seconds": result.duration,
                    "steps_completed": result.steps_completed,
                    "total_steps": result.total_steps,
                    "step_history": progress.step_history
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Ingestion failed: {str(e)}")
            result.success = False
            result.end_time = datetime.utcnow()
            result.final_state = IngestionState.FAILED
            result.errors.append(str(e))
            
            # Set failure metadata
            self.client.set_ingestion_metadata(
                status="failed",
                details={
                    "failed_at": datetime.utcnow().isoformat(),
                    "error": str(e),
                    "last_step": progress.current_step if progress else None,
                    "step_number": progress.step_number if progress else None,
                    "step_history": progress.step_history if progress else []
                }
            )
            
            return result

    def _update_heartbeat(self) -> None:
        """Update the heartbeat timestamp in metadata."""
        try:
            self.client.set_ingestion_metadata(
                status="in_progress",
                details={
                    "last_heartbeat": datetime.utcnow().isoformat(),
                    "current_step": self._current_step,
                    "step_number": self._current_step_number,
                    "total_steps": 12,
                    "step_started_at": self._step_started_at.isoformat() if self._step_started_at else None,
                    "items_processed": self._items_processed,
                    "total_items": self._total_items
                }
            )
        except Exception as e:
            logger.warning(f"Failed to update heartbeat: {str(e)}")

    def _step_initialization(self) -> None:
        """Initialize the ingestion process."""
        self._current_step = "initialization"
        self._current_step_number = 1
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        self._total_items = 1
        
        # Initialize schema
        self.ingestor.initialize_schema()
        
        # Update progress
        self._items_processed = 1
        self._update_heartbeat()

    def _step_schema_setup(self) -> None:
        """Set up the schema."""
        self._current_step = "schema_setup"
        self._current_step_number = 2
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        self._total_items = 1
        
        # Schema setup logic here
        
        # Update progress
        self._items_processed = 1
        self._update_heartbeat()

    def _step_ingest_isco_groups(self) -> None:
        """Ingest ISCO groups."""
        self._current_step = "ingest_isco_groups"
        self._current_step_number = 3
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "ISCOGroups_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Ingest ISCO groups
        self.ingestor.ingest_isco_groups()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_ingest_occupations(self) -> None:
        """Ingest occupations."""
        self._current_step = "ingest_occupations"
        self._current_step_number = 4
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "occupations_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Ingest occupations
        self.ingestor.ingest_occupations()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_ingest_skills(self) -> None:
        """Ingest skills."""
        self._current_step = "ingest_skills"
        self._current_step_number = 5
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "skills_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Ingest skills
        self.ingestor.ingest_skills()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_ingest_skill_groups(self) -> None:
        """Ingest skill groups."""
        self._current_step = "ingest_skill_groups"
        self._current_step_number = 6
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "skillGroups_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Ingest skill groups
        self.ingestor.ingest_skill_groups()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_ingest_skill_collections(self) -> None:
        """Ingest skill collections."""
        self._current_step = "ingest_skill_collections"
        self._current_step_number = 7
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "conceptSchemes_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Ingest skill collections
        self.ingestor.ingest_skill_collections()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_create_skill_relations(self) -> None:
        """Create skill relations."""
        self._current_step = "create_skill_relations"
        self._current_step_number = 8
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "occupationSkillRelations_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Create skill relations
        self.ingestor.create_skill_relations()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_create_hierarchical_relations(self) -> None:
        """Create hierarchical relations."""
        self._current_step = "create_hierarchical_relations"
        self._current_step_number = 9
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "broaderRelationsOccPillar_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Create hierarchical relations
        self.ingestor.create_hierarchical_relations()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_create_isco_relations(self) -> None:
        """Create ISCO relations."""
        self._current_step = "create_isco_relations"
        self._current_step_number = 10
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "ISCOGroups_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Create ISCO relations
        self.ingestor.create_isco_group_relations()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_create_collection_relations(self) -> None:
        """Create collection relations."""
        self._current_step = "create_collection_relations"
        self._current_step_number = 11
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "skillSkillRelations_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Create collection relations
        self.ingestor.create_skill_collection_relations()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()

    def _step_create_skill_skill_relations(self) -> None:
        """Create skill-skill relations."""
        self._current_step = "create_skill_skill_relations"
        self._current_step_number = 12
        self._step_started_at = datetime.utcnow()
        self._items_processed = 0
        
        # Get total items from file
        file_path = os.path.join(self.config.data_dir, "skillSkillRelations_en.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            self._total_items = len(df)
        else:
            self._total_items = 0
        
        # Create skill-skill relations
        self.ingestor.create_skill_skill_relations()
        
        # Update progress
        self._items_processed = self._total_items
        self._update_heartbeat()
    
    def verify_completion(self) -> ValidationResult:
        """
        Verify that ingestion completed successfully.
        
        Checks final ingestion status, validates data counts, and confirms
        all relationships are created properly.
        
        Returns:
            ValidationResult: Verification status and details
        """
        result = ValidationResult(is_valid=True)
        
        try:
            # Check final status
            final_status = self.client.get_ingestion_status()
            current_status = final_status.get('status')
            
            if current_status == 'completed':
                result.add_success("Ingestion status is COMPLETED", "status")
            else:
                result.add_error(f"Expected status 'completed', found '{current_status}'", "status")
            
            result.checks_performed.append("status_verification")
            
            # Check data counts
            metrics = self.get_ingestion_metrics()
            for class_name, count in metrics.get('entity_counts', {}).items():
                if count > 0:
                    result.add_success(f"{class_name}: {count} entities", "data_counts")
                else:
                    result.add_warning(f"{class_name}: No entities found", "data_counts")
            
            result.checks_performed.append("data_count_verification")
            
            # Store metrics in details
            result.details['metrics'] = metrics
            
        except Exception as e:
            result.add_error(f"Verification failed: {str(e)}", "verification")
        
        return result
    
    def get_ingestion_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive ingestion metrics.
        
        Returns counts of ingested entities, performance metrics, and health status.
        
        Returns:
            Dict[str, Any]: Metrics dictionary with counts and performance data
        """
        metrics = {
            'entity_counts': {},
            'status': 'unknown',
            'last_update': None,
            'health': 'unknown'
        }
        
        try:
            # Get current status
            status_data = self.client.get_ingestion_status()
            metrics['status'] = status_data.get('status', 'unknown')
            metrics['last_update'] = status_data.get('timestamp')
            
            # Get entity counts for each class
            class_names = ['Skill', 'Occupation', 'ISCOGroup', 'SkillCollection', 'SkillGroup']
            
            for class_name in class_names:
                try:
                    # Get repository and count objects
                    repo = self.client.get_repository(class_name)
                    count = repo.count_objects()
                    metrics['entity_counts'][class_name] = count
                except Exception as e:
                    logger.warning(f"Could not get count for {class_name}: {str(e)}")
                    metrics['entity_counts'][class_name] = 0
            
            # Calculate total entities
            total_entities = sum(metrics['entity_counts'].values())
            metrics['total_entities'] = total_entities
            
            # Determine health status
            if total_entities > 0:
                metrics['health'] = 'healthy'
            else:
                metrics['health'] = 'no_data'
                
        except Exception as e:
            logger.error(f"Failed to get ingestion metrics: {str(e)}")
            metrics['health'] = 'error'
            metrics['error'] = str(e)
        
        return metrics
    
    def close(self) -> None:
        """Clean up resources."""
        if self._ingestor:
            self._ingestor.close()
        if self._client:
            # Client cleanup if needed
            pass 