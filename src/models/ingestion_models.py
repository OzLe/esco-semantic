"""
Data models for the ESCO ingestion service layer.

This module contains all data classes and enums used to represent
inputs, outputs, and state throughout the ingestion process.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Any, Optional, timedelta
from datetime import datetime


class IngestionState(Enum):
    """Enumeration of possible ingestion states."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class IngestionDecision:
    """
    Contains the decision about whether to run ingestion and the reasoning.
    
    This model encapsulates the business logic decision-making process
    for determining if ingestion should proceed.
    """
    should_run: bool
    reason: str
    current_state: IngestionState
    force_required: bool
    existing_classes: List[str] = field(default_factory=list)
    timestamp: Optional[str] = None
    is_stale: bool = False


@dataclass 
class IngestionProgress:
    """
    Contains current step and progress information during ingestion.
    
    Used to track and report progress throughout the ingestion process.
    """
    current_step: str
    step_number: int
    total_steps: int
    step_description: str = ""
    started_at: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None
    step_started_at: Optional[datetime] = None
    step_estimated_completion: Optional[datetime] = None
    items_processed: int = 0
    total_items: int = 0
    step_duration_seconds: Optional[float] = None
    average_step_duration: Optional[float] = None
    step_history: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress as a percentage."""
        if self.total_steps == 0:
            return 0.0
        return (self.step_number / self.total_steps) * 100.0
    
    @property
    def step_progress_percentage(self) -> float:
        """Calculate current step progress as a percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.items_processed / self.total_items) * 100.0
    
    @property
    def progress_display(self) -> str:
        """Get a formatted progress display string."""
        return f"{self.step_number}/{self.total_steps}"
    
    @property
    def step_progress_display(self) -> str:
        """Get a formatted step progress display string."""
        return f"{self.items_processed}/{self.total_items}"
    
    @property
    def estimated_time_remaining(self) -> Optional[timedelta]:
        """Calculate estimated time remaining based on step history."""
        if not self.step_started_at or not self.average_step_duration:
            return None
        
        remaining_steps = self.total_steps - self.step_number
        if remaining_steps <= 0:
            return timedelta(seconds=0)
        
        # Calculate remaining time based on average step duration
        remaining_seconds = self.average_step_duration * remaining_steps
        
        # Add remaining time in current step if we have progress
        if self.total_items > 0:
            step_progress = self.items_processed / self.total_items
            remaining_seconds += self.average_step_duration * (1 - step_progress)
        
        return timedelta(seconds=remaining_seconds)
    
    def update_step_progress(self, items_processed: int, total_items: int) -> None:
        """Update progress within the current step."""
        self.items_processed = items_processed
        self.total_items = total_items
        
        # Update step ETA if we have timing information
        if self.step_started_at and self.average_step_duration:
            progress = items_processed / total_items if total_items > 0 else 0
            remaining_seconds = self.average_step_duration * (1 - progress)
            self.step_estimated_completion = datetime.utcnow() + timedelta(seconds=remaining_seconds)
    
    def complete_step(self, step_name: str, duration_seconds: float) -> None:
        """Record completion of a step and update timing information."""
        self.step_history.append({
            'step': step_name,
            'duration_seconds': duration_seconds,
            'completed_at': datetime.utcnow()
        })
        
        # Update average step duration
        if self.step_history:
            total_duration = sum(step['duration_seconds'] for step in self.step_history)
            self.average_step_duration = total_duration / len(self.step_history)
        
        # Reset step-specific fields
        self.step_started_at = None
        self.step_estimated_completion = None
        self.items_processed = 0
        self.total_items = 0
        self.step_duration_seconds = None


@dataclass
class IngestionResult:
    """
    Contains success/failure status and detailed results of ingestion.
    
    This is the comprehensive result object returned by the service layer.
    """
    success: bool
    steps_completed: int
    total_steps: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    final_state: Optional[IngestionState] = None
    last_completed_step: str = ""
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate duration in seconds if both start and end times are available."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion as a percentage."""
        if self.total_steps == 0:
            return 0.0
        return (self.steps_completed / self.total_steps) * 100.0


@dataclass
class ValidationResult:
    """
    Contains validation status and details.
    
    Used for pre-flight checks and configuration validation.
    """
    is_valid: bool
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checks_performed: List[str] = field(default_factory=list)
    
    def add_error(self, error: str, component: str = "general") -> None:
        """Add an error and mark validation as invalid."""
        self.errors.append(error)
        self.is_valid = False
        if component not in self.details:
            self.details[component] = {"status": "error", "messages": []}
        self.details[component]["messages"].append(error)
    
    def add_warning(self, warning: str, component: str = "general") -> None:
        """Add a warning without affecting validation status."""
        self.warnings.append(warning)
        if component not in self.details:
            self.details[component] = {"status": "warning", "messages": []}
        self.details[component]["messages"].append(warning)
    
    def add_success(self, message: str, component: str = "general") -> None:
        """Add a success message."""
        if component not in self.details:
            self.details[component] = {"status": "success", "messages": []}
        self.details[component]["messages"].append(message)


@dataclass
class IngestionConfig:
    """
    Contains all configuration parameters for ingestion.
    
    Centralizes all configuration data needed by the service layer.
    """
    config_path: str
    profile: str = "default"
    
    # Ingestion options
    delete_all: bool = False
    embeddings_only: bool = False
    classes: List[str] = field(default_factory=list)
    skip_relations: bool = False
    force_reingest: bool = False
    
    # System settings
    batch_size: int = 100
    data_dir: str = ""
    non_interactive: bool = False
    docker_env: bool = False
    
    # Timeouts and limits
    staleness_threshold_seconds: int = 7200
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 30
    
    # Loaded configuration data
    raw_config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-initialization processing."""
        # Set default classes if none specified
        if not self.classes:
            self.classes = ['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']
        
        # Detect environment settings if not explicitly set
        if not hasattr(self, '_env_detected'):
            import os
            self.docker_env = os.getenv('DOCKER_ENV') == 'true'
            self.non_interactive = (
                not hasattr(__import__('sys'), 'stdin') or 
                not __import__('sys').stdin.isatty() or 
                os.getenv('NON_INTERACTIVE') == 'true'
            )
            self._env_detected = True
    
    @property
    def classes_to_ingest(self) -> List[str]:
        """Get the list of classes to ingest."""
        return self.classes if self.classes else ['Occupation', 'Skill', 'ISCOGroup', 'SkillCollection']
    
    @property
    def is_interactive_mode(self) -> bool:
        """Check if running in interactive mode."""
        return not (self.docker_env or self.non_interactive)
    
    def validate(self) -> ValidationResult:
        """
        Validate the configuration.
        
        Returns:
            ValidationResult: Validation status and details
        """
        result = ValidationResult(is_valid=True)
        result.checks_performed.append("config_validation")
        
        # Check required fields
        if not self.config_path:
            result.add_error("config_path is required", "config")
        
        if not self.profile:
            result.add_error("profile is required", "config")
        
        # Check config file exists
        import os
        if self.config_path and not os.path.exists(self.config_path):
            result.add_error(f"Configuration file not found: {self.config_path}", "config")
        
        # Validate classes
        valid_classes = {'Occupation', 'Skill', 'ISCOGroup', 'SkillCollection'}
        for class_name in self.classes:
            if class_name not in valid_classes:
                result.add_warning(f"Unknown class specified: {class_name}", "classes")
        
        # Validate numeric values
        if self.batch_size <= 0:
            result.add_error("batch_size must be positive", "config")
        
        if self.staleness_threshold_seconds <= 0:
            result.add_error("staleness_threshold_seconds must be positive", "config")
        
        if self.max_retry_attempts < 0:
            result.add_error("max_retry_attempts must be non-negative", "config")
        
        if self.retry_delay_seconds < 0:
            result.add_error("retry_delay_seconds must be non-negative", "config")
        
        # Add success message if all validations passed
        if result.is_valid:
            result.add_success("Configuration validation passed", "config")
        
        return result 