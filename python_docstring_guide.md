# Python Docstring Guide - Google Style Standards

## Overview

This guide defines the documentation standards for the ESCO Data Management and Search Tool project. We use Google-style docstrings for consistency, readability, and compatibility with documentation generation tools.

## General Principles

1. **Clarity**: Write clear, concise descriptions that explain what the code does
2. **Completeness**: Document all public functions, classes, and methods
3. **Consistency**: Follow the same format throughout the codebase
4. **Examples**: Provide examples for complex functions or unclear usage
5. **Type Information**: Include type hints in function signatures, not docstrings

## Module-Level Docstrings

Every Python module should start with a module-level docstring describing its purpose.

```python
"""
Service layer for ESCO data ingestion operations.

This module provides a unified interface for all ingestion-related business logic,
eliminating duplication between CLI and container initialization approaches.
The service layer handles state management, validation, and orchestration
of the complete ingestion pipeline.
"""
```

## Class Docstrings

Class docstrings should describe the class purpose, key attributes, and usage patterns.

```python
class IngestionService:
    """
    Service layer for ESCO data ingestion operations.
    
    This class provides a unified interface for all ingestion-related business logic,
    eliminating duplication between CLI and container initialization approaches.
    It manages the complete lifecycle of ESCO data ingestion including validation,
    progress tracking, and error handling.
    
    Attributes:
        config: Configuration object containing all necessary parameters.
        client: WeaviateClient instance for database operations.
        ingestor: WeaviateIngestor instance for data processing.
    
    Example:
        >>> config = IngestionConfig("config.yaml", "default")
        >>> service = IngestionService(config)
        >>> result = service.run_ingestion()
        >>> print(f"Success: {result.success}")
    """
```

## Function/Method Docstrings

### Standard Function Format

```python
def validate_prerequisites(self) -> ValidationResult:
    """
    Validate all prerequisites for ingestion.
    
    Checks Weaviate connectivity, schema readiness, data directory existence,
    and configuration validity. This method should be called before starting
    any ingestion operations to ensure the system is ready.
    
    Returns:
        ValidationResult: Comprehensive validation status and details.
            Contains is_valid boolean, error messages, warnings, and
            detailed information about each validation check performed.
    
    Raises:
        WeaviateError: If Weaviate connection cannot be established.
        ConfigurationError: If configuration is invalid or missing.
    
    Example:
        >>> validation = service.validate_prerequisites()
        >>> if validation.is_valid:
        ...     print("Prerequisites passed")
        >>> else:
        ...     print(f"Validation failed: {validation.errors}")
    """
```

### Function with Multiple Parameters

```python
def run_ingestion(
    self, 
    progress_callback: Optional[Callable[[IngestionProgress], None]] = None
) -> IngestionResult:
    """
    Run the complete ingestion process.
    
    Executes all steps of the ESCO data ingestion pipeline including
    initialization, entity ingestion, relationship creation, and validation.
    Provides real-time progress updates through the optional callback.
    
    Args:
        progress_callback: Optional callback function to receive progress updates.
            Called with IngestionProgress objects containing current step information,
            completion percentage, and timing estimates. If None, no progress
            updates will be provided.
    
    Returns:
        IngestionResult: Result of the ingestion process containing success status,
            number of steps completed, any errors or warnings encountered, and
            performance metrics including total duration.
    
    Raises:
        ValidationError: If prerequisites validation fails.
        WeaviateError: If database operations fail.
        DataError: If source data is corrupted or missing.
    
    Example:
        >>> def progress_handler(progress):
        ...     print(f"Step {progress.step_number}: {progress.current_step}")
        >>> 
        >>> result = service.run_ingestion(progress_callback=progress_handler)
        >>> if result.success:
        ...     print(f"Completed in {result.duration:.1f} seconds")
    """
```

### Property Docstrings

```python
@property
def client(self) -> WeaviateClient:
    """
    Get or create WeaviateClient instance.
    
    Returns:
        WeaviateClient: Configured client instance for database operations.
            Uses lazy initialization to create the client only when needed.
    """
```

### Static and Class Methods

```python
@staticmethod
def create_from_config(config_path: str, profile: str) -> 'IngestionService':
    """
    Create an IngestionService instance from configuration file.
    
    Factory method that loads configuration from a YAML file and creates
    a properly configured IngestionService instance.
    
    Args:
        config_path: Path to the YAML configuration file.
        profile: Configuration profile name to use (e.g., 'default', 'dev', 'prod').
    
    Returns:
        IngestionService: Configured service instance ready for use.
    
    Raises:
        FileNotFoundError: If configuration file doesn't exist.
        ConfigurationError: If profile is not found or configuration is invalid.
    
    Example:
        >>> service = IngestionService.create_from_config(
        ...     "config/weaviate_config.yaml", 
        ...     "production"
        ... )
    """
```

## Data Class Docstrings

Data classes should document their purpose and key fields:

```python
@dataclass
class IngestionResult:
    """
    Contains success/failure status and detailed results of ingestion.
    
    This is the comprehensive result object returned by the service layer
    after completing an ingestion operation. It includes timing information,
    error details, and metrics about the ingestion process.
    
    Attributes:
        success: Whether the ingestion completed successfully.
        steps_completed: Number of ingestion steps that completed.
        total_steps: Total number of steps in the ingestion process.
        errors: List of error messages encountered during ingestion.
        warnings: List of warning messages that didn't stop ingestion.
        metrics: Dictionary containing performance and count metrics.
        start_time: When the ingestion process started.
        end_time: When the ingestion process completed.
        final_state: Final state of the ingestion system.
        last_completed_step: Name of the last step that completed successfully.
    """
    success: bool
    steps_completed: int
    total_steps: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    # ... other fields
```

## Exception Docstrings

Custom exceptions should document when they're raised and what they mean:

```python
class IngestionError(ESCOError):
    """
    Raised when there are issues during data ingestion.
    
    This exception is raised when the ingestion process encounters errors
    that prevent successful completion. It includes details about the
    specific failure and context information for debugging.
    
    Attributes:
        message: Human-readable error description.
        details: Dictionary with additional context about the error.
    """
```

## Repository Pattern Docstrings

Repository classes should document their data access patterns:

```python
class SkillRepository(WeaviateRepository):
    """
    Repository for Skill entities in Weaviate.
    
    Provides data access methods for ESCO skill entities including CRUD operations,
    relationship management, and specialized skill queries. Handles the mapping
    between Python objects and Weaviate's vector database format.
    
    This repository manages skills, their hierarchical relationships (broader/narrower),
    associations with occupations (essential/optional), and membership in skill
    collections and groups.
    """

    def add_skill_to_skill_relation(
        self, 
        from_skill_uri: str, 
        to_skill_uri: str, 
        relation_type: str
    ) -> bool:
        """
        Add a related skill reference between two skills.
        
        Creates a bidirectional relationship between skills using the hasRelatedSkill
        property. This method handles both directions of the relationship to ensure
        graph traversal works correctly in both directions.
        
        Args:
            from_skill_uri: URI of the source skill in the relationship.
            to_skill_uri: URI of the target skill in the relationship.
            relation_type: Type of relationship (used for logging and debugging).
        
        Returns:
            bool: True if the relationship was created successfully, False otherwise.
                Returns False if either skill is not found or if the database
                operation fails.
        
        Example:
            >>> repo = SkillRepository(client)
            >>> success = repo.add_skill_to_skill_relation(
            ...     "http://data.europa.eu/esco/skill/1",
            ...     "http://data.europa.eu/esco/skill/2",
            ...     "related"
            ... )
        """
```

## Complex Algorithm Docstrings

For complex algorithms or business logic:

```python
def _is_ingestion_stale(self, timestamp_str: Optional[str]) -> bool:
    """
    Check if an in-progress ingestion is stale (older than threshold).
    
    Uses a two-tier approach to determine staleness:
    1. First checks for heartbeat timestamp in metadata details
    2. Falls back to main timestamp if heartbeat is unavailable
    
    An ingestion is considered stale if the most recent timestamp is older
    than the configured staleness threshold. This helps detect and recover
    from interrupted ingestion processes that failed to clean up properly.
    
    Args:
        timestamp_str: Main timestamp string from ingestion metadata.
            Should be in ISO format (e.g., "2024-01-15T10:30:00Z").
    
    Returns:
        bool: True if ingestion is stale and should be restarted, False if
            the ingestion is still active and should not be interrupted.
            Returns True if timestamp parsing fails (fail-safe behavior).
    
    Note:
        The staleness threshold is configured via config.staleness_threshold_seconds
        and defaults to 7200 seconds (2 hours). Heartbeat timestamps take
        precedence over main timestamps for more accurate staleness detection.
    
    Example:
        >>> # Ingestion from 3 hours ago
        >>> old_timestamp = "2024-01-15T07:00:00Z"
        >>> is_stale = service._is_ingestion_stale(old_timestamp)
        >>> print(is_stale)  # True (older than 2 hour threshold)
    """
```

## Common Sections Reference

### Args Section
- Use `Args:` for function parameters
- Format: `parameter_name: Description of the parameter.`
- Include type information only if not in function signature
- Explain constraints, defaults, and special values

### Returns Section
- Use `Returns:` for return values
- Format: `Type: Description of what is returned.`
- Explain the structure of complex return types
- Document special return values (None, empty lists, etc.)

### Raises Section
- Use `Raises:` for exceptions
- Format: `ExceptionType: Description of when this is raised.`
- Document only exceptions that callers should handle
- Include both custom and standard library exceptions

### Example Section
- Use `Example:` for usage examples
- Provide realistic, runnable code
- Show common use cases
- Include expected output when helpful

### Note Section
- Use `Note:` for important implementation details
- Explain performance considerations
- Document side effects
- Clarify non-obvious behavior

## Best Practices

### Do's
✅ Write docstrings for all public methods and classes
✅ Use consistent terminology throughout the project
✅ Include type hints in function signatures
✅ Provide examples for complex functionality
✅ Update docstrings when code changes
✅ Use proper grammar and spelling
✅ Keep descriptions concise but complete

### Don'ts
❌ Don't repeat information from type hints in docstrings
❌ Don't document private methods unless complex
❌ Don't use abbreviations without explanation
❌ Don't include implementation details in docstrings
❌ Don't write docstrings for trivial getters/setters
❌ Don't use outdated or incorrect examples

## Tools and Validation

### Documentation Generation
This project supports automatic documentation generation using:
- **Sphinx**: For comprehensive documentation websites
- **pdoc**: For quick API documentation
- **mkdocs**: For markdown-based documentation

### Linting
Docstring quality is enforced through:
- **pydocstyle**: Checks docstring conventions
- **pylint**: Includes docstring checks
- **mypy**: Validates type annotations

### IDE Integration
Most IDEs support Google-style docstrings:
- **VS Code**: Python extension recognizes the format
- **PyCharm**: Built-in support for Google-style docstrings
- **Vim/Neovim**: Available through plugins

## Project-Specific Conventions

### Domain-Specific Terms
When documenting ESCO-specific concepts, use these consistent terms:
- **ESCO URI**: Use "URI" not "URL" for ESCO identifiers
- **Concept**: Prefer "concept" over "entity" for ESCO items
- **Taxonomy**: Use when referring to the overall ESCO structure
- **Semantic Search**: Prefer over "vector search" in user-facing docs
- **Ingestion**: Use consistently for data loading process

### Service Layer Conventions
- Document business logic purpose, not implementation details
- Explain state management and validation aspects
- Include error recovery and retry behavior
- Document progress tracking and monitoring features

### Repository Pattern Conventions
- Focus on data access patterns and relationships
- Document transaction boundaries and consistency guarantees
- Explain caching and performance characteristics
- Include examples of complex queries