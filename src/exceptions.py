"""Custom exceptions for the ESCO semantic search application."""

class ESCOError(Exception):
    """Base exception class for ESCO application errors."""
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

class ConfigurationError(ESCOError):
    """Raised when there are issues with configuration."""
    pass

class DataValidationError(ESCOError):
    """Raised when data validation fails."""
    pass

class WeaviateError(ESCOError):
    """Raised when there are issues with Weaviate operations."""
    pass

class TranslationError(ESCOError):
    """Raised when there are issues with translation operations."""
    pass

class IngestionError(ESCOError):
    """Raised when there are issues during data ingestion."""
    pass

class SearchError(ESCOError):
    """Raised when there are issues during search operations."""
    pass

class ModelError(ESCOError):
    """Raised when there are issues with model operations."""
    pass 