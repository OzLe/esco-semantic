class ESCOException(Exception):
    """Base exception for ESCO tool"""
    pass

class ConfigurationError(ESCOException):
    """Configuration related errors"""
    pass

class DatabaseError(ESCOException):
    """Database operation errors"""
    pass

class IngestionError(ESCOException):
    """Data ingestion errors"""
    pass

class SearchError(ESCOException):
    """Search related errors"""
    pass

class ValidationError(ESCOException):
    """Data validation errors"""
    pass

class EmbeddingError(ESCOException):
    """Embedding generation errors"""
    pass

class TranslationError(ESCOException):
    """Translation related errors"""
    pass

class ModelError(ESCOException):
    """Model loading and inference errors"""
    pass 