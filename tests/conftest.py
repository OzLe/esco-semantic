import pytest
import os
from pathlib import Path
from esco.database.weaviate.client import WeaviateClient
from esco.embeddings.generator import EmbeddingGenerator

@pytest.fixture(scope="session")
def test_env():
    """Set up test environment variables."""
    os.environ["WEAVIATE_URL"] = "http://localhost:8080"
    os.environ["WEAVIATE_API_KEY"] = "test-key"
    os.environ["OPENAI_API_KEY"] = "test-key"
    return os.environ

@pytest.fixture(scope="session")
def db_client(test_env):
    """Create a test database client."""
    return WeaviateClient(
        url=test_env["WEAVIATE_URL"],
        api_key=test_env["WEAVIATE_API_KEY"]
    )

@pytest.fixture(scope="session")
def embedding_generator(test_env):
    """Create a test embedding generator."""
    return EmbeddingGenerator(
        api_key=test_env["OPENAI_API_KEY"]
    )

@pytest.fixture(scope="session")
def test_data_path():
    """Get the path to test data directory."""
    return Path(__file__).parent / "fixtures" / "data"

@pytest.fixture(autouse=True)
def setup_teardown():
    """Setup and teardown for each test."""
    # Setup
    yield
    # Teardown
    # Add any cleanup code here if needed 