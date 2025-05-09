import pytest
import os
from fastapi.testclient import TestClient
from main import app
from database import DATABASE_FILE

TEST_DATABASE_FILE = "test_database.db"

@pytest.fixture
def test_client():
    """Return a TestClient for the FastAPI app."""
    return TestClient(app)

@pytest.fixture
def test_csv_content():
    """Return sample CSV content for testing."""
    return b"name,age\nJohn,30\nJane,25"

@pytest.fixture
def test_csv_file(test_csv_content):
    """Return a file dict for testing file uploads."""
    return {"file": ("test.csv", test_csv_content, "text/csv")}

@pytest.fixture
def large_csv_content():
    """Return large CSV content for performance testing."""
    rows = ["name,age,email"]
    for i in range(1000):
        rows.append(f"User{i},{20+i%30},user{i}@example.com")
    return "\n".join(rows).encode("utf-8")

@pytest.fixture
def large_csv_file(large_csv_content):
    """Return a file dict with large CSV content."""
    return {"file": ("large_file.csv", large_csv_content, "text/csv")}

@pytest.fixture
def invalid_csv_content():
    """Return invalid CSV content for testing error handling."""
    return b"This is not a CSV file"

@pytest.fixture
def empty_csv_content():
    """Return empty CSV content for testing error handling."""
    return b"name,age"

@pytest.fixture(scope="function")
def test_database():
    """
    Setup and teardown for tests.
    - Redirects database operations to a test database
    - Cleans up the test database after tests
    """
    original_db_file = DATABASE_FILE
    import database
    database.DATABASE_FILE = TEST_DATABASE_FILE

    yield TEST_DATABASE_FILE

    database.DATABASE_FILE = original_db_file
    if os.path.exists(TEST_DATABASE_FILE):
        os.remove(TEST_DATABASE_FILE)
