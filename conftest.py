import pytest
import os
from fastapi.testclient import TestClient
from main import app
import psycopg2
from database import TABLE_NAME

# Test database connection parameters
TEST_DB_NAME = "database"
TEST_DB_USER = "postgres"
TEST_DB_PASSWORD = "Password"
TEST_DB_HOST = "localhost"
TEST_DB_PORT = "5433"

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
    - Sets up a clean test database
    - Cleans up after tests
    """
    # Setup test database
    conn = None
    try:
        # Connect to the test database
        conn = psycopg2.connect(
            host=TEST_DB_HOST,
            port=TEST_DB_PORT,
            dbname=TEST_DB_NAME,
            user=TEST_DB_USER,
            password=TEST_DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Drop the test table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.commit()
    except Exception as e:
        pytest.skip(f"Could not connect to test PostgreSQL database: {str(e)}")
    finally:
        if conn:
            conn.close()

    # Return connection parameters for tests
    yield {
        "host": TEST_DB_HOST,
        "port": TEST_DB_PORT,
        "dbname": TEST_DB_NAME,
        "user": TEST_DB_USER,
        "password": TEST_DB_PASSWORD
    }

    # Cleanup after tests
    try:
        conn = psycopg2.connect(
            host=TEST_DB_HOST,
            port=TEST_DB_PORT,
            dbname=TEST_DB_NAME,
            user=TEST_DB_USER,
            password=TEST_DB_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn.commit()
    except Exception:
        pass
    finally:
        if conn:
            conn.close()
