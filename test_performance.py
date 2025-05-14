import pytest
import os
import time
import concurrent.futures
from fastapi.testclient import TestClient
from main import app
from database import TABLE_NAME
import psycopg2

client = TestClient(app)

# Test database connection parameters
TEST_DB_NAME = "database"
TEST_DB_USER = "postgres"
TEST_DB_PASSWORD = "Password"
TEST_DB_HOST = "localhost"
TEST_DB_PORT = "5433"

@pytest.fixture(autouse=True)
def setup_and_teardown():
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

    yield

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

def test_upload_large_csv():
    """Test uploading a large CSV file to validate performance."""
    # Create a large CSV with 1000 rows
    rows = ["name,age,email"]
    for i in range(1000):
        rows.append(f"User{i},{20+i%30},user{i}@example.com")

    csv_content = "\n".join(rows).encode("utf-8")
    files = {"file": ("large_file.csv", csv_content, "text/csv")}

    start_time = time.time()
    response = client.post("/upload/", files=files)
    end_time = time.time()

    assert response.status_code == 200

    # Performance assertion - should complete in reasonable time
    # Adjust the threshold for PostgreSQL which might be slower than SQLite
    assert end_time - start_time < 10.0, f"Upload took too long: {end_time - start_time:.2f} seconds"

    # Verify the data was stored correctly
    response = client.get("/records/")
    records = response.json()["records"]
    assert len(records) == 1000

def test_concurrent_requests():
    """Test handling multiple concurrent requests."""
    # Upload initial data
    csv_content = b"name,age\nJohn,30\nJane,25"
    files = {"file": ("test.csv", csv_content, "text/csv")}
    client.post("/upload/", files=files)

    # Function to make a GET request
    def make_get_request():
        return client.get("/records/")

    # Make 10 concurrent requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(make_get_request) for _ in range(10)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # All requests should succeed
    for response in results:
        assert response.status_code == 200
        records = response.json()["records"]
        assert len(records) == 2

def test_api_response_time():
    """Test API response time for basic operations."""
    # Upload data
    csv_content = b"name,age\nJohn,30\nJane,25"
    files = {"file": ("test.csv", csv_content, "text/csv")}
    client.post("/upload/", files=files)

    # Measure GET response time
    start_time = time.time()
    response = client.get("/records/")
    end_time = time.time()

    assert response.status_code == 200

    # Response time should be reasonable
    response_time = end_time - start_time
    assert response_time < 0.5, f"GET request took too long: {response_time:.2f} seconds"

    # Measure filtered GET response time
    start_time = time.time()
    response = client.get("/records/?column=name&value=John")
    end_time = time.time()

    assert response.status_code == 200

    # Response time should be reasonable
    response_time = end_time - start_time
    assert response_time < 0.5, f"Filtered GET request took too long: {response_time:.2f} seconds"
