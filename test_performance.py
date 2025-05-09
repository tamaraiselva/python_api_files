import pytest
import os
import time
import concurrent.futures
from fastapi.testclient import TestClient
from main import app
from database import DATABASE_FILE

client = TestClient(app)
TEST_DATABASE_FILE = "test_perf_database.db"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    original_db_file = DATABASE_FILE
    import database
    database.DATABASE_FILE = TEST_DATABASE_FILE

    yield

    database.DATABASE_FILE = original_db_file
    if os.path.exists(TEST_DATABASE_FILE):
        os.remove(TEST_DATABASE_FILE)

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
    # Adjust the threshold based on your performance requirements
    assert end_time - start_time < 5.0, f"Upload took too long: {end_time - start_time:.2f} seconds"

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
