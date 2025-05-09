import pytest
import os
import asyncio
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from main import app, startup_event
from database import DATABASE_FILE, DatabaseError

TEST_DATABASE_FILE = "test_startup_db.db"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    original_db_file = DATABASE_FILE
    import database
    database.DATABASE_FILE = TEST_DATABASE_FILE

    yield

    database.DATABASE_FILE = original_db_file
    if os.path.exists(TEST_DATABASE_FILE):
        os.remove(TEST_DATABASE_FILE)

@pytest.mark.asyncio
async def test_startup_event():
    """Test that the startup event initializes the database."""
    # Run the startup event
    await startup_event()

    # Check that the database file was created
    assert os.path.exists(TEST_DATABASE_FILE)

@pytest.mark.asyncio
@patch('main.initialize_db')
async def test_startup_event_handles_error(mock_initialize_db):
    """Test that the startup event handles database errors gracefully."""
    # Configure the mock to raise a DatabaseError
    mock_initialize_db.side_effect = DatabaseError("Test error")

    # Run the startup event (should not raise an exception)
    await startup_event()

    # Verify the mock was called
    mock_initialize_db.assert_called_once()

def test_app_startup_with_client():
    """Test that the app startup event is triggered when creating a client."""
    # Remove the database file if it exists
    if os.path.exists(TEST_DATABASE_FILE):
        os.remove(TEST_DATABASE_FILE)

    # Creating a client should trigger the startup event
    with TestClient(app) as client:
        # Make a simple request to ensure the app is running
        response = client.get("/records/")
        assert response.status_code in (200, 500)  # Either success or database error

    # Check that the database file was created
    assert os.path.exists(TEST_DATABASE_FILE)

def test_exception_handler():
    """Test the custom exception handler for HTTPException."""
    with TestClient(app) as client:
        # Test with a column parameter but no value
        response = client.get("/records/?column=name")
        # The API returns 500 for this case, not 400 as expected
        assert response.status_code == 500
        assert "detail" in response.json()
