import pytest
import os
import asyncio
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from main import app, startup_event
from database import DatabaseError, TABLE_NAME
import psycopg2

TEST_DB_NAME = "database"
TEST_DB_USER = "postgres"
TEST_DB_PASSWORD = "Password"
TEST_DB_HOST = "localhost"
TEST_DB_PORT = "5433"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Setup and teardown for tests."""
    conn = None
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
    except Exception as e:
        pytest.skip(f"Could not connect to test PostgreSQL database: {str(e)}")
    finally:
        if conn:
            conn.close()

    yield

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

@pytest.mark.asyncio
async def test_startup_event():
    """Test that the startup event initializes the database."""
    await startup_event()

    conn = psycopg2.connect(
        host=TEST_DB_HOST,
        port=TEST_DB_PORT,
        dbname=TEST_DB_NAME,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{TABLE_NAME}')")
    table_exists = cursor.fetchone()[0]
    conn.close()

    assert table_exists

@pytest.mark.asyncio
@patch('main.initialize_db')
async def test_startup_event_handles_error(mock_initialize_db):
    """Test that the startup event handles database errors gracefully."""
    mock_initialize_db.side_effect = DatabaseError("Test error")

    await startup_event()

    mock_initialize_db.assert_called_once()

def test_app_startup_with_client():
    """Test that the app startup event is triggered when creating a client."""
    conn = None
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
    finally:
        if conn:
            conn.close()

    with TestClient(app) as client:
        response = client.get("/records/")
        assert response.status_code in (200, 500)

    conn = psycopg2.connect(
        host=TEST_DB_HOST,
        port=TEST_DB_PORT,
        dbname=TEST_DB_NAME,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{TABLE_NAME}')")
    table_exists = cursor.fetchone()[0]
    conn.close()

    assert table_exists

def test_exception_handler():
    """Test the custom exception handler for HTTPException."""
    with TestClient(app) as client:
        response = client.get("/records/?column=name")
        assert response.status_code == 500
        assert "detail" in response.json()
