import pytest
import os
import pandas as pd
import psycopg2
from unittest.mock import patch, MagicMock
from database import (
    initialize_db, insert_csv_data, fetch_records,
    TABLE_NAME, DatabaseError, get_db_connection
)

# Test database connection parameters
TEST_DB_NAME = "database"
TEST_DB_USER = "postgres"
TEST_DB_PASSWORD = "Password"
TEST_DB_HOST = "localhost"
TEST_DB_PORT = "5433"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    # Save original connection function
    original_get_connection = get_db_connection

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

def test_initialize_db():
    initialize_db()

    # Verify the table was created
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

def test_insert_and_fetch_records():
    initialize_db()
    data = {
        'name': ['John', 'Jane'],
        'age': [30, 25]
    }
    df = pd.DataFrame(data)

    insert_csv_data(df)

    records = fetch_records()

    assert len(records) == 2
    assert records[0]['name'] == 'John'
    assert records[0]['age'] == '30'  # Age is stored as a string in PostgreSQL
    assert records[1]['name'] == 'Jane'
    assert records[1]['age'] == '25'  # Age is stored as a string in PostgreSQL

def test_fetch_records_with_filter():
    initialize_db()
    data = {
        'name': ['John', 'Jane'],
        'age': [30, 25]
    }
    df = pd.DataFrame(data)

    insert_csv_data(df)

    records = fetch_records('name', 'John')

    assert len(records) == 1
    assert records[0]['name'] == 'John'
    assert records[0]['age'] == '30'  # Age is stored as a string in PostgreSQL

def test_insert_empty_dataframe():
    initialize_db()

    df = pd.DataFrame()
    with pytest.raises(ValueError) as excinfo:
        insert_csv_data(df)

    assert "Cannot insert empty DataFrame" in str(excinfo.value)

def test_fetch_records_invalid_column():
    initialize_db()
    data = {
        'name': ['John', 'Jane'],
        'age': [30, 25]
    }
    df = pd.DataFrame(data)
    insert_csv_data(df)

    # Fetch records with invalid column should raise ValueError
    with pytest.raises(ValueError) as excinfo:
        fetch_records('invalid_column', 'John')

    assert "Invalid column name" in str(excinfo.value)

def test_database_connection_error(monkeypatch):
    """Test database connection error handling."""
    # Mock psycopg2.connect to raise an error
    def mock_connect(*args, **kwargs):
        raise psycopg2.Error("Mock connection error")

    # Apply the monkeypatch
    monkeypatch.setattr(psycopg2, "connect", mock_connect)

    # Initialize the database should raise DatabaseError
    with pytest.raises(DatabaseError) as excinfo:
        initialize_db()

    assert "Failed to initialize database" in str(excinfo.value)
