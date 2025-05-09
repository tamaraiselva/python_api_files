import pytest
import os
import pandas as pd
import sqlite3
from database import (
    initialize_db, insert_csv_data, fetch_records,
    DATABASE_FILE, TABLE_NAME, DatabaseError
)

TEST_DATABASE_FILE = "test_database.db"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    original_db_file = DATABASE_FILE
    import database
    database.DATABASE_FILE = TEST_DATABASE_FILE

    yield
    database.DATABASE_FILE = original_db_file
    if os.path.exists(TEST_DATABASE_FILE):
        os.remove(TEST_DATABASE_FILE)

def test_initialize_db():
    initialize_db()
    assert os.path.exists(TEST_DATABASE_FILE)
    conn = sqlite3.connect(TEST_DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
    table_exists = cursor.fetchone() is not None
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
    assert records[0]['age'] == 30
    assert records[1]['name'] == 'Jane'
    assert records[1]['age'] == 25

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
    assert records[0]['age'] == 30

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
    # Mock sqlite3.connect to raise an error
    def mock_connect(*args, **kwargs):
        raise sqlite3.Error("Mock connection error")

    # Apply the monkeypatch
    monkeypatch.setattr(sqlite3, "connect", mock_connect)

    # Initialize the database should raise DatabaseError
    with pytest.raises(DatabaseError) as excinfo:
        initialize_db()

    assert "Failed to initialize database" in str(excinfo.value)
