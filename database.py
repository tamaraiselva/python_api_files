import sqlite3
import os
from typing import List, Dict, Optional, Any
import pandas as pd

DATABASE_FILE = "database.db"
TABLE_NAME = "uploaded_data"

class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass

def get_db_connection() -> sqlite3.Connection:
    """
    Establish a database connection.

    Returns:
        sqlite3.Connection: A connection to the SQLite database.

    Raises:
        DatabaseError: If connection to the database fails.
    """
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        raise DatabaseError(f"Failed to connect to database: {str(e)}")

def initialize_db() -> None:
    """
    Initialize the database and create a table if not exists.

    Raises:
        DatabaseError: If database initialization fails.
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    except (sqlite3.Error, DatabaseError) as e:
        raise DatabaseError(f"Failed to initialize database: {str(e)}")
    finally:
        if conn:
            conn.close()

def insert_csv_data(df: pd.DataFrame) -> None:
    """
    Insert CSV data into the SQLite table.

    Args:
        df (pd.DataFrame): DataFrame containing the data to insert.

    Raises:
        DatabaseError: If data insertion fails.
        ValueError: If the DataFrame is empty or invalid.
    """
    if df is None or df.empty:
        raise ValueError("Cannot insert empty DataFrame")

    conn = None
    try:
        conn = get_db_connection()
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    except (sqlite3.Error, DatabaseError) as e:
        raise DatabaseError(f"Failed to insert data: {str(e)}")
    except Exception as e:
        raise ValueError(f"Invalid DataFrame: {str(e)}")
    finally:
        if conn:
            conn.close()

def fetch_records(column: Optional[str] = None, value: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch records from the database, with optional filtering.

    Args:
        column (Optional[str]): Column name to filter by.
        value (Optional[str]): Value to filter for.

    Returns:
        List[Dict[str, Any]]: List of records as dictionaries.

    Raises:
        DatabaseError: If fetching records fails.
        ValueError: If the column name is invalid.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get table column names for validation
        if column:
            cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
            columns = [info[1] for info in cursor.fetchall()]
            if column not in columns:
                raise ValueError(f"Invalid column name: {column}")

        if column and value:
            query = f"SELECT * FROM {TABLE_NAME} WHERE {column} = ?"
            cursor.execute(query, (value,))
        else:
            query = f"SELECT * FROM {TABLE_NAME}"
            cursor.execute(query)

        records = [dict(row) for row in cursor.fetchall()]
        return records
    except (sqlite3.Error, DatabaseError) as e:
        raise DatabaseError(f"Failed to fetch records: {str(e)}")
    except Exception as e:
        raise ValueError(f"Invalid query parameters: {str(e)}")
    finally:
        if conn:
            conn.close()
