import os
import sqlite3
from typing import List, Dict, Optional, Any, Union
import pandas as pd
from sqlalchemy import create_engine

# Database configuration
TABLE_NAME = "uploaded_data"

# SQLite configuration
SQLITE_DB_FILE = "csv_data.db"
USE_SQLITE = True  # Set to True to use SQLite, False to use PostgreSQL

# PostgreSQL configuration (only used if USE_SQLITE is False)
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

# PostgreSQL connection parameters (only used if USE_SQLITE is False)
DB_HOST = "localhost"  # When running outside Kubernetes
DB_PORT = "5433"       # Port forwarded from Kubernetes
DB_NAME = "database"
DB_USER = "postgres"
DB_PASSWORD = "Password"

# For Kubernetes internal connection (used when running inside the cluster)
K8S_DB_HOST = "postgresql.default.svc.cluster.local"
K8S_DB_PORT = "5432"

# Determine if running inside Kubernetes by checking for the service host env var
IN_KUBERNETES = os.environ.get('KUBERNETES_SERVICE_HOST') is not None

class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass

def get_connection_string() -> str:
    """
    Get the appropriate connection string based on the environment.

    Returns:
        str: Database connection string (SQLite or PostgreSQL)
    """
    if USE_SQLITE:
        return f"sqlite:///{SQLITE_DB_FILE}"
    elif IN_KUBERNETES:
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{K8S_DB_HOST}:{K8S_DB_PORT}/{DB_NAME}"
    else:
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_connection():
    """
    Establish a database connection.

    Returns:
        Union[sqlite3.Connection, psycopg2.connection]: A connection to the database.

    Raises:
        DatabaseError: If connection to the database fails.
    """
    try:
        if USE_SQLITE:
            # Use SQLite
            conn = sqlite3.connect(SQLITE_DB_FILE)
            conn.row_factory = sqlite3.Row
            return conn
        else:
            # Use PostgreSQL
            if not PSYCOPG2_AVAILABLE:
                raise ImportError("psycopg2 is not installed. Install it with 'pip install psycopg2-binary'")

            # Determine connection parameters based on environment
            if IN_KUBERNETES:
                host = K8S_DB_HOST
                port = K8S_DB_PORT
            else:
                host = DB_HOST
                port = DB_PORT

            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                cursor_factory=RealDictCursor
            )
            return conn
    except sqlite3.Error as e:
        raise DatabaseError(f"Failed to connect to SQLite database: {str(e)}")
    except (psycopg2.Error, ImportError) as e:
        raise DatabaseError(f"Failed to connect to PostgreSQL database: {str(e)}")
    except Exception as e:
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
        cursor = conn.cursor()

        if USE_SQLITE:
            # SQLite version
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id INTEGER PRIMARY KEY)")
        else:
            # PostgreSQL version
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id SERIAL PRIMARY KEY)")

        conn.commit()
    except (sqlite3.Error, DatabaseError) as e:
        raise DatabaseError(f"Failed to initialize database: {str(e)}")
    except Exception as e:
        raise DatabaseError(f"Failed to initialize database: {str(e)}")
    finally:
        if conn:
            conn.close()

def insert_csv_data(df: pd.DataFrame) -> None:
    """
    Insert CSV data into the database table.

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
        cursor = conn.cursor()

        # First, ensure the table exists with the correct schema
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

        if USE_SQLITE:
            # SQLite version
            # Create the table with an ID column
            create_table_query = f"CREATE TABLE {TABLE_NAME} (id INTEGER PRIMARY KEY"

            # Add columns for each column in the DataFrame
            for column in df.columns:
                create_table_query += f", {column} TEXT"

            create_table_query += ")"
            cursor.execute(create_table_query)

            # Insert data
            for _, row in df.iterrows():
                columns = ", ".join(row.index)
                placeholders = ", ".join(["?"] * len(row))
                insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, list(row))
        else:
            # PostgreSQL version
            # Create the table with an ID column
            create_table_query = f"CREATE TABLE {TABLE_NAME} (id SERIAL PRIMARY KEY"

            # Add columns for each column in the DataFrame
            for column in df.columns:
                create_table_query += f", {column} TEXT"

            create_table_query += ")"
            cursor.execute(create_table_query)

            # Insert data
            for _, row in df.iterrows():
                columns = ", ".join(row.index)
                placeholders = ", ".join(["%s"] * len(row))
                insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, list(row))

        conn.commit()
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to insert data: {str(e)}")
    except (psycopg2.Error, DatabaseError) as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to insert data: {str(e)}")
    except Exception as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to insert data: {str(e)}")
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

        if USE_SQLITE:
            # SQLite version
            # Check if table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
            if not cursor.fetchone():
                return []

            # Get table column names for validation
            if column:
                cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
                columns = [col[1] for col in cursor.fetchall()]
                if column not in columns:
                    raise ValueError(f"Invalid column name: {column}")

            if column and value:
                query = f"SELECT * FROM {TABLE_NAME} WHERE {column} = ?"
                cursor.execute(query, (value,))
            else:
                query = f"SELECT * FROM {TABLE_NAME}"
                cursor.execute(query)

            records = cursor.fetchall()
            return [dict(record) for record in records]
        else:
            # PostgreSQL version
            # Get table column names for validation
            if column:
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TABLE_NAME}'")
                columns = [col['column_name'] for col in cursor.fetchall()]
                if column not in columns:
                    raise ValueError(f"Invalid column name: {column}")

            if column and value:
                query = f"SELECT * FROM {TABLE_NAME} WHERE {column} = %s"
                cursor.execute(query, (value,))
            else:
                query = f"SELECT * FROM {TABLE_NAME}"
                cursor.execute(query)

            records = cursor.fetchall()
            return [dict(record) for record in records]
    except sqlite3.Error as e:
        raise DatabaseError(f"Failed to fetch records: {str(e)}")
    except (psycopg2.Error, DatabaseError) as e:
        raise DatabaseError(f"Failed to fetch records: {str(e)}")
    except Exception as e:
        raise ValueError(f"Invalid query parameters: {str(e)}")
    finally:
        if conn:
            conn.close()

def get_record_by_id(record_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a specific record by ID.

    Args:
        record_id (int): The ID of the record to retrieve.

    Returns:
        Optional[Dict[str, Any]]: The record as a dictionary, or None if not found.

    Raises:
        DatabaseError: If fetching the record fails.
        ValueError: If the record ID is invalid.
    """
    if not isinstance(record_id, int) or record_id <= 0:
        raise ValueError("Record ID must be a positive integer")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if USE_SQLITE:
            # SQLite version
            # First check if the table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
            if not cursor.fetchone():
                return None

            query = f"SELECT * FROM {TABLE_NAME} WHERE id = ?"
            cursor.execute(query, (record_id,))
            record = cursor.fetchone()
            return dict(record) if record else None
        else:
            # PostgreSQL version
            # First check if the table exists
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{TABLE_NAME}')")
            table_exists = cursor.fetchone()['exists']
            if not table_exists:
                return None

            query = f"SELECT * FROM {TABLE_NAME} WHERE id = %s"
            cursor.execute(query, (record_id,))
            record = cursor.fetchone()
            return dict(record) if record else None
    except sqlite3.Error as e:
        raise DatabaseError(f"Failed to fetch record: {str(e)}")
    except (psycopg2.Error, DatabaseError) as e:
        raise DatabaseError(f"Failed to fetch record: {str(e)}")
    except Exception as e:
        if 'record' in locals() and record is None:
            # This is a "not found" case, not an error
            return None
        raise ValueError(f"Invalid record ID: {str(e)}")
    finally:
        if conn:
            conn.close()

def update_record(record_id: int, data: Dict[str, Any]) -> bool:
    """
    Update a specific record by ID.

    Args:
        record_id (int): The ID of the record to update.
        data (Dict[str, Any]): The new data for the record.

    Returns:
        bool: True if the record was updated, False if the record was not found.

    Raises:
        DatabaseError: If updating the record fails.
        ValueError: If the record ID or data is invalid.
    """
    if not isinstance(record_id, int) or record_id <= 0:
        raise ValueError("Record ID must be a positive integer")

    if not data or not isinstance(data, dict):
        raise ValueError("Data must be a non-empty dictionary")

    # Remove id from data if present (can't update the primary key)
    if 'id' in data:
        del data['id']

    if not data:
        raise ValueError("Data must contain at least one field to update")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if USE_SQLITE:
            # SQLite version
            # Check if record exists
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {TABLE_NAME} WHERE id = ?)", (record_id,))
            exists = cursor.fetchone()[0]
            if not exists:
                return False

            # Get table column names for validation
            cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
            valid_columns = [col[1] for col in cursor.fetchall()]

            # Filter out invalid columns
            valid_data = {k: v for k, v in data.items() if k in valid_columns}

            if not valid_data:
                raise ValueError("No valid columns to update")

            # Build the SET clause for the UPDATE statement
            set_clause = ", ".join([f"{key} = ?" for key in valid_data.keys()])
            values = list(valid_data.values())
            values.append(record_id)  # Add record_id for the WHERE clause

            # Execute the UPDATE statement
            query = f"UPDATE {TABLE_NAME} SET {set_clause} WHERE id = ?"
            cursor.execute(query, values)
            conn.commit()

            return cursor.rowcount > 0
        else:
            # PostgreSQL version
            # Check if record exists
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {TABLE_NAME} WHERE id = %s)", (record_id,))
            exists = cursor.fetchone()['exists']
            if not exists:
                return False

            # Get table column names for validation
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TABLE_NAME}'")
            valid_columns = [col['column_name'] for col in cursor.fetchall()]

            # Filter out invalid columns
            valid_data = {k: v for k, v in data.items() if k in valid_columns}

            if not valid_data:
                raise ValueError("No valid columns to update")

            # Build the SET clause for the UPDATE statement
            set_clause = ", ".join([f"{key} = %s" for key in valid_data.keys()])
            values = list(valid_data.values())
            values.append(record_id)  # Add record_id for the WHERE clause

            # Execute the UPDATE statement
            query = f"UPDATE {TABLE_NAME} SET {set_clause} WHERE id = %s"
            cursor.execute(query, values)
            conn.commit()

            return cursor.rowcount > 0
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to update record: {str(e)}")
    except (psycopg2.Error, DatabaseError) as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to update record: {str(e)}")
    except Exception as e:
        if conn:
            conn.rollback()
        raise ValueError(f"Invalid update parameters: {str(e)}")
    finally:
        if conn:
            conn.close()

def delete_record(record_id: int) -> bool:
    """
    Delete a specific record by ID.

    Args:
        record_id (int): The ID of the record to delete.

    Returns:
        bool: True if the record was deleted, False if the record was not found.

    Raises:
        DatabaseError: If deleting the record fails.
        ValueError: If the record ID is invalid.
    """
    if not isinstance(record_id, int) or record_id <= 0:
        raise ValueError("Record ID must be a positive integer")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if USE_SQLITE:
            # SQLite version
            query = f"DELETE FROM {TABLE_NAME} WHERE id = ?"
            cursor.execute(query, (record_id,))
        else:
            # PostgreSQL version
            query = f"DELETE FROM {TABLE_NAME} WHERE id = %s"
            cursor.execute(query, (record_id,))

        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to delete record: {str(e)}")
    except (psycopg2.Error, DatabaseError) as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to delete record: {str(e)}")
    except Exception as e:
        if conn:
            conn.rollback()
        raise ValueError(f"Invalid record ID: {str(e)}")
    finally:
        if conn:
            conn.close()

def delete_all_records() -> int:
    """
    Delete all records from the table.

    Returns:
        int: The number of records deleted.

    Raises:
        DatabaseError: If deleting the records fails.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"DELETE FROM {TABLE_NAME}"
        cursor.execute(query)
        conn.commit()
        return cursor.rowcount
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to delete records: {str(e)}")
    except (psycopg2.Error, DatabaseError) as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to delete records: {str(e)}")
    except Exception as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Failed to delete records: {str(e)}")
    finally:
        if conn:
            conn.close()
