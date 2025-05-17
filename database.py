import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from io import StringIO

# PostgreSQL connection parameters
# Default to localhost and port 5432 (standard PostgreSQL port)
# These can be overridden by environment variables
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "Testing")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "Password")

TABLE_NAME = "uploaded_data"

def get_db_connection():
    """Establish a database connection to PostgreSQL."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

def initialize_db():
    """Initialize the database and create a table if not exists."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    # Using SERIAL for auto-incrementing primary key in PostgreSQL
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()

def insert_csv_data(df):
    """Insert CSV data into the PostgreSQL table."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # First, drop the table if it exists and recreate it with the new schema
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    # Create the table with columns from the DataFrame
    columns = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        pg_type = "TEXT"  # Default type
        if "int" in str(dtype):
            pg_type = "INTEGER"
        elif "float" in str(dtype):
            pg_type = "FLOAT"
        columns.append(f"{col_name} {pg_type}")

    # Add id column as primary key
    create_table_query = f"""
        CREATE TABLE {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            {', '.join(columns)}
        )
    """
    cursor.execute(create_table_query)

    # Insert data
    if not df.empty:
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        # Use COPY command for efficient bulk insert
        columns_str = ', '.join(df.columns)
        cursor.copy_expert(f"COPY {TABLE_NAME} ({columns_str}) FROM STDIN WITH CSV", csv_buffer)

    conn.commit()
    cursor.close()
    conn.close()

def fetch_records(column=None, value=None):
    """Fetch records from the database, with optional filtering."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)  # Returns results as dictionaries

    if column and value:
        query = f"SELECT * FROM {TABLE_NAME} WHERE {column} = %s"
        cursor.execute(query, (value,))
    else:
        query = f"SELECT * FROM {TABLE_NAME}"
        cursor.execute(query)

    records = cursor.fetchall()
    cursor.close()
    conn.close()

    # Convert to list of dictionaries
    return [dict(record) for record in records]

def get_record_by_id(record_id):
    """Fetch a single record by ID."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = f"SELECT * FROM {TABLE_NAME} WHERE id = %s"
    cursor.execute(query, (record_id,))
    record = cursor.fetchone()

    cursor.close()
    conn.close()

    if record:
        return dict(record)
    return None

def create_record(record_data):
    """Create a new record in the database."""
    if not record_data or not isinstance(record_data, dict):
        raise ValueError("Record data must be a non-empty dictionary")

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Extract column names and values
    columns = list(record_data.keys())
    values = list(record_data.values())

    # Create placeholders for the SQL query
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)

    # Insert the record
    query = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders}) RETURNING *"
    cursor.execute(query, values)

    # Get the inserted record
    new_record = cursor.fetchone()

    conn.commit()
    cursor.close()
    conn.close()

    return dict(new_record) if new_record else None

def update_record(record_id, record_data):
    """Update a record by ID."""
    if not record_data or not isinstance(record_data, dict):
        raise ValueError("Record data must be a non-empty dictionary")

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Check if the record exists
    cursor.execute(f"SELECT id FROM {TABLE_NAME} WHERE id = %s", (record_id,))
    if cursor.fetchone() is None:
        cursor.close()
        conn.close()
        return None

    # Prepare the SET clause for the UPDATE statement
    set_clause = ', '.join([f"{key} = %s" for key in record_data.keys()])
    values = list(record_data.values())
    values.append(record_id)  # Add the ID for the WHERE clause

    # Update the record
    query = f"UPDATE {TABLE_NAME} SET {set_clause} WHERE id = %s RETURNING *"
    cursor.execute(query, values)

    # Get the updated record
    updated_record = cursor.fetchone()

    conn.commit()
    cursor.close()
    conn.close()

    return dict(updated_record) if updated_record else None

def delete_record(record_id):
    """Delete a record by ID."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Check if the record exists
    cursor.execute(f"SELECT id FROM {TABLE_NAME} WHERE id = %s", (record_id,))
    if cursor.fetchone() is None:
        cursor.close()
        conn.close()
        return False

    # Delete the record
    query = f"DELETE FROM {TABLE_NAME} WHERE id = %s"
    cursor.execute(query, (record_id,))

    conn.commit()
    cursor.close()
    conn.close()

    return True
