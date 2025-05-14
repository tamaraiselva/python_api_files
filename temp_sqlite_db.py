from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import sqlite3
import io
import uvicorn

app = FastAPI(
    title="CSV Data API (SQLite Version)",
    description="API for uploading and retrieving CSV data with SQLite",
    version="1.0.0"
)

# SQLite database file
DB_FILE = "csv_data.db"
TABLE_NAME = "uploaded_data"

def initialize_db():
    """Initialize the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

def insert_csv_data(df):
    """Insert CSV data into the SQLite table."""
    if df is None or df.empty:
        raise ValueError("Cannot insert empty DataFrame")

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

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

    conn.commit()
    conn.close()

def fetch_records(column=None, value=None):
    """Fetch records from the database, with optional filtering."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    if column and value:
        query = f"SELECT * FROM {TABLE_NAME} WHERE {column} = ?"
        cursor.execute(query, (value,))
    else:
        query = f"SELECT * FROM {TABLE_NAME}"
        cursor.execute(query)

    records = cursor.fetchall()
    result = [dict(record) for record in records]
    conn.close()
    return result

def process_csv(file_content):
    """Process a CSV file and return a DataFrame."""
    if not file_content:
        raise HTTPException(status_code=400, detail="Empty file content.")

    try:
        # Try to decode as UTF-8
        csv_text = file_content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            # Fallback to Latin-1 encoding
            csv_text = file_content.decode("latin-1")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"File encoding not supported: {str(e)}")

    try:
        df = pd.read_csv(io.StringIO(csv_text))

        # Validate DataFrame
        if df.empty:
            raise HTTPException(status_code=400, detail="CSV file is empty.")

        # Check for minimum required columns (can be customized)
        if len(df.columns) < 1:
            raise HTTPException(status_code=400, detail="CSV must contain at least one column.")

        return df
    except pd.errors.ParserError as e:
        raise HTTPException(status_code=400, detail=f"CSV parsing error: {str(e)}")
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize the database on application startup."""
    try:
        initialize_db()
    except Exception as e:
        print(f"Database initialization error: {str(e)}")

@app.post("/upload/")
async def upload_csv(file: UploadFile = File(...)):
    """Upload and process a CSV file."""
    # Validate file type
    if not file.filename.endswith(('.csv', '.CSV')):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    try:
        content = await file.read()
        df = process_csv(content)
        insert_csv_data(df)
        return {"message": "CSV uploaded and data stored successfully", "filename": file.filename}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/records/")
async def get_records(
    column: str = Query(None, description="Column name to filter by"),
    value: str = Query(None, description="Value to filter for")
):
    """Fetch records from the database."""
    try:
        # Validate that if column is provided, value must also be provided
        if column is not None and value is None:
            raise HTTPException(status_code=400, detail="If column is provided, value must also be provided.")

        records = fetch_records(column, value)
        return {"records": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
