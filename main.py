from fastapi import FastAPI, UploadFile, File, Query, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Any, Union
from database import initialize_db, insert_csv_data, fetch_records, DatabaseError
from utils import process_csv

app = FastAPI(
    title="CSV Data API",
    description="API for uploading and retrieving CSV data",
    version="1.0.0"
)

# Initialize DB on startup
@app.on_event("startup")
async def startup_event():
    """Initialize the database on application startup."""
    try:
        initialize_db()
    except DatabaseError as e:
        # Log the error but allow the application to start
        print(f"Database initialization error: {str(e)}")

@app.post("/upload/",
         summary="Upload CSV file",
         description="Upload a CSV file to store its data in the database")
async def upload_csv(file: UploadFile = File(...)) -> Dict[str, str]:
    """
    Upload and process a CSV file.

    Args:
        file (UploadFile): The CSV file to upload.

    Returns:
        Dict[str, str]: A message indicating success.

    Raises:
        HTTPException: If file validation or processing fails.
    """
    # Validate file type
    if not file.filename.endswith(('.csv', '.CSV')):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    try:
        content = await file.read()
        df = process_csv(content)
        insert_csv_data(df)
        return {"message": "CSV uploaded and data stored successfully", "filename": file.filename}
    except HTTPException as e:
        # Re-raise HTTP exceptions from process_csv
        raise e
    except DatabaseError as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        # Handle validation errors
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/records/",
        summary="Get records",
        description="Retrieve records from the database with optional filtering")
async def get_records(
    column: Optional[str] = Query(None, description="Column name to filter by"),
    value: Optional[str] = Query(None, description="Value to filter for")
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch records from the database.

    Args:
        column (Optional[str]): Column name to filter by.
        value (Optional[str]): Value to filter for.

    Returns:
        Dict[str, List[Dict[str, Any]]]: A dictionary containing the records.

    Raises:
        HTTPException: If fetching records fails.
    """
    try:
        # Validate that if column is provided, value must also be provided
        if column is not None and value is None:
            raise HTTPException(status_code=400, detail="If column is provided, value must also be provided.")

        records = fetch_records(column, value)
        return {"records": records}
    except DatabaseError as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        # Handle validation errors
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom exception handler for HTTPException."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)