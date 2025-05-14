from fastapi import FastAPI, UploadFile, File, Query, HTTPException, Depends, Path, Body
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Any, Union
from database import (
    initialize_db, insert_csv_data, fetch_records, DatabaseError,
    get_record_by_id, update_record, delete_record, delete_all_records
)
from utils import process_csv
from pydantic import BaseModel, Field

class RecordUpdate(BaseModel):
    class Config:
        extra = "allow"

app = FastAPI(
    title="CSV Data API",
    description="API for uploading and retrieving CSV data with PostgreSQL",
    version="1.1.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize the database on application startup."""
    try:
        initialize_db()
    except DatabaseError as e:
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
    if not file.filename.endswith(('.csv', '.CSV')):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    try:
        content = await file.read()
        df = process_csv(content)
        insert_csv_data(df)
        return {"message": "CSV uploaded and data stored successfully", "filename": file.filename}
    except HTTPException as e:
        raise e
    except DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
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

@app.get("/records/{record_id}",
        summary="Get record by ID",
        description="Retrieve a specific record by its ID")
async def get_record(
    record_id: int = Path(..., description="The ID of the record to retrieve", gt=0)
) -> Dict[str, Any]:
    """
    Fetch a specific record by ID.

    Args:
        record_id (int): The ID of the record to retrieve.

    Returns:
        Dict[str, Any]: The record data.

    Raises:
        HTTPException: If the record is not found or if fetching fails.
    """
    try:
        record = get_record_by_id(record_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")
        return {"record": record}
    except DatabaseError as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        # Handle validation errors
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.put("/records/{record_id}",
        summary="Update record",
        description="Update a specific record by its ID")
async def update_record_endpoint(
    record_id: int = Path(..., description="The ID of the record to update", gt=0),
    record_data: RecordUpdate = Body(..., description="The updated record data")
) -> Dict[str, Any]:
    """
    Update a specific record by ID.

    Args:
        record_id (int): The ID of the record to update.
        record_data (RecordUpdate): The updated record data.

    Returns:
        Dict[str, Any]: A message indicating success or failure.

    Raises:
        HTTPException: If the record is not found or if updating fails.
    """
    try:
        data = record_data.model_dump()
        success = update_record(record_id, data)

        if not success:
            raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")

        return {"message": f"Record with ID {record_id} updated successfully"}
    except DatabaseError as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        # Handle validation errors
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.delete("/records/{record_id}",
        summary="Delete record",
        description="Delete a specific record by its ID")
async def delete_record_endpoint(
    record_id: int = Path(..., description="The ID of the record to delete", gt=0)
) -> Dict[str, Any]:
    """
    Delete a specific record by ID.

    Args:
        record_id (int): The ID of the record to delete.

    Returns:
        Dict[str, Any]: A message indicating success or failure.

    Raises:
        HTTPException: If the record is not found or if deletion fails.
    """
    try:
        success = delete_record(record_id)

        if not success:
            raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")

        return {"message": f"Record with ID {record_id} deleted successfully"}
    except DatabaseError as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except ValueError as e:
        # Handle validation errors
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.delete("/records/",
        summary="Delete all records",
        description="Delete all records from the database")
async def delete_all_records_endpoint() -> Dict[str, Any]:
    """
    Delete all records from the database.

    Returns:
        Dict[str, Any]: A message indicating success and the number of records deleted.

    Raises:
        HTTPException: If deletion fails.
    """
    try:
        count = delete_all_records()
        return {"message": f"All records deleted successfully", "count": count}
    except DatabaseError as e:
        # Handle database errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
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