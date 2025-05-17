from fastapi import FastAPI, UploadFile, File, Query, HTTPException, Path, Body, status
from database import (
    initialize_db, insert_csv_data, fetch_records,
    get_record_by_id, create_record, update_record, delete_record
)
from utils import process_csv
from typing import Dict, Any, Optional

app = FastAPI()

# Initialize DB on startup
initialize_db()

@app.post("/upload/")
async def upload_csv(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = process_csv(content)
        insert_csv_data(df)
        return {"message": "CSV uploaded and data stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/records/")
async def get_records(column: str = Query(None), value: str = Query(None)):
    try:
        records = fetch_records(column, value)
        return {"records": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/records/{record_id}", response_model=Dict[str, Any])
async def get_record(record_id: int = Path(..., title="The ID of the record to get")):
    try:
        record = get_record_by_id(record_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")
        return {"record": record}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records/", status_code=status.HTTP_201_CREATED, response_model=Dict[str, Any])
async def create_new_record(record_data: Dict[str, Any] = Body(..., title="Data for the new record")):
    try:
        new_record = create_record(record_data)
        return {"record": new_record, "message": "Record created successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/records/{record_id}", response_model=Dict[str, Any])
async def update_existing_record(
    record_id: int = Path(..., title="The ID of the record to update"),
    record_data: Dict[str, Any] = Body(..., title="Updated data for the record")
):
    try:
        updated_record = update_record(record_id, record_data)
        if updated_record is None:
            raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")
        return {"record": updated_record, "message": "Record updated successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/records/{record_id}", response_model=Dict[str, str])
async def delete_existing_record(record_id: int = Path(..., title="The ID of the record to delete")):
    try:
        success = delete_record(record_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")
        return {"message": f"Record with ID {record_id} deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
