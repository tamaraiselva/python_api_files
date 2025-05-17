import pandas as pd
import io
from fastapi import HTTPException

def process_csv(file_content: bytes):
    """Reads and processes CSV content."""
    try:
        df = pd.read_csv(io.StringIO(file_content.decode("utf-8")))
        if df.empty:
            raise HTTPException(status_code=400, detail="CSV file is empty.")
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV format: {str(e)}")
