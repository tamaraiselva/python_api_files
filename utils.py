import pandas as pd
import io
from fastapi import HTTPException
from typing import Optional

class CSVProcessingError(Exception):
    pass

def process_csv(file_content: bytes) -> pd.DataFrame:
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
