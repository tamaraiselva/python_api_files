import pytest
import pandas as pd
from fastapi import HTTPException
from utils import process_csv

def test_process_csv_valid():
    valid_csv = b"name,age\nJohn,30\nJane,25"
    result = process_csv(valid_csv)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["name", "age"]
    assert len(result) == 2
    assert result.iloc[0]["name"] == "John"
    assert result.iloc[0]["age"] == 30

def test_process_csv_empty():
    empty_csv = b"name,age"
    with pytest.raises(HTTPException) as excinfo:
        process_csv(empty_csv)
    assert excinfo.value.status_code == 400
    assert "CSV file is empty" in excinfo.value.detail

def test_process_csv_invalid():
    invalid_csv = b"This is not a CSV file"
    with pytest.raises(HTTPException) as excinfo:
        process_csv(invalid_csv)
    assert excinfo.value.status_code == 400
    assert "Invalid CSV format" in excinfo.value.detail

def test_process_csv_empty_content():
    with pytest.raises(HTTPException) as excinfo:
        process_csv(b"")
    assert excinfo.value.status_code == 400
    assert "Empty file content" in excinfo.value.detail

def test_process_csv_encoding():
    special_chars_csv = b"name,description\nJohn,Caf\xe9"

    result = process_csv(special_chars_csv)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["name", "description"]
    assert len(result) == 1
    assert result.iloc[0]["name"] == "John"
    assert "Café" in result.iloc[0]["description"]

def test_process_csv_no_columns():
    no_columns_csv = b"\nJohn,30"

    with pytest.raises(HTTPException) as excinfo:
        process_csv(no_columns_csv)

    assert excinfo.value.status_code == 400
