# Unit Tests for Python API

This document provides instructions on how to run the unit tests for the Python API project.

## Prerequisites

Make sure you have all the required dependencies installed:

```bash
pip install -r requirements.txt
```

## Running the Tests

### Running All Tests

To run all tests:

```bash
pytest
```

### Running Specific Test Files

To run tests for database.py:

```bash
pytest test_database.py
```

To run tests for main.py:

```bash
pytest test_main.py
```

### Running with Verbose Output

For more detailed output:

```bash
pytest -v
```

## Test Coverage

To generate a test coverage report, install pytest-cov:

```bash
pip install pytest-cov
```

Then run:

```bash
pytest --cov=.
```

## Test Structure

- `test_database.py`: Tests for database.py functions
  - Tests for database connection
  - Tests for database initialization
  - Tests for data insertion
  - Tests for record retrieval

- `test_main.py`: Tests for main.py FastAPI endpoints
  - Tests for CSV upload endpoint
  - Tests for record retrieval endpoint
  - Tests for error handling

## Notes

- The tests use mock objects to simulate PostgreSQL connections and avoid affecting any real database.
- No actual PostgreSQL connection is established during tests.
