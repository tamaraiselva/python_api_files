# FastAPI CSV Data API

A FastAPI application for uploading and retrieving CSV data.

## Features

- Upload CSV files and store data in SQLite database
- Retrieve records with optional filtering
- Comprehensive error handling
- Containerized with Docker
- Automated testing with pytest

## Installation

### Local Development

1. Clone the repository:

   ```bash
   git clone https://github.com/tamaraiselva/python_api_files
   cd python_api_files
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the application:

   ```bash
   python main.py
   ```

   The API will be available at `http://localhost:8000`

### Using Docker

1. Build the Docker image:

   ```bash
   docker build -t fastapi_app .
   ```

2. Run the container:

   ```bash
   docker run -p 8000:8000 fastapi_app
   ```

## API Endpoints

### Upload CSV

```bash
POST /upload/
```

Upload a CSV file to store its data in the database.

Example:

```bash
curl -X 'POST' 'http://127.0.0.1:8000/upload/' -F 'file=@your_file.csv'
```

### Get Records

```bash
GET /records/
```

Retrieve records from the database with optional filtering.

Parameters:

- `column` (optional): Column name to filter by
- `value` (optional): Value to filter for

Example:

```bash
curl -X 'GET' 'http://127.0.0.1:8000/records/'
curl -X 'GET' 'http://127.0.0.1:8000/records/?column=name&value=John'
```

## Testing

Run the tests with:

```bash
python run_tests.py
```

This will:

1. Run all tests with coverage reporting
2. Build the Docker image if tests pass

To run tests manually:

```bash
pytest -v
```
