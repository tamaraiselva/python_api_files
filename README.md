# Python API Project

A FastAPI-based REST API for uploading and querying CSV data with PostgreSQL storage.

## Overview

This project provides a full CRUD (Create, Read, Update, Delete) API for:

- Uploading CSV files
- Storing the data in a PostgreSQL database running in Kubernetes (minikube)
- Creating, reading, updating, and deleting individual records
- Retrieving records with optional filtering

## Project Structure

```bash
.
├── database.py         # Database connection and operations
├── main.py             # FastAPI application and endpoints
├── utils.py            # Utility functions for processing data
├── test_database.py    # Unit tests for database.py
├── test_main.py        # Unit tests for main.py
├── requirements.txt    # Project dependencies
├── Dockerfile          # Docker configuration
└── README.md           # Project documentation
```

## Installation

### Prerequisites

- Python 3.9 or higher
- pip (Python package installer)
- PostgreSQL database (local or remote)

### Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd fastapi_app
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure PostgreSQL connection:

   Set the following environment variables to connect to your PostgreSQL database:

   ```bash
   # Windows
   set PG_HOST=localhost
   set PG_PORT=5432
   set PG_DATABASE=postgres
   set PG_USER=postgres
   set PG_PASSWORD=postgres

   # Linux/Mac
   export PG_HOST=localhost
   export PG_PORT=5432
   export PG_DATABASE=postgres
   export PG_USER=postgres
   export PG_PASSWORD=postgres
   ```

   Or you can use the default values in the code (localhost:5432, database: postgres, user: postgres, password: postgres).

## Running the API

Start the API server:

```bash
python main.py
```

The API will be available at `http://localhost:8000`.

### Using Docker

You can also run the application using Docker:

```bash
# Build the Docker image
docker build -t fastapi_app .

# Run the container with PostgreSQL environment variables
docker run -p 8000:8000 \
  -e PG_HOST=host.docker.internal \
  -e PG_PORT=5432 \
  -e PG_DATABASE=postgres \
  -e PG_USER=postgres \
  -e PG_PASSWORD=postgres \
  fastapi_app
```

Note: `host.docker.internal` is used to connect to the PostgreSQL server running on your host machine from inside the Docker container. If you're using a remote PostgreSQL server, replace it with the appropriate hostname or IP address.

### Using with Kubernetes (minikube)

To connect to a PostgreSQL database running in Kubernetes:

1. Start your minikube cluster:

   ```bash
   minikube start
   ```

2. Deploy PostgreSQL to your cluster (if not already deployed).

3. Expose the PostgreSQL service using port-forward:

   ```bash
   kubectl port-forward service/postgres 5432:5432
   ```

4. Run the application with the appropriate environment variables:

   ```bash
   # Windows PowerShell
   $env:PG_HOST = "localhost"
   $env:PG_PORT = "5432"
   $env:PG_DATABASE = "postgres"
   $env:PG_USER = "postgres"
   $env:PG_PASSWORD = "your_password"
   python main.py
   ```

## API Endpoints

### Upload CSV

Upload a CSV file to store its data in the database.

- **URL**: `/upload/`
- **Method**: `POST`
- **Content-Type**: `multipart/form-data`
- **Parameter**: `file` (CSV file)

**Example Request**:

```bash
curl -X POST -F "file=@data.csv" http://localhost:8000/upload/
```

**Example Response**:

```json
{
  "message": "CSV uploaded and data stored successfully"
}
```

### Get All Records

Retrieve records from the database with optional filtering.

- **URL**: `/records/`
- **Method**: `GET`
- **Query Parameters**:
  - `column` (optional): Column name to filter by
  - `value` (optional): Value to filter for

**Example Request (all records)**:

```bash
curl http://localhost:8000/records/
```

**Example Request (filtered records)**:

```bash
curl http://localhost:8000/records/?column=name&value=John
```

**Example Response**:

```json
{
  "records": [
    {
      "name": "John",
      "age": 30,
      "city": "New York"
    },
    ...
  ]
}
```

### Get Record by ID

Retrieve a single record by its ID.

- **URL**: `/records/{id}`
- **Method**: `GET`
- **URL Parameters**:
  - `id`: The ID of the record to retrieve

**Example Request**:

```bash
curl http://localhost:8000/records/1
```

**Example Response**:

```json
{
  "record": {
    "id": 1,
    "name": "John",
    "age": 30,
    "city": "New York"
  }
}
```

### Create Record

Create a new record in the database.

- **URL**: `/records/`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Body**: JSON object with record data

**Example Request**:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"name": "Alice", "age": 28, "city": "Seattle"}' http://localhost:8000/records/
```

**Example Response**:

```json
{
  "record": {
    "id": 4,
    "name": "Alice",
    "age": 28,
    "city": "Seattle"
  },
  "message": "Record created successfully"
}
```

### Update Record

Update an existing record by its ID.

- **URL**: `/records/{id}`
- **Method**: `PUT`
- **Content-Type**: `application/json`
- **URL Parameters**:
  - `id`: The ID of the record to update
- **Body**: JSON object with updated record data

**Example Request**:

```bash
curl -X PUT -H "Content-Type: application/json" -d '{"name": "John", "age": 31, "city": "Boston"}' http://localhost:8000/records/1
```

**Example Response**:

```json
{
  "record": {
    "id": 1,
    "name": "John",
    "age": 31,
    "city": "Boston"
  },
  "message": "Record updated successfully"
}
```

### Delete Record

Delete a record by its ID.

- **URL**: `/records/{id}`
- **Method**: `DELETE`
- **URL Parameters**:
  - `id`: The ID of the record to delete

**Example Request**:

```bash
curl -X DELETE http://localhost:8000/records/1
```

**Example Response**:

```json
{
  "message": "Record with ID 1 deleted successfully"
}
```

## Testing

### Running Tests

Run all tests:

```bash
pytest
```

Run tests with verbose output:

```bash
pytest -v
```

Run specific test files:

```bash
pytest test_database.py
pytest test_main.py
```

### Test Coverage

Generate a test coverage report:

```bash
pytest --cov=.
```

Current coverage:

- database.py: 100%
- main.py: 83%
- Overall: 93%

## Project Components

### database.py

Handles all database operations:

- Establishing connections to PostgreSQL
- Initializing the database schema
- Inserting CSV data
- Full CRUD operations:
  - Creating individual records
  - Reading records (by ID or with filters)
  - Updating records
  - Deleting records

### main.py

Contains the FastAPI application and defines the API endpoints:

- `/upload/` for uploading CSV files
- `/records/` for retrieving all records
- `/records/{id}` for retrieving, updating, or deleting a specific record
- `/records/` (POST) for creating a new record

### utils.py

Provides utility functions:

- Processing CSV files
- Error handling

## Development

### Adding New Features

1. Implement the feature
2. Add appropriate tests
3. Ensure all tests pass
4. Update documentation

### Modifying the Database Schema

If you need to modify the database schema:

1. Update the `initialize_db()` function in database.py
2. Update the corresponding tests in test_database.py

### Connecting to PostgreSQL

The application uses environment variables to configure the PostgreSQL connection:

- `PG_HOST`: PostgreSQL server hostname (default: localhost)
- `PG_PORT`: PostgreSQL server port (default: 5432)
- `PG_DATABASE`: PostgreSQL database name (default: postgres)
- `PG_USER`: PostgreSQL username (default: postgres)
- `PG_PASSWORD`: PostgreSQL password (default: postgres)

You can set these environment variables before running the application, or use the default values.
