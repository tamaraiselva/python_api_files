# FastAPI CSV Data API

A FastAPI application for uploading and retrieving CSV data.

## Features

- Upload CSV files and store data in PostgreSQL or SQLite database
- Retrieve records with optional filtering
- Comprehensive error handling
- Containerized with Docker
- Automated testing with pytest
- Kubernetes deployment support

## Installation

### Database Options

This application supports two database options:

1. **PostgreSQL** (running in Kubernetes) - The default option
2. **SQLite** (local file-based database) - Simpler alternative that doesn't require Kubernetes

### Option 1: PostgreSQL Setup (Requires Kubernetes)

This option uses PostgreSQL running in Kubernetes. Follow these steps to set up PostgreSQL:

1. Make sure Docker Desktop is running

2. Make sure Minikube is running:

   ```bash
   minikube start
   minikube status
   ```

3. Deploy PostgreSQL using Helm:

   ```bash
   helm repo add bitnami https://charts.bitnami.com/bitnami
   helm repo update
   helm install postgresql bitnami/postgresql -f postgres-values.yaml
   ```

4. Set up port forwarding to access PostgreSQL from your local machine:

   ```bash
   kubectl port-forward svc/postgresql 5433:5432
   ```

   This will make PostgreSQL accessible at `localhost:5433`.

5. Run the application with PostgreSQL:

   ```bash
   python main.py
   ```

### Option 2: SQLite Setup (No Kubernetes Required)

This option uses SQLite, which is a file-based database that doesn't require any additional setup:

1. Run the SQLite version of the application:

   ```bash
   python temp_sqlite_db.py
   ```

   This will create a local SQLite database file (`csv_data.db`) in the current directory.

### Local Development (Common Steps)

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd python_api_files
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Choose either the PostgreSQL or SQLite option above.

   The API will be available at `http://localhost:8000` in both cases.

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

### Get All Records

```bash
GET /records/
```

Retrieve all records from the database with optional filtering.

Parameters:

- `column` (optional): Column name to filter by
- `value` (optional): Value to filter for

Example:

```bash
curl -X 'GET' 'http://127.0.0.1:8000/records/'
curl -X 'GET' 'http://127.0.0.1:8000/records/?column=name&value=Yegna Subramanian Jambunath'
```

### Get Record by ID

```bash
GET /records/{record_id}
```

Retrieve a specific record by its ID.

Example:

```bash
curl -X 'GET' 'http://127.0.0.1:8000/records/1'
```

### Update Record

```bash
PUT /records/{record_id}
```

Update a specific record by its ID.

Example:

```bash
curl -X 'PUT' 'http://127.0.0.1:8000/records/1' \
  -H 'Content-Type: application/json' \
  -d '{"name": "Updated Name", "age": 35}'
```

### Delete Record

```bash
DELETE /records/{record_id}
```

Delete a specific record by its ID.

Example:

```bash
curl -X 'DELETE' 'http://127.0.0.1:8000/records/1'
```

### Delete All Records

```bash
DELETE /records/
```

Delete all records from the database.

Example:

```bash
curl -X 'DELETE' 'http://127.0.0.1:8000/records/'
```

## Testing

### Testing with PostgreSQL

The default tests are configured to use PostgreSQL. Make sure PostgreSQL is running in Kubernetes and port-forwarded to localhost:5433 before running the tests.

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

### Testing with SQLite

The SQLite version doesn't have dedicated tests yet. You can manually test it by:

1. Running the application: `python temp_sqlite_db.py`
2. Using curl or a browser to interact with the API endpoints

## Troubleshooting

### PostgreSQL Connection Issues

If you see errors like:

```text
Database error: Failed to connect to database: connection to server at "localhost" (::1), port 5433 failed: Connection refused
```

Check the following:

1. Make sure Docker Desktop is running
2. Make sure Minikube is running (`minikube status`, `minikube start` if needed)
3. Make sure PostgreSQL is deployed in Kubernetes (`kubectl get pods`)
4. Make sure port forwarding is active (`kubectl port-forward svc/postgresql 5433:5432`)

If you continue to have issues, consider using the SQLite option instead.

## Working Demo Drive Link

https://drive.google.com/drive/folders/1AYOyvsuXU4CCDKBw4zWrgEyVgl1oR82k?usp=sharing
