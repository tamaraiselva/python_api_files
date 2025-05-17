import unittest
import pandas as pd
import json
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from io import BytesIO

# Import the app
from main import app

# Create a test client
client = TestClient(app)

class TestMain(unittest.TestCase):

    def setUp(self):
        """Set up test environment before each test."""
        # Mock the database connection and initialization
        self.db_patcher = patch('database.get_db_connection')
        self.mock_get_conn = self.db_patcher.start()

        # Create a mock connection and cursor
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_get_conn.return_value = self.mock_conn

        # Mock initialize_db to avoid actual database operations
        self.init_db_patcher = patch('database.initialize_db')
        self.mock_init_db = self.init_db_patcher.start()

        # Create a sample CSV content for testing
        self.sample_csv_content = "name,age,city\nJohn,30,New York\nJane,25,Boston\nBob,40,Chicago"

    def tearDown(self):
        """Clean up after each test."""
        self.db_patcher.stop()
        self.init_db_patcher.stop()

    @patch('main.process_csv')
    @patch('main.insert_csv_data')
    def test_upload_csv(self, mock_insert_csv_data, mock_process_csv):
        """Test the /upload/ endpoint."""
        # Mock the process_csv function to return a DataFrame
        sample_df = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'age': [30, 25, 40],
            'city': ['New York', 'Boston', 'Chicago']
        })
        mock_process_csv.return_value = sample_df

        # Create a test file
        file = BytesIO(self.sample_csv_content.encode())

        # Make the request
        response = client.post(
            "/upload/",
            files={"file": ("test.csv", file, "text/csv")}
        )

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "CSV uploaded and data stored successfully"})

        # Verify that process_csv and insert_csv_data were called
        mock_process_csv.assert_called_once()
        mock_insert_csv_data.assert_called_once_with(sample_df)

    @patch('main.fetch_records')
    def test_get_records_filtered(self, mock_fetch_records):
        """Test the /records/ endpoint with filters."""
        # Mock the fetch_records function to return filtered records
        sample_records = [
            {'name': 'Jane', 'age': 25, 'city': 'Boston'}
        ]
        mock_fetch_records.return_value = sample_records

        # Make the request
        response = client.get("/records/?column=name&value=Jane")

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"records": sample_records})

        # Verify that fetch_records was called with the correct filters
        mock_fetch_records.assert_called_once_with("name", "Jane")

    @patch('main.process_csv')
    def test_upload_csv_error(self, mock_process_csv):
        """Test the /upload/ endpoint with an error."""
        # Mock the process_csv function to raise an exception
        mock_process_csv.side_effect = Exception("Test error")

        # Create a test file
        file = BytesIO(self.sample_csv_content.encode())

        # Make the request
        response = client.post(
            "/upload/",
            files={"file": ("test.csv", file, "text/csv")}
        )

        # Check the response
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json(), {"detail": "Test error"})

    @patch('main.get_record_by_id')
    def test_get_record_by_id(self, mock_get_record_by_id):
        """Test the /records/{record_id} endpoint."""
        # Mock the get_record_by_id function to return a record
        mock_record = {'id': 1, 'name': 'John', 'age': 30, 'city': 'New York'}
        mock_get_record_by_id.return_value = mock_record

        # Make the request
        response = client.get("/records/1")

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"record": mock_record})

        # Verify that get_record_by_id was called with the correct ID
        mock_get_record_by_id.assert_called_once_with(1)

    @patch('main.get_record_by_id')
    def test_get_record_by_id_not_found(self, mock_get_record_by_id):
        """Test the /records/{record_id} endpoint when record is not found."""
        # Mock the get_record_by_id function to return None
        mock_get_record_by_id.return_value = None

        # Make the request
        response = client.get("/records/999")

        # Check the response
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Record with ID 999 not found"})

        # Verify that get_record_by_id was called with the correct ID
        mock_get_record_by_id.assert_called_once_with(999)

    @patch('main.create_record')
    def test_create_record(self, mock_create_record):
        """Test the POST /records/ endpoint."""
        # Mock the create_record function to return a new record
        new_record = {'id': 1, 'name': 'John', 'age': 30, 'city': 'New York'}
        mock_create_record.return_value = new_record

        # Create record data
        record_data = {'name': 'John', 'age': 30, 'city': 'New York'}

        # Make the request
        response = client.post(
            "/records/",
            json=record_data
        )

        # Check the response
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json(), {
            "record": new_record,
            "message": "Record created successfully"
        })

        # Verify that create_record was called with the correct data
        mock_create_record.assert_called_once_with(record_data)

    @patch('main.create_record')
    def test_create_record_error(self, mock_create_record):
        """Test the POST /records/ endpoint with an error."""
        # Mock the create_record function to raise an exception
        mock_create_record.side_effect = ValueError("Invalid record data")

        # Create invalid record data
        record_data = {}

        # Make the request
        response = client.post(
            "/records/",
            json=record_data
        )

        # Check the response
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"detail": "Invalid record data"})

    @patch('main.update_record')
    def test_update_record(self, mock_update_record):
        """Test the PUT /records/{record_id} endpoint."""
        # Mock the update_record function to return an updated record
        updated_record = {'id': 1, 'name': 'John Updated', 'age': 31, 'city': 'Boston'}
        mock_update_record.return_value = updated_record

        # Create update data
        update_data = {'name': 'John Updated', 'age': 31, 'city': 'Boston'}

        # Make the request
        response = client.put(
            "/records/1",
            json=update_data
        )

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "record": updated_record,
            "message": "Record updated successfully"
        })

        # Verify that update_record was called with the correct ID and data
        mock_update_record.assert_called_once_with(1, update_data)

    @patch('main.update_record')
    def test_update_record_not_found(self, mock_update_record):
        """Test the PUT /records/{record_id} endpoint when record is not found."""
        # Mock the update_record function to return None
        mock_update_record.return_value = None

        # Create update data
        update_data = {'name': 'John Updated', 'age': 31, 'city': 'Boston'}

        # Make the request
        response = client.put(
            "/records/999",
            json=update_data
        )

        # Check the response
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Record with ID 999 not found"})

        # Verify that update_record was called with the correct ID and data
        mock_update_record.assert_called_once_with(999, update_data)

    @patch('main.delete_record')
    def test_delete_record(self, mock_delete_record):
        """Test the DELETE /records/{record_id} endpoint."""
        # Mock the delete_record function to return True
        mock_delete_record.return_value = True

        # Make the request
        response = client.delete("/records/1")

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "Record with ID 1 deleted successfully"})

        # Verify that delete_record was called with the correct ID
        mock_delete_record.assert_called_once_with(1)

    @patch('main.delete_record')
    def test_delete_record_not_found(self, mock_delete_record):
        """Test the DELETE /records/{record_id} endpoint when record is not found."""
        # Mock the delete_record function to return False
        mock_delete_record.return_value = False

        # Make the request
        response = client.delete("/records/999")

        # Check the response
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Record with ID 999 not found"})

        # Verify that delete_record was called with the correct ID
        mock_delete_record.assert_called_once_with(999)

if __name__ == '__main__':
    unittest.main()
