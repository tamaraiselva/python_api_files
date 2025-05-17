import unittest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
import psycopg2

# Import the functions to test
from database import (
    get_db_connection,
    initialize_db,
    insert_csv_data,
    fetch_records,
    get_record_by_id,
    create_record,
    update_record,
    delete_record,
    TABLE_NAME
)

class TestDatabase(unittest.TestCase):

    def setUp(self):
        """Set up test environment before each test."""
        # Create mock connection and cursor
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

        # Patch the database connection
        self.conn_patcher = patch('database.get_db_connection', return_value=self.mock_conn)
        self.mock_get_conn = self.conn_patcher.start()

        # Create a sample DataFrame for testing
        self.sample_data = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'age': [30, 25, 40],
            'city': ['New York', 'Boston', 'Chicago']
        })

    def tearDown(self):
        """Clean up after each test."""
        self.conn_patcher.stop()

    def test_get_db_connection(self):
        """Test that get_db_connection returns a valid PostgreSQL connection."""
        # Stop the connection patcher to test the real function
        self.conn_patcher.stop()

        # Patch psycopg2.connect to return a mock connection
        with patch('psycopg2.connect', return_value=MagicMock()) as mock_connect:
            conn = get_db_connection()

            # Check that connect was called with the right parameters
            mock_connect.assert_called_once()

            # Check that the connection is returned
            self.assertIsNotNone(conn)

        # Restart the connection patcher for other tests
        self.conn_patcher = patch('database.get_db_connection', return_value=self.mock_conn)
        self.mock_get_conn = self.conn_patcher.start()

    def test_initialize_db(self):
        """Test that initialize_db creates the table if it doesn't exist."""
        # Call initialize_db
        initialize_db()

        # Check that the cursor executed the CREATE TABLE statement
        self.mock_cursor.execute.assert_called_with("""
        CREATE TABLE IF NOT EXISTS uploaded_data (
            id SERIAL PRIMARY KEY
        )
    """)

        # Check that commit was called
        self.mock_conn.commit.assert_called_once()

        # Check that cursor and connection were closed
        self.mock_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_insert_csv_data(self):
        """Test that insert_csv_data correctly inserts data into the database."""
        # Set up the mock cursor to handle the copy_expert method
        self.mock_cursor.copy_expert = MagicMock()

        # Call insert_csv_data
        insert_csv_data(self.sample_data)

        # Check that DROP TABLE was called
        self.mock_cursor.execute.assert_any_call(f"DROP TABLE IF EXISTS {TABLE_NAME}")

        # Check that CREATE TABLE was called with the right columns
        create_table_call = False
        for call in self.mock_cursor.execute.call_args_list:
            args, _ = call
            if "CREATE TABLE" in args[0] and all(col in args[0] for col in ['name', 'age', 'city']):
                create_table_call = True
                break
        self.assertTrue(create_table_call, "CREATE TABLE was not called with the right columns")

        # Check that copy_expert was called for data insertion
        self.mock_cursor.copy_expert.assert_called_once()

        # Check that commit was called
        self.mock_conn.commit.assert_called_once()

        # Check that cursor and connection were closed
        self.mock_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_fetch_records_filtered(self):
        """Test that fetch_records returns filtered records when a filter is provided."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return sample data
        mock_dict_cursor.fetchall.return_value = [
            {'name': 'Jane', 'age': 25, 'city': 'Boston'}
        ]

        # Call fetch_records
        records = fetch_records('name', 'Jane')

        # Check that the cursor executed the right query
        mock_dict_cursor.execute.assert_called_with(
            f"SELECT * FROM {TABLE_NAME} WHERE name = %s",
            ('Jane',)
        )

        # Check the returned records
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['name'], 'Jane')
        self.assertEqual(records[0]['age'], 25)
        self.assertEqual(records[0]['city'], 'Boston')

        # Check that cursor and connection were closed
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_get_record_by_id(self):
        """Test that get_record_by_id returns a record by ID."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return sample data
        mock_dict_cursor.fetchone.return_value = {
            'id': 1, 'name': 'Jane', 'age': 25, 'city': 'Boston'
        }

        # Call get_record_by_id
        record = get_record_by_id(1)

        # Check that the cursor executed the right query
        mock_dict_cursor.execute.assert_called_with(
            f"SELECT * FROM {TABLE_NAME} WHERE id = %s",
            (1,)
        )

        # Check the returned record
        self.assertEqual(record['id'], 1)
        self.assertEqual(record['name'], 'Jane')
        self.assertEqual(record['age'], 25)
        self.assertEqual(record['city'], 'Boston')

        # Check that cursor and connection were closed
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_get_record_by_id_not_found(self):
        """Test that get_record_by_id returns None when record is not found."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return None
        mock_dict_cursor.fetchone.return_value = None

        # Call get_record_by_id
        record = get_record_by_id(999)

        # Check that the cursor executed the right query
        mock_dict_cursor.execute.assert_called_with(
            f"SELECT * FROM {TABLE_NAME} WHERE id = %s",
            (999,)
        )

        # Check the returned record is None
        self.assertIsNone(record)

        # Check that cursor and connection were closed
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_create_record(self):
        """Test that create_record creates a new record."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return the created record
        mock_dict_cursor.fetchone.return_value = {
            'id': 1, 'name': 'John', 'age': 30, 'city': 'New York'
        }

        # Call create_record
        record_data = {'name': 'John', 'age': 30, 'city': 'New York'}
        new_record = create_record(record_data)

        # Check that the cursor executed the right query
        mock_dict_cursor.execute.assert_called_once()
        args, _ = mock_dict_cursor.execute.call_args
        self.assertIn("INSERT INTO", args[0])
        self.assertIn("RETURNING", args[0])

        # Check the returned record
        self.assertEqual(new_record['id'], 1)
        self.assertEqual(new_record['name'], 'John')
        self.assertEqual(new_record['age'], 30)
        self.assertEqual(new_record['city'], 'New York')

        # Check that commit, cursor close, and connection close were called
        self.mock_conn.commit.assert_called_once()
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_create_record_invalid_data(self):
        """Test that create_record raises ValueError with invalid data."""
        # Test with empty dictionary
        with self.assertRaises(ValueError):
            create_record({})

        # Test with None
        with self.assertRaises(ValueError):
            create_record(None)

        # Test with non-dictionary
        with self.assertRaises(ValueError):
            create_record("not a dictionary")

    def test_update_record(self):
        """Test that update_record updates a record."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return the record exists
        mock_dict_cursor.fetchone.side_effect = [
            {'id': 1},  # First call to check if record exists
            {'id': 1, 'name': 'John Updated', 'age': 31, 'city': 'Boston'}  # Second call to return updated record
        ]

        # Call update_record
        record_data = {'name': 'John Updated', 'age': 31, 'city': 'Boston'}
        updated_record = update_record(1, record_data)

        # Check that the cursor executed the right queries
        self.assertEqual(mock_dict_cursor.execute.call_count, 2)

        # First call should check if record exists
        first_call_args = mock_dict_cursor.execute.call_args_list[0][0]
        self.assertIn(f"SELECT id FROM {TABLE_NAME} WHERE id = %s", first_call_args[0])

        # Second call should update the record
        second_call_args = mock_dict_cursor.execute.call_args_list[1][0]
        self.assertIn(f"UPDATE {TABLE_NAME} SET", second_call_args[0])
        self.assertIn("RETURNING", second_call_args[0])

        # Check the returned record
        self.assertEqual(updated_record['id'], 1)
        self.assertEqual(updated_record['name'], 'John Updated')
        self.assertEqual(updated_record['age'], 31)
        self.assertEqual(updated_record['city'], 'Boston')

        # Check that commit, cursor close, and connection close were called
        self.mock_conn.commit.assert_called_once()
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_update_record_not_found(self):
        """Test that update_record returns None when record is not found."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return None (record not found)
        mock_dict_cursor.fetchone.return_value = None

        # Call update_record
        record_data = {'name': 'John Updated', 'age': 31, 'city': 'Boston'}
        updated_record = update_record(999, record_data)

        # Check that the cursor executed the right query
        mock_dict_cursor.execute.assert_called_once_with(
            f"SELECT id FROM {TABLE_NAME} WHERE id = %s",
            (999,)
        )

        # Check the returned record is None
        self.assertIsNone(updated_record)

        # Check that cursor and connection were closed
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_update_record_invalid_data(self):
        """Test that update_record raises ValueError with invalid data."""
        # Test with empty dictionary
        with self.assertRaises(ValueError):
            update_record(1, {})

        # Test with None
        with self.assertRaises(ValueError):
            update_record(1, None)

        # Test with non-dictionary
        with self.assertRaises(ValueError):
            update_record(1, "not a dictionary")

    def test_delete_record(self):
        """Test that delete_record deletes a record."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return the record exists
        mock_dict_cursor.fetchone.return_value = {'id': 1}

        # Call delete_record
        success = delete_record(1)

        # Check that the cursor executed the right queries
        self.assertEqual(mock_dict_cursor.execute.call_count, 2)

        # First call should check if record exists
        first_call_args = mock_dict_cursor.execute.call_args_list[0][0]
        self.assertIn(f"SELECT id FROM {TABLE_NAME} WHERE id = %s", first_call_args[0])

        # Second call should delete the record
        second_call_args = mock_dict_cursor.execute.call_args_list[1][0]
        self.assertIn(f"DELETE FROM {TABLE_NAME} WHERE id = %s", second_call_args[0])

        # Check the returned success flag
        self.assertTrue(success)

        # Check that commit, cursor close, and connection close were called
        self.mock_conn.commit.assert_called_once()
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

    def test_delete_record_not_found(self):
        """Test that delete_record returns False when record is not found."""
        # Set up the mock cursor to return sample data
        mock_dict_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_dict_cursor

        # Set up the mock cursor to return None (record not found)
        mock_dict_cursor.fetchone.return_value = None

        # Call delete_record
        success = delete_record(999)

        # Check that the cursor executed the right query
        mock_dict_cursor.execute.assert_called_once_with(
            f"SELECT id FROM {TABLE_NAME} WHERE id = %s",
            (999,)
        )

        # Check the returned success flag is False
        self.assertFalse(success)

        # Check that cursor and connection were closed
        mock_dict_cursor.close.assert_called_once()
        self.mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
