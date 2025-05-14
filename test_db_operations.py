import database
import pandas as pd

print('Testing PostgreSQL database operations...')

try:
    # Initialize the database
    print('Initializing database...')
    database.initialize_db()
    print('Database initialized successfully!')

    # Create a sample DataFrame
    print('Creating sample data...')
    data = {
        'name': ['John', 'Jane'],
        'age': [30, 25]
    }
    df = pd.DataFrame(data)
    print('Sample data created successfully!')

    # Insert data into the database
    print('Inserting data into database...')
    database.insert_csv_data(df)
    print('Data inserted successfully!')

    # Fetch records from the database
    print('Fetching records from database...')
    records = database.fetch_records()
    print(f'Records fetched successfully! Found {len(records)} records:')
    for record in records:
        print(record)

except Exception as e:
    print(f'Error: {str(e)}')
