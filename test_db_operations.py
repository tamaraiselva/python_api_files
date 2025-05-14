import database
import pandas as pd

print('Testing PostgreSQL database operations...')

try:
    print('Initializing database...')
    database.initialize_db()
    print('Database initialized successfully!')

    print('Creating sample data...')
    data = {
        'name': ['Yegna Subramanian Jambunath', 'Jane'],
        'age': [30, 25]
    }
    df = pd.DataFrame(data)
    print('Sample data created successfully!')

    print('Inserting data into database...')
    database.insert_csv_data(df)
    print('Data inserted successfully!')

    print('Fetching records from database...')
    records = database.fetch_records()
    print(f'Records fetched successfully! Found {len(records)} records:')
    for record in records:
        print(record)

except Exception as e:
    print(f'Error: {str(e)}')
