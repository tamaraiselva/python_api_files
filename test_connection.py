import database

print('Testing PostgreSQL database connection...')
try:
    conn = database.get_db_connection()
    print('Connection successful!')
    conn.close()
except Exception as e:
    print(f'Connection failed: {str(e)}')
