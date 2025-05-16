import psycopg2

print("Testing PostgreSQL connection...")

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5433",
        dbname="database",
        user="postgres",
        password="Password"
    )
    print("Connection successful!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    conn.close()
except Exception as e:
    print(f"Connection failed: {str(e)}")
