import sqlite3

conn = sqlite3.connect('test.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT,
    value INTEGER
)
''')

cursor.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ('test1', 100))
cursor.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ('test2', 200))

conn.commit()

cursor.execute("SELECT * FROM test_table")
rows = cursor.fetchall()

print("Data in test_table:")
for row in rows:
    print(row)

conn.close()

print("SQLite test completed successfully!")
