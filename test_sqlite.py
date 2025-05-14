import sqlite3

# Create a connection to a SQLite database (will be created if it doesn't exist)
conn = sqlite3.connect('test.db')
cursor = conn.cursor()

# Create a table
cursor.execute('''
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT,
    value INTEGER
)
''')

# Insert some data
cursor.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ('test1', 100))
cursor.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ('test2', 200))

# Commit the changes
conn.commit()

# Query the data
cursor.execute("SELECT * FROM test_table")
rows = cursor.fetchall()

# Print the results
print("Data in test_table:")
for row in rows:
    print(row)

# Close the connection
conn.close()

print("SQLite test completed successfully!")
