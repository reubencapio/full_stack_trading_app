import sqlite3
import config

# Connect to the SQLite database
connection = sqlite3.connect(config.DB_FILE)

# Create a cursor object
cursor = connection.cursor()

# Create the stock table if it does not exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL UNIQUE,
        company TEXT NOT NULL,
        exchange TEXT NOT NULL
    )
""")

# Create the stock_price table if it does not exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price (
        id INTEGER PRIMARY KEY,
        stock_id INTEGER,
        date TEXT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        adjusted_close REAL NOT NULL,
        volume INTEGER NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock (id)
    )
""")

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()
