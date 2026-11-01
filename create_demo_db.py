import os
import sqlite3

# Remove any existing demo.db so we start fresh
if os.path.exists("demo.db"):
    os.remove("demo.db")

conn = sqlite3.connect("demo.db")
conn.executescript("""
CREATE TABLE IF NOT EXISTS movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    year INTEGER,
    genre TEXT
);
INSERT INTO movies VALUES (1, 'Inception', 2010, 'Sci-Fi');
INSERT INTO movies VALUES (2, 'The Matrix', 1999, 'Sci-Fi');
INSERT INTO movies VALUES (3, 'Interstellar', 2014, 'Sci-Fi');
""")
conn.commit()
conn.close()
print("demo.db created successfully")

