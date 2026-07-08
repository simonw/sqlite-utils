import pytest

EXAMPLES = [
    ("TEXT DEFAULT 'foo'", "'foo'", "'foo'"),
    ("TEXT DEFAULT 'foo)'", "'foo)'", "'foo)'"),
    ("INTEGER DEFAULT '1'", "'1'", "'1'"),
    ("INTEGER DEFAULT 1", "1", "'1'"),
    ("INTEGER DEFAULT (1)", "1", "'1'"),
    # Expressions
    (
        "TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))",
        "STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')",
        "(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))",
    ),
    # Special values
    ("TEXT DEFAULT CURRENT_TIME", "CURRENT_TIME", "CURRENT_TIME"),
    ("TEXT DEFAULT CURRENT_DATE", "CURRENT_DATE", "CURRENT_DATE"),
    ("TEXT DEFAULT CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"),
    ("TEXT DEFAULT current_timestamp", "current_timestamp", "current_timestamp"),
    ("TEXT DEFAULT (CURRENT_TIMESTAMP)", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"),
    # Strings
    ("TEXT DEFAULT 'CURRENT_TIMESTAMP'", "'CURRENT_TIMESTAMP'", "'CURRENT_TIMESTAMP'"),
    ('TEXT DEFAULT "CURRENT_TIMESTAMP"', '"CURRENT_TIMESTAMP"', '"CURRENT_TIMESTAMP"'),
    # Boolean and null keyword literals must stay unquoted
    ("INTEGER DEFAULT TRUE", "TRUE", "TRUE"),
    ("INTEGER DEFAULT FALSE", "FALSE", "FALSE"),
    ("INTEGER DEFAULT true", "true", "true"),
    ("TEXT DEFAULT NULL", "NULL", "NULL"),
]


@pytest.mark.parametrize("column_def,initial_value,expected_value", EXAMPLES)
def test_quote_default_value(fresh_db, column_def, initial_value, expected_value):
    fresh_db.execute("create table foo (col {})".format(column_def))
    assert initial_value == fresh_db["foo"].columns[0].default_value
    assert expected_value == fresh_db.quote_default_value(
        fresh_db["foo"].columns[0].default_value
    )


def test_insert_empty_record_uses_default_values(fresh_db):
    fresh_db.execute("""
        CREATE TABLE has_defaults (
            id INTEGER PRIMARY KEY,
            name TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """)

    table = fresh_db["has_defaults"]
    table.insert({})

    rows = list(table.rows)
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] is None
    assert rows[0]["timestamp"] is not None
    assert rows[0]["is_active"] == 1
