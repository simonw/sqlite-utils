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
]


@pytest.mark.parametrize("column_def,initial_value,expected_value", EXAMPLES)
def test_quote_default_value(fresh_db, column_def, initial_value, expected_value):
    fresh_db.execute("create table foo (col {})".format(column_def))
    assert initial_value == fresh_db["foo"].columns[0].default_value
    assert expected_value == fresh_db.quote_default_value(
        fresh_db["foo"].columns[0].default_value
    )
