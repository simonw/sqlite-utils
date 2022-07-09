import datetime


def test_duplicate(fresh_db):
    # Create table using native Sqlite statement:
    fresh_db.execute(
        """CREATE TABLE [table1] (
    [text_col] TEXT,
    [real_col] REAL,
    [int_col] INTEGER,
    [bool_col] INTEGER,
    [datetime_col] TEXT)"""
    )
    # Insert one row of mock data:
    dt = datetime.datetime.now()
    data = {
        "text_col": "Cleo",
        "real_col": 3.14,
        "int_col": -255,
        "bool_col": True,
        "datetime_col": str(dt),
    }
    table1 = fresh_db["table1"]
    row_id = table1.insert(data).last_rowid
    # Duplicate table:
    table2 = table1.duplicate("table2")
    # Ensure data integrity:
    assert data == table2.get(row_id)
    # Ensure schema integrity:
    assert [
        {"name": "text_col", "type": "TEXT"},
        {"name": "real_col", "type": "REAL"},
        {"name": "int_col", "type": "INT"},
        {"name": "bool_col", "type": "INT"},
        {"name": "datetime_col", "type": "TEXT"},
    ] == [{"name": col.name, "type": col.type} for col in table2.columns]
