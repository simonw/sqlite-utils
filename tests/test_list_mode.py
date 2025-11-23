"""
Tests for list-based iteration in insert_all and upsert_all
"""

import pytest
from sqlite_utils import Database


def test_insert_all_list_mode_basic():
    """Test basic insert_all with list-based iteration"""
    db = Database(memory=True)

    def data_generator():
        # First yield column names
        yield ["id", "name", "age"]
        # Then yield data rows
        yield [1, "Alice", 30]
        yield [2, "Bob", 25]
        yield [3, "Charlie", 35]

    db["people"].insert_all(data_generator())

    rows = list(db["people"].rows)
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_insert_all_list_mode_with_pk():
    """Test insert_all with list mode and primary key"""
    db = Database(memory=True)

    def data_generator():
        yield ["id", "name", "score"]
        yield [1, "Alice", 95]
        yield [2, "Bob", 87]

    db["scores"].insert_all(data_generator(), pk="id")

    assert db["scores"].pks == ["id"]
    rows = list(db["scores"].rows)
    assert len(rows) == 2


def test_upsert_all_list_mode():
    """Test upsert_all with list-based iteration"""
    db = Database(memory=True)

    # Initial insert
    def initial_data():
        yield ["id", "name", "value"]
        yield [1, "Alice", 100]
        yield [2, "Bob", 200]

    db["data"].insert_all(initial_data(), pk="id")

    # Upsert with some updates and new records
    def upsert_data():
        yield ["id", "name", "value"]
        yield [1, "Alice", 150]  # Update existing
        yield [3, "Charlie", 300]  # Insert new

    db["data"].upsert_all(upsert_data(), pk="id")

    rows = list(db["data"].rows_where(order_by="id"))
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "value": 150}
    assert rows[1] == {"id": 2, "name": "Bob", "value": 200}
    assert rows[2] == {"id": 3, "name": "Charlie", "value": 300}


def test_list_mode_with_various_types():
    """Test list mode with different data types"""
    db = Database(memory=True)

    def data_generator():
        yield ["id", "name", "score", "active"]
        yield [1, "Alice", 95.5, True]
        yield [2, "Bob", 87.3, False]
        yield [3, "Charlie", None, True]

    db["mixed"].insert_all(data_generator())

    rows = list(db["mixed"].rows)
    assert len(rows) == 3
    assert rows[0]["score"] == 95.5
    assert rows[1]["active"] == 0  # SQLite stores boolean as int
    assert rows[2]["score"] is None


def test_list_mode_error_non_string_columns():
    """Test that non-string column names raise an error"""
    db = Database(memory=True)

    def bad_data():
        yield [1, 2, 3]  # Non-string column names
        yield ["a", "b", "c"]

    with pytest.raises(ValueError, match="must be a list of column name strings"):
        db["bad"].insert_all(bad_data())


def test_list_mode_error_mixed_types():
    """Test that mixing list and dict raises an error"""
    db = Database(memory=True)

    def bad_data():
        yield ["id", "name"]
        yield {"id": 1, "name": "Alice"}  # Should be a list, not dict

    with pytest.raises(ValueError, match="must also be lists"):
        db["bad"].insert_all(bad_data())


def test_list_mode_empty_after_headers():
    """Test that only headers without data works gracefully"""
    db = Database(memory=True)

    def data_generator():
        yield ["id", "name", "age"]
        # No data rows

    result = db["people"].insert_all(data_generator())
    assert result is not None
    assert not db["people"].exists()


def test_list_mode_batch_processing():
    """Test list mode with large dataset requiring batching"""
    db = Database(memory=True)

    def large_data():
        yield ["id", "value"]
        for i in range(1000):
            yield [i, f"value_{i}"]

    db["large"].insert_all(large_data(), batch_size=100)

    count = db.execute("SELECT COUNT(*) as c FROM large").fetchone()[0]
    assert count == 1000


def test_list_mode_shorter_rows():
    """Test that rows shorter than column list get NULL values"""
    db = Database(memory=True)

    def data_generator():
        yield ["id", "name", "age", "city"]
        yield [1, "Alice", 30, "NYC"]
        yield [2, "Bob"]  # Missing age and city
        yield [3, "Charlie", 35]  # Missing city

    db["people"].insert_all(data_generator())

    rows = list(db["people"].rows_where(order_by="id"))
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30, "city": "NYC"}
    assert rows[1] == {"id": 2, "name": "Bob", "age": None, "city": None}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35, "city": None}


def test_backwards_compatibility_dict_mode():
    """Ensure dict mode still works (backward compatibility)"""
    db = Database(memory=True)

    # Traditional dict-based insert
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
    ]

    db["people"].insert_all(data)

    rows = list(db["people"].rows)
    assert len(rows) == 2
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}


def test_insert_all_tuple_mode_basic():
    """Test basic insert_all with tuple-based iteration"""
    db = Database(memory=True)

    def data_generator():
        # First yield column names as tuple
        yield ("id", "name", "age")
        # Then yield data rows as tuples
        yield (1, "Alice", 30)
        yield (2, "Bob", 25)
        yield (3, "Charlie", 35)

    db["people"].insert_all(data_generator())

    rows = list(db["people"].rows)
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_insert_all_mixed_list_tuple():
    """Test insert_all with mixed lists and tuples for data rows"""
    db = Database(memory=True)

    def data_generator():
        # Column names as list
        yield ["id", "name", "age"]
        # Mix of list and tuple data rows
        yield [1, "Alice", 30]
        yield (2, "Bob", 25)
        yield [3, "Charlie", 35]
        yield (4, "Diana", 40)

    db["people"].insert_all(data_generator())

    rows = list(db["people"].rows)
    assert len(rows) == 4
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35}
    assert rows[3] == {"id": 4, "name": "Diana", "age": 40}


def test_upsert_all_tuple_mode():
    """Test upsert_all with tuple-based iteration"""
    db = Database(memory=True)

    # Initial insert with tuples
    def initial_data():
        yield ("id", "name", "value")
        yield (1, "Alice", 100)
        yield (2, "Bob", 200)

    db["data"].insert_all(initial_data(), pk="id")

    # Upsert with tuples
    def upsert_data():
        yield ("id", "name", "value")
        yield (1, "Alice", 150)  # Update existing
        yield (3, "Charlie", 300)  # Insert new

    db["data"].upsert_all(upsert_data(), pk="id")

    rows = list(db["data"].rows_where(order_by="id"))
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "value": 150}
    assert rows[1] == {"id": 2, "name": "Bob", "value": 200}
    assert rows[2] == {"id": 3, "name": "Charlie", "value": 300}


def test_tuple_mode_shorter_rows():
    """Test that tuple rows shorter than column list get NULL values"""
    db = Database(memory=True)

    def data_generator():
        yield "id", "name", "age", "city"
        yield 1, "Alice", 30, "NYC"
        yield 2, "Bob"  # Missing age and city
        yield 3, "Charlie", 35  # Missing city

    db["people"].insert_all(data_generator())

    rows = list(db["people"].rows_where(order_by="id"))
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30, "city": "NYC"}
    assert rows[1] == {"id": 2, "name": "Bob", "age": None, "city": None}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35, "city": None}
