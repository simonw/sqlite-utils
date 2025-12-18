import pytest
import json
from sqlite_utils import Database

def test_insert_dict_default_behavior():
    """
    By default (without use_json_converters=True), inserting a dict
    should create a TEXT column and return a string.
    """
    db = Database(memory=True)
    data = {"id": 1, "nested": {"foo": "bar"}}
    t = db["t"]
    t.insert(data, pk="id")
    
    # Check column type
    # Access column by name to be robust against ordering changes
    col = next(c for c in t.columns if c.name == "nested")
    assert col.type == "TEXT"
    
    # Check returned value
    row = t.get(1)
    assert isinstance(row["nested"], str)
    assert row["nested"] == '{"foo": "bar"}'

def test_explicit_json_column_creation():
    """
    Test repeatedly creating a table with a specific JSON column type.
    """
    db = Database(memory=True)
    # This should work if JSON is in COLUMN_TYPE_MAPPING
    db["t"].create({"id": int, "data": "JSON"}, pk="id")
    col = next(c for c in db["t"].columns if c.name == "data")
    assert col.type == "JSON"

def test_use_json_converters_argument_exists():
    """
    Test that Database accepts use_json_converters argument.
    """
    # strict=False is default, just ensuring we can pass the arg
    try:
        db = Database(memory=True, use_json_converters=True)
    except TypeError:
        pytest.fail("Database does not accept use_json_converters argument")

def test_insert_dict_with_json_converters_enabled():
    """
    With use_json_converters=True:
    1. Inserting a dict should create a JSON column.
    2. Retrieving it should return a dict (auto-deserialization).
    """
    db = Database(memory=True, use_json_converters=True)
    data = {"id": 1, "attrs": {"color": "red", "size": 10}}
    
    db["items"].insert(data, pk="id")
    
    # Verify column type is inferred as JSON (not JSONB)
    col = next(c for c in db["items"].columns if c.name == "attrs")
    assert col.type == "JSON"
    
    # Verify auto-deserialization
    row = db["items"].get(1)
    assert isinstance(row["attrs"], dict)
    assert row["attrs"] == data["attrs"]
    
def test_list_deserialization():
    """
    Test that lists are also handled correctly when use_json_converters=True.
    """
    db = Database(memory=True, use_json_converters=True)

    data = {"id": 1, "tags": ["a", "b", "c"]}
    db["items"].insert(data, pk="id")
    
    # Verify column type is inferred as JSON
    col = next(c for c in db["items"].columns if c.name == "tags")
    assert col.type == "JSON"

    # Verify deserialization
    row = db["items"].get(1)
    assert isinstance(row["tags"], list)
    assert row["tags"] == ["a", "b", "c"]

def test_explicit_json_column_deserialization():
    """
    Test that explicit JSON columns are deserialized when flag is enabled.
    """
    db = Database(memory=True, use_json_converters=True)
    
    # Create table explicitly
    db["t"].create({"id": int, "data": "JSON"}, pk="id")
    
    data = {"foo": "bar"}
    db["t"].insert({"id": 1, "data": data})
    
    # Explicit creation doesn't rely on suggest_column_types for type, but insert might
    row = db["t"].get(1)
    assert isinstance(row["data"], dict)
    assert row["data"] == data

def test_suggest_column_types_conditional_behavior():
    """
    Test that suggest_column_types behaves differently when json_converters=True.
    """
    from sqlite_utils.utils import suggest_column_types
    records = [{"a": {"foo": "bar"}}, {"a": {"baz": 1}}]
    
    # Default: returns str
    assert suggest_column_types(records) == {"a": str}
    assert suggest_column_types(records, json_converters=False) == {"a": str}
    
    # With flag: returns dict
    assert suggest_column_types(records, json_converters=True) == {"a": dict}
    
    list_records = [{"b": [1, 2]}, {"b": [3]}]
    assert suggest_column_types(list_records, json_converters=True) == {"b": list}

def test_json_null_values():
    """
    Test that null values are handled correctly in JSON columns.
    """
    db = Database(memory=True, use_json_converters=True)
    db["t"].insert({"id": 1, "data": None}, pk="id")
    
    # Check column type (should be inferred as TEXT by default if only None is seen, 
    # but here we just want to see if it breaks)
    row = db["t"].get(1)
    assert row["data"] is None

def test_deeply_nested_structures():
    """
    Test deeply nested structures.
    """
    db = Database(memory=True, use_json_converters=True)
    data = {"a": {"b": {"c": [1, 2, {"d": "e"}]}}}
    db["t"].insert({"id": 1, "data": data}, pk="id")
    
    row = db["t"].get(1)
    assert row["data"] == data

def test_malformed_json_raises_error():
    """
    Test that malformed JSON in a JSON-declared column raises an error on retrieval.
    This is standard SQLite PARSE_DECLTYPES behavior with a registered converter.
    """
    import sqlite3
    db = Database(memory=True, use_json_converters=True)
    # Manually insert malformed JSON into a JSON column
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON)")
    db.execute("INSERT INTO t (id, data) VALUES (1, '{malformed')")
    
    with pytest.raises(Exception):
        db["t"].get(1)

def test_json_converters_only_affects_json_columns():
    """
    Verfiy that use_json_converters=True does NOT affect columns declared as TEXT.
    """
    db = Database(memory=True, use_json_converters=True)
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
    db.execute("INSERT INTO t (id, data) VALUES (1, '{\"a\": 1}')")
    
    row = db["t"].get(1)
    assert isinstance(row["data"], str)
    assert row["data"] == '{"a": 1}'

def test_tuple_deserialization():
    """
    Test that tuples are also handled correctly when use_json_converters=True.
    """
    db = Database(memory=True, use_json_converters=True)

    data = {"id": 1, "nested_tuple": (1, 2, 3)}
    db["items"].insert(data, pk="id")
    
    # Verify column type is inferred as JSON
    col = next(c for c in db["items"].columns if c.name == "nested_tuple")
    assert col.type == "JSON"

    # Verify deserialization
    row = db["items"].get(1)
    # Tuples become lists after JSON roundtrip
    assert isinstance(row["nested_tuple"], list)
    assert row["nested_tuple"] == [1, 2, 3]

