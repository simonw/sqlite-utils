import pytest
from click.testing import CliRunner
from sqlite_utils import cli, Database
import json

def test_insert_json_converters(tmpdir):
    db_path = str(tmpdir / "test.db")
    runner = CliRunner()
    
    # Input data with nested structure
    data = [{"id": 1, "nested": {"foo": "bar"}}, {"id": 2, "nested": {"baz": 123}}]
    input_str = json.dumps(data)
    
    # Insert without --use-json-converters
    result = runner.invoke(cli.cli, ["insert", db_path, "t1", "-"], input=input_str)
    assert result.exit_code == 0
    
    db = Database(db_path)
    # Access column by name to be robust against ordering changes
    assert next(c for c in db["t1"].columns if c.name == "nested").type == "TEXT"
    row = db["t1"].get(1)
    assert isinstance(row["nested"], str)
    
    # Insert WITH --use-json-converters
    result = runner.invoke(cli.cli, ["insert", db_path, "t2", "-", "--use-json-converters"], input=input_str)
    assert result.exit_code == 0
    
    # We need to open with use_json_converters=True to see the effect on get()
    db_json = Database(db_path, use_json_converters=True)
    assert next(c for c in db_json["t2"].columns if c.name == "nested").type == "JSON"
    row = db_json["t2"].get(1)
    assert isinstance(row["nested"], dict)
    assert row["nested"] == {"foo": "bar"}

def test_query_json_converters(tmpdir):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path, use_json_converters=True)
    db["t"].insert({"id": 1, "data": {"a": 1}}, pk="id")
    
    runner = CliRunner()
    
    # Query without flag - ensure it does not automatically deserialize by default in CLI
    result = runner.invoke(cli.cli, ["query", db_path, "select * from t"])
    assert result.exit_code == 0
    assert '"data": "{\\"a\\": 1}"' in result.output
    
    # Query WITH flag
    result = runner.invoke(cli.cli, ["query", db_path, "select * from t", "--use-json-converters"])
    assert result.exit_code == 0
    # Now it should be a real nested object in the JSON output
    assert '"data": {"a": 1}' in result.output

def test_create_table_json_type(tmpdir):
    db_path = str(tmpdir / "test.db")
    runner = CliRunner()
    
    # Create table with JSON type
    result = runner.invoke(cli.cli, ["create-table", db_path, "t", "id", "integer", "data", "json", "--pk", "id"])
    assert result.exit_code == 0
    
    db = Database(db_path)
    assert next(c for c in db["t"].columns if c.name == "data").type == "JSON"

def test_memory_json_converters(tmpdir):
    csv_path = str(tmpdir / "test.csv")
    with open(csv_path, "w") as f:
        f.write("id,data\n1,'{\"a\": 1}'")
        
    runner = CliRunner()
    
    # Query memory with flag - check output deserialization
    result = runner.invoke(cli.cli, ["memory", csv_path, "select * from test", "--use-json-converters"])
    assert result.exit_code == 0
    
    # Let's try with JSON input
    json_path = str(tmpdir / "test.json")
    with open(json_path, "w") as f:
        json.dump([{"id": 1, "data": {"a": 1}}], f)
        
    result = runner.invoke(cli.cli, ["memory", json_path, "select * from test", "--use-json-converters"])
    assert result.exit_code == 0
    assert '"data": {"a": 1}' in result.output
