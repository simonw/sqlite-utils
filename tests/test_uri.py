"""
Tests for SQLite URI filename support
https://github.com/simonw/sqlite-utils/issues/650
"""
import pytest
import sqlite_utils
from sqlite_utils import Database
from click.testing import CliRunner
from sqlite_utils import cli
import pathlib
import tempfile
import os


@pytest.fixture
def test_db_file(tmp_path):
    """Create a test database file with some data"""
    db_path = tmp_path / "test.db"
    db = Database(str(db_path))
    db["test_table"].insert_all([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ], pk="id")
    db.close()
    return db_path


def test_database_class_with_uri(test_db_file):
    """Test that Database class can open a URI"""
    # Test read-only mode
    uri = f"file:{test_db_file}?mode=ro"
    db = Database(uri)
    rows = list(db["test_table"].rows)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    db.close()


def test_database_class_with_uri_immutable(test_db_file):
    """Test that Database class can open a URI with immutable flag"""
    uri = f"file:{test_db_file}?immutable=1"
    db = Database(uri)
    rows = list(db["test_table"].rows)
    assert len(rows) == 3
    db.close()


def test_database_class_with_uri_multiple_params(test_db_file):
    """Test URI with multiple query parameters"""
    uri = f"file:{test_db_file}?mode=ro&immutable=1"
    db = Database(uri)
    rows = list(db["test_table"].rows)
    assert len(rows) == 3
    db.close()


def test_cli_tables_with_uri(test_db_file):
    """Test that tables command works with URI"""
    runner = CliRunner()
    uri = f"file:{test_db_file}?mode=ro"
    result = runner.invoke(cli.cli, ["tables", uri, "--csv"])
    assert result.exit_code == 0
    assert "test_table" in result.output


def test_cli_tables_with_uri_immutable(test_db_file):
    """Test that tables command works with URI and immutable flag"""
    runner = CliRunner()
    uri = f"file:{test_db_file}?mode=ro&immutable=1"
    result = runner.invoke(cli.cli, ["tables", uri])
    assert result.exit_code == 0
    assert "test_table" in result.output


def test_cli_query_with_uri(test_db_file):
    """Test that query command works with URI"""
    runner = CliRunner()
    uri = f"file:{test_db_file}?mode=ro"
    result = runner.invoke(cli.cli, ["query", uri, "SELECT * FROM test_table"])
    assert result.exit_code == 0
    assert "Alice" in result.output
    assert "Bob" in result.output


def test_cli_query_with_uri_multiple_params(test_db_file):
    """Test that query command works with URI with multiple parameters"""
    runner = CliRunner()
    uri = f"file:{test_db_file}?mode=ro&immutable=1"
    result = runner.invoke(cli.cli, ["query", uri, "SELECT name FROM test_table WHERE id = 1"])
    assert result.exit_code == 0
    assert "Alice" in result.output


def test_cli_rows_with_uri(test_db_file):
    """Test that rows command works with URI"""
    runner = CliRunner()
    uri = f"file:{test_db_file}?mode=ro"
    result = runner.invoke(cli.cli, ["rows", uri, "test_table"])
    assert result.exit_code == 0
    assert "Alice" in result.output


def test_uri_with_relative_path(tmp_path):
    """Test URI with relative path"""
    # Create a database in a subdirectory
    db_path = tmp_path / "test.db"
    db = Database(str(db_path))
    db["test_table"].insert({"id": 1, "name": "Test"})
    db.close()

    # Test with relative path in URI
    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        uri = "file:test.db?mode=ro"
        db = Database(uri)
        rows = list(db["test_table"].rows)
        assert len(rows) == 1
        db.close()
    finally:
        os.chdir(old_cwd)


def test_uri_with_absolute_path(test_db_file):
    """Test URI with absolute path"""
    # Use triple slash for absolute path
    uri = f"file:///{test_db_file}?mode=ro"
    db = Database(uri)
    rows = list(db["test_table"].rows)
    assert len(rows) == 3
    db.close()


def test_regular_path_still_works(test_db_file):
    """Ensure regular file paths still work after URI changes"""
    # Test Database class
    db = Database(str(test_db_file))
    rows = list(db["test_table"].rows)
    assert len(rows) == 3
    db.close()

    # Test CLI
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["tables", str(test_db_file)])
    assert result.exit_code == 0
    assert "test_table" in result.output


def test_cli_insert_with_uri_fails_readonly():
    """Test that insert fails with read-only URI"""
    runner = CliRunner()
    with runner.isolated_filesystem():
        # Create a database first
        db = Database("test.db")
        db["test_table"].insert({"id": 1, "name": "Test"})
        db.close()

        # Try to insert with read-only URI
        uri = "file:test.db?mode=ro"
        result = runner.invoke(cli.cli, ["insert", uri, "test_table", "-"],
                              input='{"id": 2, "name": "Another"}')
        assert result.exit_code != 0
        assert "readonly" in result.output.lower() or "read-only" in result.output.lower() or "attempt to write" in result.output.lower()


def test_nonexistent_file_with_uri_mode_rwc():
    """Test that URI with mode=rwc can create new database"""
    runner = CliRunner()
    with runner.isolated_filesystem():
        uri = "file:newdb.db?mode=rwc"
        # This should create the database and table
        result = runner.invoke(cli.cli, ["insert", uri, "test_table", "-"],
                              input='{"id": 1, "name": "Test"}')
        assert result.exit_code == 0
        # Verify the database was created
        assert os.path.exists("newdb.db")
