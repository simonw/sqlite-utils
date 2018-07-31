from sqlite_utils import Database
import sqlite3
import pytest


@pytest.fixture
def existing_db():
    database = Database(sqlite3.connect(":memory:"))
    database.conn.executescript(
        """
        CREATE TABLE foo (text TEXT);
        INSERT INTO foo (text) values ("one");
        INSERT INTO foo (text) values ("two");
        INSERT INTO foo (text) values ("three");
    """
    )
    return database


def test_count(existing_db):
    assert 3 == existing_db["foo"].count


def test_columns(existing_db):
    table = existing_db["foo"]
    assert [{"name": "text", "type": "TEXT"}] == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]
