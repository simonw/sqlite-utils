from sqlite_utils import Database
import sqlite3
import pytest


@pytest.fixture
def fresh_db():
    return Database(sqlite3.connect(":memory:"))


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
