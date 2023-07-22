from sqlite_utils import Database
from sqlite_utils.utils import sqlite3
import pytest

CREATE_TABLES = """
create table Gosh (c1 text, c2 text, c3 text);
create table Gosh2 (c1 text, c2 text, c3 text);
"""


def pytest_configure(config):
    import sys

    sys._called_from_test = True


@pytest.fixture
def fresh_db():
    return Database(memory=True)


@pytest.fixture
def existing_db():
    database = Database(memory=True)
    database.executescript(
        """
        CREATE TABLE foo (text TEXT);
        INSERT INTO foo (text) values ("one");
        INSERT INTO foo (text) values ("two");
        INSERT INTO foo (text) values ("three");
    """
    )
    return database


@pytest.fixture
def db_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite3.connect(path)
    db.executescript(CREATE_TABLES)
    return path
