from sqlite_utils import Database
import pytest


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
