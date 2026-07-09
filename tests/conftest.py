from sqlite_utils import Database
from sqlite_utils.utils import sqlite3
import pytest

CREATE_TABLES = """
create table Gosh (c1 text, c2 text, c3 text);
create table Gosh2 (c1 text, c2 text, c3 text);
"""


def pytest_addoption(parser):
    parser.addoption(
        "--sqlite-autocommit",
        action="store_true",
        default=False,
        help=(
            "Run every test against connections created with the Python 3.12+ "
            "sqlite3.connect(autocommit=True) mode"
        ),
    )
    parser.addoption(
        "--sqlite-autocommit-false",
        action="store_true",
        default=False,
        help=(
            "Run every test against connections created with the Python 3.12+ "
            "sqlite3.connect(autocommit=False) mode"
        ),
    )


def pytest_configure(config):
    import sys

    sys._called_from_test = True  # type: ignore[attr-defined]

    autocommit_true = config.getoption("--sqlite-autocommit")
    autocommit_false = config.getoption("--sqlite-autocommit-false")
    if autocommit_true and autocommit_false:
        raise pytest.UsageError(
            "--sqlite-autocommit and --sqlite-autocommit-false are mutually exclusive"
        )
    if autocommit_true or autocommit_false:
        if sys.version_info < (3, 12):
            raise pytest.UsageError(
                "--sqlite-autocommit and --sqlite-autocommit-false require "
                "Python 3.12 or higher"
            )
        real_connect = sqlite3.connect

        def autocommit_connect(*args, **kwargs):
            kwargs.setdefault("autocommit", autocommit_true)
            return real_connect(*args, **kwargs)

        sqlite3.connect = autocommit_connect


@pytest.fixture(autouse=True)
def close_all_databases():
    """Automatically close all Database objects created during a test."""
    databases = []
    original_init = Database.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        databases.append(self)

    Database.__init__ = tracking_init  # type: ignore[method-assign]
    yield
    Database.__init__ = original_init  # type: ignore[method-assign]
    for db in databases:
        try:
            db.close()
        except Exception:
            pass


@pytest.fixture
def fresh_db():
    return Database(memory=True)


@pytest.fixture
def existing_db():
    database = Database(memory=True)
    database.executescript("""
        CREATE TABLE foo (text TEXT);
        INSERT INTO foo (text) values ("one");
        INSERT INTO foo (text) values ("two");
        INSERT INTO foo (text) values ("three");
    """)
    return database


@pytest.fixture
def db_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite3.connect(path)
    db.executescript(CREATE_TABLES)
    db.commit()
    db.close()
    return path
