from sqlite_utils import cli
from click.testing import CliRunner
import pytest
import sqlite3


CREATE_TABLES = """
create table Gosh (c1 text, c2 text, c3 text);
create table Gosh2 (c1 text, c2 text, c3 text);
"""


@pytest.fixture
def db_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite3.connect(path)
    db.executescript(CREATE_TABLES)
    return path


def test_table_names(db_path):
    result = CliRunner().invoke(cli.cli, ["table_names", db_path])
    assert "Gosh\nGosh2" == result.output.strip()
