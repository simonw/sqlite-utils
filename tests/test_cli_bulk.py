from click.testing import CliRunner
from sqlite_utils import cli, Database
import pathlib
import pytest
import subprocess
import sys
import time


@pytest.fixture
def test_db_and_path(tmpdir):
    db_path = str(pathlib.Path(tmpdir) / "data.db")
    db = Database(db_path)
    db["example"].insert_all(
        [
            {"id": 1, "name": "One"},
            {"id": 2, "name": "Two"},
        ],
        pk="id",
    )
    return db, db_path


def test_cli_bulk(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "bulk",
            db_path,
            "insert into example (id, name) values (:id, myupper(:name))",
            "-",
            "--nl",
            "--functions",
            "myupper = lambda s: s.upper()",
        ],
        input='{"id": 3, "name": "Three"}\n{"id": 4, "name": "Four"}\n',
    )
    assert result.exit_code == 0, result.output
    assert [
        {"id": 1, "name": "One"},
        {"id": 2, "name": "Two"},
        {"id": 3, "name": "THREE"},
        {"id": 4, "name": "FOUR"},
    ] == list(db["example"].rows)


def test_cli_bulk_batch_size(test_db_and_path):
    db, db_path = test_db_and_path
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "sqlite_utils",
            "bulk",
            db_path,
            "insert into example (id, name) values (:id, :name)",
            "-",
            "--nl",
            "--batch-size",
            "2",
        ],
        stdin=subprocess.PIPE,
        stdout=sys.stdout,
    )
    # Writing one record should not commit
    proc.stdin.write(b'{"id": 3, "name": "Three"}\n\n')
    proc.stdin.flush()
    time.sleep(1)
    assert db["example"].count == 2

    # Writing another should trigger a commit:
    proc.stdin.write(b'{"id": 4, "name": "Four"}\n\n')
    proc.stdin.flush()
    time.sleep(1)
    assert db["example"].count == 4

    proc.stdin.close()
    proc.wait()
    assert proc.returncode == 0


def test_cli_bulk_error(test_db_and_path):
    _, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "bulk",
            db_path,
            "insert into example (id, name) value (:id, :name)",
            "-",
            "--nl",
        ],
        input='{"id": 3, "name": "Three"}',
    )
    assert result.exit_code == 1
    assert result.output == 'Error: near "value": syntax error\n'
