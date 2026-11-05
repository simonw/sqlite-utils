import pathlib
import subprocess
import sys
import time

import pytest
from click.testing import CliRunner

from sqlite_utils import Database, cli


@pytest.fixture
def test_db_and_path(tmpdir):
    db_path = str(pathlib.Path(tmpdir) / "data.db")
    db = Database(db_path)
    db.table("example").insert_all(
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
    ] == list(db.table("example").rows)


def test_cli_bulk_multiple_functions(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "bulk",
            db_path,
            "insert into example (id, name) values (:id, myupper(mylower(:name)))",
            "-",
            "--nl",
            "--functions",
            "myupper = lambda s: s.upper()",
            "--functions",
            "mylower = lambda s: s.lower()",
        ],
        input='{"id": 3, "name": "ThReE"}\n{"id": 4, "name": "FoUr"}\n',
    )
    assert result.exit_code == 0, result.output
    assert [
        {"id": 1, "name": "One"},
        {"id": 2, "name": "Two"},
        {"id": 3, "name": "THREE"},
        {"id": 4, "name": "FOUR"},
    ] == list(db.table("example").rows)


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
    assert proc.stdin is not None
    # Writing one record should not commit
    proc.stdin.write(b'{"id": 3, "name": "Three"}\n\n')
    proc.stdin.flush()
    time.sleep(1)
    assert db.table("example").count == 2

    # Writing another should trigger a commit:
    proc.stdin.write(b'{"id": 4, "name": "Four"}\n\n')
    proc.stdin.flush()
    time.sleep(1)
    assert db.table("example").count == 4

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


def test_cli_bulk_stdin_conversion_conflict(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "bulk",
            db_path,
            "insert into example (id, name) values (:id, :name)",
            "-",
            "--convert-file",
            "-",
        ],
        input="def convert(row):\n    return row\n",
    )
    assert result.exit_code == 2, result.output
    assert "Cannot read both data and conversion code from stdin" in result.output
    assert db.table("example").count == 2


def test_cli_bulk_stdin_with_inline_convert(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "bulk",
            db_path,
            "insert into example (id, name) values (:id, :name)",
            "-",
            "--nl",
            "--convert",
            "def convert(row):\n    row['name'] = row['name'].upper()\n    return row",
        ],
        input='{"id": 3, "name": "Three"}\n',
    )
    assert result.exit_code == 0, result.output
    assert list(db.query("select id, name from example where id = 3")) == [
        {"id": 3, "name": "THREE"}
    ]


def test_cli_bulk_script_from_stdin(test_db_and_path, tmp_path):
    db, db_path = test_db_and_path
    data_file = tmp_path / "data.json"
    data_file.write_text('{"id": 3, "name": "Three"}', encoding="utf-8")

    result = CliRunner().invoke(
        cli.cli,
        [
            "bulk",
            db_path,
            "insert into example (id, name) values (:id, :name)",
            str(data_file),
            "--convert-file",
            "-",
        ],
        input="def convert(row):\n    row['name'] = row['name'].upper()\n    return row\n",
    )
    assert result.exit_code == 0, result.output
    assert list(db.query("select id, name from example where id = 3")) == [
        {"id": 3, "name": "THREE"}
    ]
