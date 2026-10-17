import csv
import io
import json

import pytest
from click.testing import CliRunner

from sqlite_utils import Database, cli


@pytest.mark.parametrize("command", ["insert", "upsert"])
@pytest.mark.parametrize("format", ["json", "nl", "csv", "tsv"])
@pytest.mark.parametrize("batch_size", [1, 100])
def test_cli_extract(tmp_path, command, format, batch_size):
    path = str(tmp_path / "trees.db")
    rows = [
        {"id": 1, "species": "oak", "region": "west", "height": 1.5},
        {"id": 2, "species": "ash", "region": "west", "height": 2.5},
        {"id": 3, "species": "oak", "region": "east", "height": 3.5},
    ]
    if format == "json":
        input = json.dumps(rows)
    elif format == "nl":
        input = "\n".join(json.dumps(row) for row in rows)
    else:
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=list(rows[0]),
            delimiter="\t" if format == "tsv" else ",",
        )
        writer.writeheader()
        writer.writerows(rows)
        input = output.getvalue()
    args = [
        command,
        path,
        "trees",
        "-",
        "--pk",
        "id",
        "--extract",
        "species",
        "--extract",
        "region",
        "--batch-size",
        str(batch_size),
    ]
    if format != "json":
        args.append("--" + format)
    result = CliRunner().invoke(cli.cli, args, input=input)
    assert result.exit_code == 0, result.output
    db = Database(path)
    assert list(db["trees"].rows) == [
        {"id": 1, "species": 1, "region": 1, "height": 1.5},
        {"id": 2, "species": 2, "region": 1, "height": 2.5},
        {"id": 3, "species": 1, "region": 2, "height": 3.5},
    ]
    assert list(db["species"].rows) == [
        {"id": 1, "value": "oak"},
        {"id": 2, "value": "ash"},
    ]
    assert list(db["region"].rows) == [
        {"id": 1, "value": "west"},
        {"id": 2, "value": "east"},
    ]
    assert db["trees"].columns_dict == {
        "id": int,
        "species": int,
        "region": int,
        "height": float,
    }
    assert {
        (fk.column, fk.other_table, fk.other_column) for fk in db["trees"].foreign_keys
    } == {
        ("species", "species", "id"),
        ("region", "region", "id"),
    }
    assert list(db.query("pragma foreign_key_check")) == []


@pytest.mark.parametrize("command", ["insert", "upsert"])
def test_cli_extract_code(tmp_path, command):
    path = str(tmp_path / "code.db")
    result = CliRunner().invoke(
        cli.cli,
        [
            command,
            path,
            "trees",
            "--code",
            'rows = [{"id": 1, "species": "oak"}]',
            "--pk",
            "id",
            "--extract",
            "species",
        ],
    )
    assert result.exit_code == 0, result.output
    db = Database(path)
    assert list(db["trees"].rows) == [{"id": 1, "species": 1}]
    assert list(db["species"].rows) == [{"id": 1, "value": "oak"}]


@pytest.mark.parametrize("command", ["insert", "upsert"])
@pytest.mark.parametrize(
    "extra_args",
    [[], ["--strict"], ["--no-detect-types"], ["--type", "species", "TEXT"]],
)
def test_cli_extract_csv_types(tmp_path, command, extra_args):
    path = str(tmp_path / "types.db")
    result = CliRunner().invoke(
        cli.cli,
        [command, path, "trees", "-", "--pk", "id", "--csv", "--extract", "species"]
        + extra_args,
        input="id,species,height\n1,oak,1.5\n",
    )
    assert result.exit_code == 0, result.output
    db = Database(path)
    assert db["trees"].columns_dict["species"] is int
    assert list(db["trees"].rows)[0]["species"] == 1
    assert list(db.query("pragma foreign_key_check")) == []


@pytest.mark.parametrize("command", ["insert", "upsert"])
def test_cli_extract_nulls(tmp_path, command):
    path = str(tmp_path / "nulls.db")
    result = CliRunner().invoke(
        cli.cli,
        [command, path, "trees", "-", "--pk", "id", "--extract", "species"],
        input=json.dumps(
            [
                {"id": 1, "species": None},
                {"id": 2, "species": "oak"},
                {"id": 3, "species": None},
            ]
        ),
    )
    assert result.exit_code == 0, result.output
    db = Database(path)
    assert list(db["species"].rows) == [{"id": 1, "value": "oak"}]
    assert list(db["trees"].rows) == [
        {"id": 1, "species": None},
        {"id": 2, "species": 1},
        {"id": 3, "species": None},
    ]


def test_cli_extract_upsert(tmp_path):
    path = str(tmp_path / "upsert.db")
    for command, rows in [
        ("insert", [{"id": 1, "species": "oak"}, {"id": 2, "species": "ash"}]),
        ("upsert", [{"id": 1, "species": "ash"}, {"id": 3, "species": "oak"}]),
    ]:
        result = CliRunner().invoke(
            cli.cli,
            [command, path, "trees", "-", "--pk", "id", "--extract", "species"],
            input=json.dumps(rows),
        )
        assert result.exit_code == 0, result.output
    db = Database(path)
    assert list(db["species"].rows) == [
        {"id": 1, "value": "oak"},
        {"id": 2, "value": "ash"},
    ]
    assert list(db["trees"].rows) == [
        {"id": 1, "species": 2},
        {"id": 2, "species": 2},
        {"id": 3, "species": 1},
    ]


@pytest.mark.parametrize("column", ['tree "species"', "品種", ""])
def test_cli_extract_identifiers(tmp_path, column):
    path = str(tmp_path / "identifiers.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", path, "trees", "-", "--extract", column],
        input=json.dumps({column: "oak"}),
    )
    assert result.exit_code == 0, result.output
    db = Database(path)
    assert list(db["trees"].rows) == [{column: 1}]
    assert list(db[column].rows) == [{"id": 1, "value": "oak"}]


@pytest.mark.parametrize("command", ["insert", "upsert"])
def test_cli_extract_errors(tmp_path, command):
    path = str(tmp_path / "invalid.db")
    result = CliRunner().invoke(
        cli.cli,
        [command, path, "trees", "-", "--pk", "id", "--extract", "species"],
        input="invalid JSON",
    )
    assert result.exit_code == 1
    assert "Invalid JSON" in result.output
    assert Database(path).table_names() == []


@pytest.mark.parametrize("command", ["insert", "upsert"])
def test_cli_extract_requires_column(tmp_path, command):
    path = tmp_path / "missing.db"
    result = CliRunner().invoke(
        cli.cli, [command, str(path), "trees", "-", "--extract"], input="{}"
    )
    assert result.exit_code == 2
    assert "requires an argument" in result.output
    assert not path.exists()


def test_cli_extract_existing_schema(tmp_path):
    path = str(tmp_path / "existing.db")
    db = Database(path)
    db["trees"].create({"id": int, "species": int}, pk="id")
    schema = db["trees"].schema
    result = CliRunner().invoke(
        cli.cli,
        ["insert", path, "trees", "-", "--extract", "species"],
        input='{"id": 1, "species": "oak"}',
    )
    assert result.exit_code == 0, result.output
    assert db["trees"].schema == schema
    assert db["trees"].foreign_keys == []
    assert list(db["trees"].rows) == [{"id": 1, "species": 1}]
    assert list(db["species"].rows) == [{"id": 1, "value": "oak"}]


@pytest.mark.parametrize("command", ["insert", "upsert"])
def test_cli_no_extract_default(tmp_path, command):
    path = str(tmp_path / "default.db")
    result = CliRunner().invoke(
        cli.cli,
        [command, path, "trees", "-", "--pk", "id"],
        input='[{"id": 1, "species": "oak"}, {"id": 2, "species": "oak"}]',
    )
    assert result.exit_code == 0, result.output
    db = Database(path)
    assert db.table_names() == ["trees"]
    assert list(db["trees"].rows) == [
        {"id": 1, "species": "oak"},
        {"id": 2, "species": "oak"},
    ]
    assert db["trees"].foreign_keys == []
