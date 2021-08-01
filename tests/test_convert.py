from click.testing import CliRunner
from sqlite_utils import cli
import sqlite_utils
import textwrap
import pathlib
import pytest


@pytest.fixture
def test_db_and_path(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["example"].insert_all(
        [
            {"id": 1, "dt": "5th October 2019 12:04"},
            {"id": 2, "dt": "6th October 2019 00:05:06"},
            {"id": 3, "dt": ""},
            {"id": 4, "dt": None},
        ],
        pk="id",
    )
    return db, db_path


@pytest.fixture
def fresh_db_and_path(tmpdir):
    db_path = str(pathlib.Path(tmpdir) / "data.db")
    db = sqlite_utils.Database(db_path)
    return db, db_path


@pytest.mark.parametrize(
    "code",
    [
        "return value.replace('October', 'Spooktober')",
        # Return is optional:
        "value.replace('October', 'Spooktober')",
    ],
)
def test_convert_single_line(test_db_and_path, code):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(cli.cli, ["convert", db_path, "example", "dt", code])
    assert 0 == result.exit_code, result.output
    assert [
        {"id": 1, "dt": "5th Spooktober 2019 12:04"},
        {"id": 2, "dt": "6th Spooktober 2019 00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ] == list(db["example"].rows)


def test_convert_multiple_lines(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
            "dt",
            "v = value.replace('October', 'Spooktober')\nreturn v.upper()",
        ],
    )
    assert 0 == result.exit_code, result.output
    assert [
        {"id": 1, "dt": "5TH SPOOKTOBER 2019 12:04"},
        {"id": 2, "dt": "6TH SPOOKTOBER 2019 00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ] == list(db["example"].rows)


def test_convert_import(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
            "dt",
            "return re.sub('O..', 'OXX', value)",
            "--import",
            "re",
        ],
    )
    assert 0 == result.exit_code, result.output
    assert [
        {"id": 1, "dt": "5th OXXober 2019 12:04"},
        {"id": 2, "dt": "6th OXXober 2019 00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ] == list(db["example"].rows)


def test_convert_dryrun(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
            "dt",
            "return re.sub('O..', 'OXX', value)",
            "--import",
            "re",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert result.output.strip() == (
        "5th October 2019 12:04\n"
        " --- becomes:\n"
        "5th OXXober 2019 12:04\n"
        "\n"
        "6th October 2019 00:05:06\n"
        " --- becomes:\n"
        "6th OXXober 2019 00:05:06\n"
        "\n"
        "\n"
        " --- becomes:\n"
        "\n"
        "\n"
        "None\n"
        " --- becomes:\n"
        "None"
    )
    # But it should not have actually modified the table data
    assert list(db["example"].rows) == [
        {"id": 1, "dt": "5th October 2019 12:04"},
        {"id": 2, "dt": "6th October 2019 00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ]


@pytest.mark.parametrize("drop", (True, False))
def test_convert_output_column(test_db_and_path, drop):
    db, db_path = test_db_and_path
    args = [
        "convert",
        db_path,
        "example",
        "dt",
        "value.replace('October', 'Spooktober')",
        "--output",
        "newcol",
    ]
    if drop:
        args += ["--drop"]
    result = CliRunner().invoke(cli.cli, args)
    assert 0 == result.exit_code, result.output
    expected = [
        {
            "id": 1,
            "dt": "5th October 2019 12:04",
            "newcol": "5th Spooktober 2019 12:04",
        },
        {
            "id": 2,
            "dt": "6th October 2019 00:05:06",
            "newcol": "6th Spooktober 2019 00:05:06",
        },
        {"id": 3, "dt": "", "newcol": ""},
        {"id": 4, "dt": None, "newcol": None},
    ]
    if drop:
        for row in expected:
            del row["dt"]
    assert list(db["example"].rows) == expected


@pytest.mark.parametrize(
    "output_type,expected",
    (
        ("text", [(1, "1"), (2, "2"), (3, "3"), (4, "4")]),
        ("float", [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]),
        ("integer", [(1, 1), (2, 2), (3, 3), (4, 4)]),
        (None, [(1, "1"), (2, "2"), (3, "3"), (4, "4")]),
    ),
)
def test_convert_output_column_output_type(test_db_and_path, output_type, expected):
    db, db_path = test_db_and_path
    args = [
        "convert",
        db_path,
        "example",
        "id",
        "value",
        "--output",
        "new_id",
    ]
    if output_type:
        args += ["--output-type", output_type]
    result = CliRunner().invoke(
        cli.cli,
        args,
    )
    assert 0 == result.exit_code, result.output
    assert expected == list(db.execute("select id, new_id from example"))


@pytest.mark.parametrize(
    "options,expected_error",
    [
        (
            [
                "dt",
                "id",
                "value.replace('October', 'Spooktober')",
                "--output",
                "newcol",
            ],
            "Cannot use --output with more than one column",
        ),
        (
            [
                "dt",
                "value.replace('October', 'Spooktober')",
                "--output",
                "newcol",
                "--output-type",
                "invalid",
            ],
            "Error: Invalid value for '--output-type'",
        ),
        (
            [
                "value.replace('October', 'Spooktober')",
            ],
            "Missing argument 'COLUMNS...'",
        ),
    ],
)
def test_convert_output_error(test_db_and_path, options, expected_error):
    db_path = test_db_and_path[1]
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
        ]
        + options,
    )
    assert result.exit_code != 0
    assert expected_error in result.output


@pytest.mark.parametrize("drop", (True, False))
def test_convert_multi(fresh_db_and_path, drop):
    db, db_path = fresh_db_and_path
    db["creatures"].insert_all(
        [
            {"id": 1, "name": "Simon"},
            {"id": 2, "name": "Cleo"},
        ],
        pk="id",
    )
    args = [
        "convert",
        db_path,
        "creatures",
        "name",
        "--multi",
        '{"upper": value.upper(), "lower": value.lower()}',
    ]
    if drop:
        args += ["--drop"]
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 0, result.output
    expected = [
        {"id": 1, "name": "Simon", "upper": "SIMON", "lower": "simon"},
        {"id": 2, "name": "Cleo", "upper": "CLEO", "lower": "cleo"},
    ]
    if drop:
        for row in expected:
            del row["name"]
    assert list(db["creatures"].rows) == expected


def test_convert_multi_complex_column_types(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["rows"].insert_all(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
        ],
        pk="id",
    )
    code = textwrap.dedent(
        """
    if value == 1:
        return {"is_str": "", "is_float": 1.2, "is_int": None}
    elif value == 2:
        return {"is_float": 1, "is_int": 12}
    elif value == 3:
        return {"is_bytes": b"blah"}
    """
    )
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "rows",
            "id",
            "--multi",
            code,
        ],
    )
    assert result.exit_code == 0, result.output
    assert list(db["rows"].rows) == [
        {"id": 1, "is_str": "", "is_float": 1.2, "is_int": None, "is_bytes": None},
        {"id": 2, "is_str": None, "is_float": 1.0, "is_int": 12, "is_bytes": None},
        {
            "id": 3,
            "is_str": None,
            "is_float": None,
            "is_int": None,
            "is_bytes": b"blah",
        },
        {"id": 4, "is_str": None, "is_float": None, "is_int": None, "is_bytes": None},
    ]
    assert db["rows"].schema == (
        "CREATE TABLE [rows] (\n"
        "   [id] INTEGER PRIMARY KEY\n"
        ", [is_str] TEXT, [is_float] FLOAT, [is_int] INTEGER, [is_bytes] BLOB)"
    )
