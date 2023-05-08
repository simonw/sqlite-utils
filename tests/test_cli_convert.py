from click.testing import CliRunner
from sqlite_utils import cli
import sqlite_utils
import json
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
        # Multiple lines are supported:
        "v = value.replace('October', 'Spooktober')\nreturn v",
        # Can also define a convert() function
        "def convert(value): return value.replace('October', 'Spooktober')",
        # ... with imports
        "import re\n\ndef convert(value): return value.replace('October', 'Spooktober')",
    ],
)
def test_convert_code(fresh_db_and_path, code):
    db, db_path = fresh_db_and_path
    db["t"].insert({"text": "October"})
    result = CliRunner().invoke(
        cli.cli, ["convert", db_path, "t", "text", code], catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output
    value = list(db["t"].rows)[0]["text"]
    assert value == "Spooktober"


@pytest.mark.parametrize(
    "bad_code",
    (
        "def foo(value)",
        "$",
    ),
)
def test_convert_code_errors(fresh_db_and_path, bad_code):
    db, db_path = fresh_db_and_path
    db["t"].insert({"text": "October"})
    result = CliRunner().invoke(
        cli.cli, ["convert", db_path, "t", "text", bad_code], catch_exceptions=False
    )
    assert 1 == result.exit_code
    assert result.output == "Error: Could not compile code\n"


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


def test_convert_import_nested(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["example"].insert({"xml": '<item name="Cleo" />'})
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
            "xml",
            'xml.etree.ElementTree.fromstring(value).attrib["name"]',
            "--import",
            "xml.etree.ElementTree",
        ],
    )
    assert 0 == result.exit_code, result.output
    assert [
        {"xml": "Cleo"},
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
        "None\n\n"
        "Would affect 4 rows"
    )
    # But it should not have actually modified the table data
    assert list(db["example"].rows) == [
        {"id": 1, "dt": "5th October 2019 12:04"},
        {"id": 2, "dt": "6th October 2019 00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ]
    # Test with a where clause too
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
            "--where",
            "id = :id",
            "-p",
            "id",
            "4",
        ],
    )
    assert result.exit_code == 0
    assert result.output.strip().split("\n")[-1] == "Would affect 1 row"


def test_convert_multi_dryrun(test_db_and_path):
    db_path = test_db_and_path[1]
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
            "dt",
            "{'foo': 'bar', 'baz': 1}",
            "--dry-run",
            "--multi",
        ],
    )
    assert result.exit_code == 0
    assert result.output.strip() == (
        "5th October 2019 12:04\n"
        " --- becomes:\n"
        '{"foo": "bar", "baz": 1}\n'
        "\n"
        "6th October 2019 00:05:06\n"
        " --- becomes:\n"
        '{"foo": "bar", "baz": 1}\n'
        "\n"
        "\n"
        " --- becomes:\n"
        "\n"
        "\n"
        "None\n"
        " --- becomes:\n"
        "None\n"
        "\n"
        "Would affect 4 rows"
    )


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


@pytest.mark.parametrize("delimiter", [None, ";", "-"])
def test_recipe_jsonsplit(tmpdir, delimiter):
    db_path = str(pathlib.Path(tmpdir) / "data.db")
    db = sqlite_utils.Database(db_path)
    db["example"].insert_all(
        [
            {"id": 1, "tags": (delimiter or ",").join(["foo", "bar"])},
            {"id": 2, "tags": (delimiter or ",").join(["bar", "baz"])},
        ],
        pk="id",
    )
    code = "r.jsonsplit(value)"
    if delimiter:
        code = 'recipes.jsonsplit(value, delimiter="{}")'.format(delimiter)
    args = ["convert", db_path, "example", "tags", code]
    result = CliRunner().invoke(cli.cli, args)
    assert 0 == result.exit_code, result.output
    assert list(db["example"].rows) == [
        {"id": 1, "tags": '["foo", "bar"]'},
        {"id": 2, "tags": '["bar", "baz"]'},
    ]


@pytest.mark.parametrize(
    "type,expected_array",
    (
        (None, ["1", "2", "3"]),
        ("float", [1.0, 2.0, 3.0]),
        ("int", [1, 2, 3]),
    ),
)
def test_recipe_jsonsplit_type(fresh_db_and_path, type, expected_array):
    db, db_path = fresh_db_and_path
    db["example"].insert_all(
        [
            {"id": 1, "records": "1,2,3"},
        ],
        pk="id",
    )
    code = "r.jsonsplit(value)"
    if type:
        code = "recipes.jsonsplit(value, type={})".format(type)
    args = ["convert", db_path, "example", "records", code]
    result = CliRunner().invoke(cli.cli, args)
    assert 0 == result.exit_code, result.output
    assert json.loads(db["example"].get(1)["records"]) == expected_array


@pytest.mark.parametrize("drop", (True, False))
def test_recipe_jsonsplit_output(fresh_db_and_path, drop):
    db, db_path = fresh_db_and_path
    db["example"].insert_all(
        [
            {"id": 1, "records": "1,2,3"},
        ],
        pk="id",
    )
    code = "r.jsonsplit(value)"
    args = ["convert", db_path, "example", "records", code, "--output", "tags"]
    if drop:
        args += ["--drop"]
    result = CliRunner().invoke(cli.cli, args)
    assert 0 == result.exit_code, result.output
    expected = {
        "id": 1,
        "records": "1,2,3",
        "tags": '["1", "2", "3"]',
    }
    if drop:
        del expected["records"]
    assert db["example"].get(1) == expected


def test_cannot_use_drop_without_multi_or_output(fresh_db_and_path):
    args = ["convert", fresh_db_and_path[1], "example", "records", "value", "--drop"]
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 1, result.output
    assert "Error: --drop can only be used with --output or --multi" in result.output


def test_cannot_use_multi_with_more_than_one_column(fresh_db_and_path):
    args = [
        "convert",
        fresh_db_and_path[1],
        "example",
        "records",
        "othercol",
        "value",
        "--multi",
    ]
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 1, result.output
    assert "Error: Cannot use --multi with more than one column" in result.output


def test_multi_with_bad_function(test_db_and_path):
    args = [
        "convert",
        test_db_and_path[1],
        "example",
        "dt",
        "value.upper()",
        "--multi",
    ]
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 1, result.output
    assert "When using --multi code must return a Python dictionary" in result.output


def test_convert_where(test_db_and_path):
    db, db_path = test_db_and_path
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "example",
            "dt",
            "str(value).upper()",
            "--where",
            "id = :id",
            "-p",
            "id",
            2,
        ],
    )
    assert result.exit_code == 0, result.output
    assert list(db["example"].rows) == [
        {"id": 1, "dt": "5th October 2019 12:04"},
        {"id": 2, "dt": "6TH OCTOBER 2019 00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ]


def test_convert_where_multi(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["names"].insert_all(
        [{"id": 1, "name": "Cleo"}, {"id": 2, "name": "Bants"}], pk="id"
    )
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "names",
            "name",
            '{"upper": value.upper()}',
            "--where",
            "id = :id",
            "-p",
            "id",
            2,
            "--multi",
        ],
    )
    assert 0 == result.exit_code, result.output
    assert list(db["names"].rows) == [
        {"id": 1, "name": "Cleo", "upper": None},
        {"id": 2, "name": "Bants", "upper": "BANTS"},
    ]


def test_convert_code_standard_input(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["names"].insert_all([{"id": 1, "name": "Cleo"}], pk="id")
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "names",
            "name",
            "-",
        ],
        input="value.upper()",
    )
    assert 0 == result.exit_code, result.output
    assert list(db["names"].rows) == [
        {"id": 1, "name": "CLEO"},
    ]


def test_convert_hyphen_workaround(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["names"].insert_all([{"id": 1, "name": "Cleo"}], pk="id")
    result = CliRunner().invoke(
        cli.cli,
        ["convert", db_path, "names", "name", '"-"'],
    )
    assert 0 == result.exit_code, result.output
    assert list(db["names"].rows) == [
        {"id": 1, "name": "-"},
    ]


def test_convert_initialization_pattern(fresh_db_and_path):
    db, db_path = fresh_db_and_path
    db["names"].insert_all([{"id": 1, "name": "Cleo"}], pk="id")
    result = CliRunner().invoke(
        cli.cli,
        [
            "convert",
            db_path,
            "names",
            "name",
            "-",
        ],
        input="import random\nrandom.seed(1)\ndef convert(value):    return random.randint(0, 100)",
    )
    assert 0 == result.exit_code, result.output
    assert list(db["names"].rows) == [
        {"id": 1, "name": "17"},
    ]


@pytest.mark.parametrize(
    "no_skip_false,expected",
    (
        (True, 1),
        (False, 0),
    ),
)
def test_convert_no_skip_false(fresh_db_and_path, no_skip_false, expected):
    db, db_path = fresh_db_and_path
    args = [
        "convert",
        db_path,
        "t",
        "x",
        "-",
    ]
    if no_skip_false:
        args.append("--no-skip-false")
    db["t"].insert_all([{"x": 0}, {"x": 1}])
    assert db["t"].get(1)["x"] == 0
    assert db["t"].get(2)["x"] == 1
    result = CliRunner().invoke(cli.cli, args, input="value + 1")
    assert 0 == result.exit_code, result.output
    assert db["t"].get(1)["x"] == expected
    assert db["t"].get(2)["x"] == 2
