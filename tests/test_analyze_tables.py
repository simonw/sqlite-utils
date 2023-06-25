from sqlite_utils.db import Database, ColumnDetails
from sqlite_utils import cli
from click.testing import CliRunner
import pytest
import sqlite3


@pytest.fixture
def db_to_analyze(fresh_db):
    stuff = fresh_db["stuff"]
    stuff.insert_all(
        [
            {"id": 1, "owner": "Terryterryterry", "size": 5},
            {"id": 2, "owner": "Joan", "size": 4},
            {"id": 3, "owner": "Kumar", "size": 5},
            {"id": 4, "owner": "Anne", "size": 5},
            {"id": 5, "owner": "Terryterryterry", "size": 5},
            {"id": 6, "owner": "Joan", "size": 4},
            {"id": 7, "owner": "Kumar", "size": 5},
            {"id": 8, "owner": "Joan", "size": 4},
        ],
        pk="id",
    )
    return fresh_db


@pytest.fixture
def big_db_to_analyze_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = Database(path)
    categories = {
        "A": 40,
        "B": 30,
        "C": 20,
        "D": 10,
    }
    to_insert = []
    for category, count in categories.items():
        for _ in range(count):
            to_insert.append(
                {
                    "category": category,
                    "all_null": None,
                }
            )
    db["stuff"].insert_all(to_insert)
    return path


@pytest.mark.parametrize(
    "column,extra_kwargs,expected",
    [
        (
            "id",
            {},
            ColumnDetails(
                table="stuff",
                column="id",
                total_rows=8,
                num_null=0,
                num_blank=0,
                num_distinct=8,
                most_common=None,
                least_common=None,
            ),
        ),
        (
            "owner",
            {},
            ColumnDetails(
                table="stuff",
                column="owner",
                total_rows=8,
                num_null=0,
                num_blank=0,
                num_distinct=4,
                most_common=[("Joan", 3), ("Kumar", 2)],
                least_common=[("Anne", 1), ("Terry...", 2)],
            ),
        ),
        (
            "size",
            {},
            ColumnDetails(
                table="stuff",
                column="size",
                total_rows=8,
                num_null=0,
                num_blank=0,
                num_distinct=2,
                most_common=[(5, 5), (4, 3)],
                least_common=None,
            ),
        ),
        (
            "owner",
            {"most_common": False},
            ColumnDetails(
                table="stuff",
                column="owner",
                total_rows=8,
                num_null=0,
                num_blank=0,
                num_distinct=4,
                most_common=None,
                least_common=[("Anne", 1), ("Terry...", 2)],
            ),
        ),
        (
            "owner",
            {"least_common": False},
            ColumnDetails(
                table="stuff",
                column="owner",
                total_rows=8,
                num_null=0,
                num_blank=0,
                num_distinct=4,
                most_common=[("Joan", 3), ("Kumar", 2)],
                least_common=None,
            ),
        ),
    ],
)
def test_analyze_column(db_to_analyze, column, extra_kwargs, expected):
    assert (
        db_to_analyze["stuff"].analyze_column(
            column, common_limit=2, value_truncate=5, **extra_kwargs
        )
        == expected
    )


@pytest.fixture
def db_to_analyze_path(db_to_analyze, tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite3.connect(path)
    sql = "\n".join(db_to_analyze.iterdump())
    db.executescript(sql)
    return path


def test_analyze_table(db_to_analyze_path):
    result = CliRunner().invoke(cli.cli, ["analyze-tables", db_to_analyze_path])
    assert (
        result.output.strip()
        == (
            """
stuff.id: (1/3)

  Total rows: 8
  Null rows: 0
  Blank rows: 0

  Distinct values: 8

stuff.owner: (2/3)

  Total rows: 8
  Null rows: 0
  Blank rows: 0

  Distinct values: 4

  Most common:
    3: Joan
    2: Terryterryterry
    2: Kumar
    1: Anne

stuff.size: (3/3)

  Total rows: 8
  Null rows: 0
  Blank rows: 0

  Distinct values: 2

  Most common:
    5: 5
    3: 4"""
        ).strip()
    )


def test_analyze_table_save(db_to_analyze_path):
    result = CliRunner().invoke(
        cli.cli, ["analyze-tables", db_to_analyze_path, "--save"]
    )
    assert result.exit_code == 0
    rows = list(Database(db_to_analyze_path)["_analyze_tables_"].rows)
    assert rows == [
        {
            "table": "stuff",
            "column": "id",
            "total_rows": 8,
            "num_null": 0,
            "num_blank": 0,
            "num_distinct": 8,
            "most_common": None,
            "least_common": None,
        },
        {
            "table": "stuff",
            "column": "owner",
            "total_rows": 8,
            "num_null": 0,
            "num_blank": 0,
            "num_distinct": 4,
            "most_common": '[["Joan", 3], ["Terryterryterry", 2], ["Kumar", 2], ["Anne", 1]]',
            "least_common": None,
        },
        {
            "table": "stuff",
            "column": "size",
            "total_rows": 8,
            "num_null": 0,
            "num_blank": 0,
            "num_distinct": 2,
            "most_common": "[[5, 5], [4, 3]]",
            "least_common": None,
        },
    ]


@pytest.mark.parametrize(
    "no_most,no_least",
    (
        (False, False),
        (True, False),
        (False, True),
        (True, True),
    ),
)
def test_analyze_table_save_no_most_no_least_options(
    no_most, no_least, big_db_to_analyze_path
):
    args = [
        "analyze-tables",
        big_db_to_analyze_path,
        "--save",
        "--common-limit",
        "2",
        "--column",
        "category",
    ]
    if no_most:
        args.append("--no-most")
    if no_least:
        args.append("--no-least")
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 0
    rows = list(Database(big_db_to_analyze_path)["_analyze_tables_"].rows)
    expected = {
        "table": "stuff",
        "column": "category",
        "total_rows": 100,
        "num_null": 0,
        "num_blank": 0,
        "num_distinct": 4,
        "most_common": None,
        "least_common": None,
    }
    if not no_most:
        expected["most_common"] = '[["A", 40], ["B", 30]]'
    if not no_least:
        expected["least_common"] = '[["D", 10], ["C", 20]]'

    assert rows == [expected]


def test_analyze_table_column_all_nulls(big_db_to_analyze_path):
    result = CliRunner().invoke(
        cli.cli,
        ["analyze-tables", big_db_to_analyze_path, "stuff", "--column", "all_null"],
    )
    assert result.exit_code == 0
    assert result.output == (
        "stuff.all_null: (1/1)\n\n  Total rows: 100\n"
        "  Null rows: 100\n"
        "  Blank rows: 0\n"
        "\n"
        "  Distinct values: 0\n\n"
    )


@pytest.mark.parametrize(
    "args,expected_error",
    (
        (["-c", "bad_column"], "These columns were not found: bad_column\n"),
        (["one", "-c", "age"], "These columns were not found: age\n"),
        (["two", "-c", "age"], None),
        (
            ["one", "-c", "age", "--column", "bad"],
            "These columns were not found: age, bad\n",
        ),
    ),
)
def test_analyze_table_validate_columns(tmpdir, args, expected_error):
    path = str(tmpdir / "test_validate_columns.db")
    db = Database(path)
    db["one"].insert(
        {
            "id": 1,
            "name": "one",
        }
    )
    db["two"].insert(
        {
            "id": 1,
            "age": 5,
        }
    )
    result = CliRunner().invoke(
        cli.cli,
        ["analyze-tables", path] + args,
        catch_exceptions=False,
    )
    assert result.exit_code == (1 if expected_error else 0)
    if expected_error:
        assert expected_error in result.output
