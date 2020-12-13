from sqlite_utils.db import Database, ForeignKey, ColumnDetails
from sqlite_utils import cli
from sqlite_utils.utils import OperationalError
from click.testing import CliRunner
import pytest
import sqlite3
import textwrap


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


@pytest.mark.parametrize(
    "column,expected",
    [
        (
            "id",
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
    ],
)
def test_analyze_column(db_to_analyze, column, expected):
    assert (
        db_to_analyze["stuff"].analyze_column(column, common_limit=2, value_truncate=5)
        == expected
    )


@pytest.fixture
def db_to_analyze_path(db_to_analyze, tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite3.connect(path)
    db.executescript("\n".join(db_to_analyze.conn.iterdump()))
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
