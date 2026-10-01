import csv
import io
import sqlite3

import pytest

from sqlite_utils.export import rows_to_csv_file


def _cursor():
    db = sqlite3.connect(":memory:")
    db.execute("create table creatures (id integer, name text)")
    db.executemany(
        "insert into creatures (id, name) values (?, ?)",
        ((1, "Cleo"), (2, "Cardi, Jr.")),
    )
    return db, db.execute("select id, name from creatures order by id")


def test_rows_to_csv_file():
    db, cursor = _cursor()
    output = io.StringIO(newline="")
    rows_to_csv_file(cursor, output, lineterminator="\n")
    assert output.getvalue() == 'id,name\n1,Cleo\n2,"Cardi, Jr."\n'
    db.close()


def test_rows_to_csv_file_without_header():
    db, cursor = _cursor()
    output = io.StringIO(newline="")
    rows_to_csv_file(cursor, output, header=False, lineterminator="\n")
    assert output.getvalue() == '1,Cleo\n2,"Cardi, Jr."\n'
    db.close()


def test_rows_to_csv_file_tsv():
    db, cursor = _cursor()
    output = io.StringIO(newline="")
    rows_to_csv_file(cursor, output, dialect="excel-tab", lineterminator="\n")
    assert output.getvalue() == "id\tname\n1\tCleo\n2\tCardi, Jr.\n"
    db.close()


def test_rows_to_csv_file_accepts_dialect_class():
    class PipeDialect(csv.excel):
        delimiter = "|"

    db, cursor = _cursor()
    output = io.StringIO(newline="")
    rows_to_csv_file(cursor, output, dialect=PipeDialect, lineterminator="\n")
    assert output.getvalue() == "id|name\n1|Cleo\n2|Cardi, Jr.\n"
    db.close()


def test_rows_to_csv_file_with_mapping_row_factory():
    db = sqlite3.connect(":memory:")
    db.execute("create table creatures (id integer, name text)")
    db.executemany(
        "insert into creatures (id, name) values (?, ?)",
        ((1, "Cleo"), (2, "Cardi, Jr.")),
    )

    def dict_factory(cursor, row):
        return {column[0]: value for column, value in zip(cursor.description, row)}

    db.row_factory = dict_factory
    cursor = db.execute("select id, name from creatures order by id")
    output = io.StringIO(newline="")
    rows_to_csv_file(cursor, output, lineterminator="\n")
    assert output.getvalue() == 'id,name\n1,Cleo\n2,"Cardi, Jr."\n'
    db.close()


def test_rows_to_csv_file_rejects_duplicate_mapping_columns():
    db = sqlite3.connect(":memory:")
    db.execute("create table creatures (id integer, name text)")
    db.execute("insert into creatures (id, name) values (1, 'Cleo')")

    def dict_factory(cursor, row):
        return {column[0]: value for column, value in zip(cursor.description, row)}

    db.row_factory = dict_factory
    cursor = db.execute("select id as value, name as value from creatures")
    output = io.StringIO(newline="")
    with pytest.raises(
        ValueError, match="Mapping rows cannot represent duplicate column names"
    ):
        rows_to_csv_file(cursor, output, lineterminator="\n")
    assert output.getvalue() == ""
    db.close()


@pytest.mark.parametrize("header", (True, False))
def test_rows_to_csv_file_requires_result_columns(header):
    db = sqlite3.connect(":memory:")
    cursor = db.execute("create table creatures (id integer)")
    output = io.StringIO(newline="")
    with pytest.raises(ValueError, match="Cursor does not have result columns"):
        rows_to_csv_file(cursor, output, header=header)
    assert output.getvalue() == ""
    db.close()
