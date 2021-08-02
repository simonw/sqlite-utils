from sqlite_utils.db import BadMultiValues
import pytest


@pytest.mark.parametrize(
    "columns,fn,expected",
    (
        (
            "title",
            lambda value: value.upper(),
            {"title": "MIXED CASE", "abstract": "Abstract"},
        ),
        (
            ["title", "abstract"],
            lambda value: value.upper(),
            {"title": "MIXED CASE", "abstract": "ABSTRACT"},
        ),
    ),
)
def test_convert(fresh_db, columns, fn, expected):
    table = fresh_db["table"]
    table.insert({"title": "Mixed Case", "abstract": "Abstract"})
    table.convert(columns, fn)
    assert list(table.rows) == [expected]


@pytest.mark.parametrize(
    "where,where_args", (("id > 1", None), ("id > :id", {"id": 1}), ("id > ?", [1]))
)
def test_convert_where(fresh_db, where, where_args):
    table = fresh_db["table"]
    table.insert_all(
        [
            {"id": 1, "title": "One"},
            {"id": 2, "title": "Two"},
        ],
        pk="id",
    )
    table.convert(
        "title", lambda value: value.upper(), where=where, where_args=where_args
    )
    assert list(table.rows) == [{"id": 1, "title": "One"}, {"id": 2, "title": "TWO"}]


@pytest.mark.parametrize(
    "drop,expected",
    (
        (False, {"title": "Mixed Case", "other": "MIXED CASE"}),
        (True, {"other": "MIXED CASE"}),
    ),
)
def test_convert_output(fresh_db, drop, expected):
    table = fresh_db["table"]
    table.insert({"title": "Mixed Case"})
    table.convert("title", lambda v: v.upper(), output="other", drop=drop)
    assert list(table.rows) == [expected]


def test_convert_output_multiple_column_error(fresh_db):
    table = fresh_db["table"]
    with pytest.raises(AssertionError) as excinfo:
        table.convert(["title", "other"], lambda v: v, output="out")
        assert "output= can only be used with a single column" in str(excinfo.value)


@pytest.mark.parametrize(
    "type,expected",
    (
        (int, {"other": 123}),
        (float, {"other": 123.0}),
    ),
)
def test_convert_output_type(fresh_db, type, expected):
    table = fresh_db["table"]
    table.insert({"number": "123"})
    table.convert("number", lambda v: v, output="other", output_type=type, drop=True)
    assert list(table.rows) == [expected]


def test_convert_multi(fresh_db):
    table = fresh_db["table"]
    table.insert({"title": "Mixed Case"})
    table.convert(
        "title", lambda v: {"upper": v.upper(), "lower": v.lower()}, multi=True
    )
    assert list(table.rows) == [
        {"title": "Mixed Case", "upper": "MIXED CASE", "lower": "mixed case"}
    ]


def test_convert_multi_where(fresh_db):
    table = fresh_db["table"]
    table.insert_all(
        [
            {"id": 1, "title": "One"},
            {"id": 2, "title": "Two"},
        ],
        pk="id",
    )
    table.convert(
        "title",
        lambda v: {"upper": v.upper(), "lower": v.lower()},
        multi=True,
        where="id > ?",
        where_args=[1],
    )
    assert list(table.rows) == [
        {"id": 1, "lower": None, "title": "One", "upper": None},
        {"id": 2, "lower": "two", "title": "Two", "upper": "TWO"},
    ]


def test_convert_multi_exception(fresh_db):
    table = fresh_db["table"]
    table.insert({"title": "Mixed Case"})
    with pytest.raises(BadMultiValues):
        table.convert("title", lambda v: v.upper(), multi=True)
