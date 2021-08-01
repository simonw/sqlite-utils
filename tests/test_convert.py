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
