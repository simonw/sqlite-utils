from sqlite_utils import recipes
from sqlite_utils.utils import sqlite3
import json
import pytest


@pytest.fixture
def dates_db(fresh_db):
    fresh_db["example"].insert_all(
        [
            {"id": 1, "dt": "5th October 2019 12:04"},
            {"id": 2, "dt": "6th October 2019 00:05:06"},
            {"id": 3, "dt": ""},
            {"id": 4, "dt": None},
        ],
        pk="id",
    )
    return fresh_db


def test_parsedate(dates_db):
    dates_db["example"].convert("dt", recipes.parsedate)
    assert list(dates_db["example"].rows) == [
        {"id": 1, "dt": "2019-10-05"},
        {"id": 2, "dt": "2019-10-06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ]


def test_parsedatetime(dates_db):
    dates_db["example"].convert("dt", recipes.parsedatetime)
    assert list(dates_db["example"].rows) == [
        {"id": 1, "dt": "2019-10-05T12:04:00"},
        {"id": 2, "dt": "2019-10-06T00:05:06"},
        {"id": 3, "dt": ""},
        {"id": 4, "dt": None},
    ]


@pytest.mark.parametrize(
    "recipe,kwargs,expected",
    (
        ("parsedate", {}, "2005-03-04"),
        ("parsedate", {"dayfirst": True}, "2005-04-03"),
        ("parsedatetime", {}, "2005-03-04T00:00:00"),
        ("parsedatetime", {"dayfirst": True}, "2005-04-03T00:00:00"),
    ),
)
def test_dayfirst_yearfirst(fresh_db, recipe, kwargs, expected):
    fresh_db["example"].insert_all(
        [
            {"id": 1, "dt": "03/04/05"},
        ],
        pk="id",
    )
    fresh_db["example"].convert(
        "dt", lambda value: getattr(recipes, recipe)(value, **kwargs)
    )
    assert list(fresh_db["example"].rows) == [
        {"id": 1, "dt": expected},
    ]


@pytest.mark.parametrize("fn", ("parsedate", "parsedatetime"))
@pytest.mark.parametrize("errors", (None, recipes.SET_NULL, recipes.IGNORE))
def test_dateparse_errors(fresh_db, fn, errors):
    fresh_db["example"].insert_all(
        [
            {"id": 1, "dt": "invalid"},
        ],
        pk="id",
    )
    if errors is None:
        # Should raise an error
        with pytest.raises(sqlite3.OperationalError):
            fresh_db["example"].convert("dt", lambda value: getattr(recipes, fn)(value))
    else:
        fresh_db["example"].convert(
            "dt", lambda value: getattr(recipes, fn)(value, errors=errors)
        )
        rows = list(fresh_db["example"].rows)
        expected = [{"id": 1, "dt": None if errors is recipes.SET_NULL else "invalid"}]
        assert rows == expected


@pytest.mark.parametrize("delimiter", [None, ";", "-"])
def test_jsonsplit(fresh_db, delimiter):
    fresh_db["example"].insert_all(
        [
            {"id": 1, "tags": (delimiter or ",").join(["foo", "bar"])},
            {"id": 2, "tags": (delimiter or ",").join(["bar", "baz"])},
        ],
        pk="id",
    )
    fn = recipes.jsonsplit
    if delimiter is not None:

        def fn(value):
            return recipes.jsonsplit(value, delimiter=delimiter)

    fresh_db["example"].convert("tags", fn)
    assert list(fresh_db["example"].rows) == [
        {"id": 1, "tags": '["foo", "bar"]'},
        {"id": 2, "tags": '["bar", "baz"]'},
    ]


@pytest.mark.parametrize(
    "type,expected",
    (
        (None, ["1", "2", "3"]),
        (float, [1.0, 2.0, 3.0]),
        (int, [1, 2, 3]),
    ),
)
def test_jsonsplit_type(fresh_db, type, expected):
    fresh_db["example"].insert_all(
        [
            {"id": 1, "records": "1,2,3"},
        ],
        pk="id",
    )
    fn = recipes.jsonsplit
    if type is not None:

        def fn(value):
            return recipes.jsonsplit(value, type=type)

    fresh_db["example"].convert("records", fn)
    assert json.loads(fresh_db["example"].get(1)["records"]) == expected
