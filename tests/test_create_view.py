import pytest
from sqlite_utils.utils import OperationalError


def test_create_view(fresh_db):
    fresh_db.create_view("bar", "select 1 + 1")
    rows = fresh_db.execute("select * from bar").fetchall()
    assert [(2,)] == rows


def test_create_view_error(fresh_db):
    fresh_db.create_view("bar", "select 1 + 1")
    with pytest.raises(OperationalError):
        fresh_db.create_view("bar", "select 1 + 2")


def test_create_view_only_arrow_one_param(fresh_db):
    with pytest.raises(AssertionError):
        fresh_db.create_view("bar", "select 1 + 2", ignore=True, replace=True)


def test_create_view_ignore(fresh_db):
    fresh_db.create_view("bar", "select 1 + 1").create_view(
        "bar", "select 1 + 2", ignore=True
    )
    rows = fresh_db.execute("select * from bar").fetchall()
    assert [(2,)] == rows


def test_create_view_replace(fresh_db):
    fresh_db.create_view("bar", "select 1 + 1").create_view(
        "bar", "select 1 + 2", replace=True
    )
    rows = fresh_db.execute("select * from bar").fetchall()
    assert [(3,)] == rows


def test_create_view_replace_with_same_does_nothing(fresh_db):
    fresh_db.create_view("bar", "select 1 + 1")
    initial_version = fresh_db.execute("PRAGMA schema_version").fetchone()[0]
    fresh_db.create_view("bar", "select 1 + 1", replace=True)
    after_version = fresh_db.execute("PRAGMA schema_version").fetchone()[0]
    assert after_version == initial_version
