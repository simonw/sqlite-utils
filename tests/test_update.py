import pytest


def test_update_rowid_table(fresh_db):
    table = fresh_db["table"]
    rowid = table.insert({"foo": "bar"}).last_pk
    table.update(rowid, {"foo": "baz"})
    assert [{"foo": "baz"}] == list(table.rows)


def test_update_pk_table(fresh_db):
    table = fresh_db["table"]
    pk = table.insert({"foo": "bar", "id": 5}, pk="id").last_pk
    assert 5 == pk
    table.update(pk, {"foo": "baz"})
    assert [{"id": 5, "foo": "baz"}] == list(table.rows)


def test_update_compound_pk_table(fresh_db):
    table = fresh_db["table"]
    pk = table.insert({"id1": 5, "id2": 3, "v": 1}, pk=("id1", "id2")).last_pk
    assert (5, 3) == pk
    table.update(pk, {"v": 2})
    assert [{"id1": 5, "id2": 3, "v": 2}] == list(table.rows)
