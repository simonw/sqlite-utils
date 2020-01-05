from sqlite_utils.db import PrimaryKeyRequired
import pytest


def test_upsert(fresh_db):
    table = fresh_db["table"]
    table.insert({"id": 1, "name": "Cleo"}, pk="id")
    table.upsert({"id": 1, "age": 5}, pk="id", alter=True)
    assert [{"id": 1, "name": "Cleo", "age": 5}] == list(table.rows)


def test_upsert_all(fresh_db):
    table = fresh_db["table"]
    table.upsert_all([{"id": 1, "name": "Cleo"}, {"id": 2, "name": "Nixie"}], pk="id")
    table.upsert_all([{"id": 1, "age": 5}, {"id": 2, "age": 5}], pk="id", alter=True)
    assert [
        {"id": 1, "name": "Cleo", "age": 5},
        {"id": 2, "name": "Nixie", "age": 5},
    ] == list(table.rows)
    assert 2 == table.last_pk


def test_upsert_error_if_no_pk(fresh_db):
    table = fresh_db["table"]
    with pytest.raises(PrimaryKeyRequired):
        table.upsert_all([{"id": 1, "name": "Cleo"}])
    with pytest.raises(PrimaryKeyRequired):
        table.upsert({"id": 1, "name": "Cleo"})


def test_upsert_compound_primary_key(fresh_db):
    table = fresh_db["table"]
    table.upsert_all(
        [
            {"species": "dog", "id": 1, "name": "Cleo", "age": 4},
            {"species": "cat", "id": 1, "name": "Catbag"},
        ],
        pk=("species", "id"),
    )
    table.upsert_all([{"species": "dog", "id": 1, "age": 5}], pk=("species", "id"))
    assert [
        {"species": "dog", "id": 1, "name": "Cleo", "age": 5},
        {"species": "cat", "id": 1, "name": "Catbag", "age": None},
    ] == list(table.rows)
