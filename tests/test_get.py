import pytest
from sqlite_utils.db import NotFoundError


def test_get_rowid(fresh_db):
    dogs = fresh_db["dogs"]
    cleo = {"name": "Cleo", "age": 4}
    row_id = dogs.insert(cleo).last_rowid
    assert cleo == dogs.get(row_id)


def test_get_primary_key(fresh_db):
    dogs = fresh_db["dogs"]
    cleo = {"name": "Cleo", "age": 4, "id": 5}
    last_pk = dogs.insert(cleo, pk="id").last_pk
    assert 5 == last_pk
    assert cleo == dogs.get(5)


@pytest.mark.parametrize(
    "argument,expected_msg",
    [(100, None), (None, None), ((1, 2), "Need 1 primary key value"), ("2", None)],
)
def test_get_not_found(argument, expected_msg, fresh_db):
    fresh_db["dogs"].insert(
        {"id": 1, "name": "Cleo", "age": 4, "is_good": True}, pk="id"
    )
    with pytest.raises(NotFoundError) as excinfo:
        fresh_db["dogs"].get(argument)
    if expected_msg is not None:
        assert expected_msg == excinfo.value.args[0]


@pytest.mark.parametrize(
    "where,where_args,expected_ids",
    [
        ("name = ?", ["Pancakes"], {2}),
        ("age > ?", [3], {1}),
        ("name is not null", [], {1, 2}),
        ("is_good = ?", [True], {1, 2}),
    ],
)
def test_rows_where(where, where_args, expected_ids, fresh_db):
    table = fresh_db["dogs"]
    table.insert_all(
        [
            {"id": 1, "name": "Cleo", "age": 4, "is_good": True},
            {"id": 2, "name": "Pancakes", "age": 3, "is_good": True},
        ],
        pk="id",
    )
    assert expected_ids == {r["id"] for r in table.rows_where(where, where_args)}
