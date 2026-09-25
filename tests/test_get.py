import pytest

from sqlite_utils.db import NotFoundError


def test_get_rowid(fresh_db):
    dogs = fresh_db.table("dogs")
    cleo = {"name": "Cleo", "age": 4}
    row_id = dogs.insert(cleo).last_rowid
    assert cleo == dogs.get(row_id)


def test_get_primary_key(fresh_db):
    dogs = fresh_db.table("dogs")
    cleo = {"name": "Cleo", "age": 4, "id": 5}
    last_pk = dogs.insert(cleo, pk="id").last_pk
    assert 5 == last_pk
    assert cleo == dogs.get(5)


@pytest.mark.parametrize(
    "argument,expected_msg",
    [(100, None), (None, None), ((1, 2), "Need 1 primary key value"), ("2", None)],
)
def test_get_not_found(argument, expected_msg, fresh_db):
    fresh_db.table("dogs").insert(
        {"id": 1, "name": "Cleo", "age": 4, "is_good": True}, pk="id"
    )
    with pytest.raises(NotFoundError) as excinfo:
        fresh_db.table("dogs").get(argument)
    if expected_msg is not None:
        assert expected_msg == excinfo.value.args[0]


def test_get_by_column(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert_all(
        [
            {"id": 1, "name": "Cleo", "age": 4},
            {"id": 2, "name": "Pancakes", "age": 2},
        ],
        pk="id",
    )
    assert dogs.get(name="Pancakes") == {"id": 2, "name": "Pancakes", "age": 2}


def test_get_by_multiple_columns(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert_all(
        [
            {"id": 1, "name": "Cleo", "age": 4},
            {"id": 2, "name": "Cleo", "age": 2},
        ],
        pk="id",
    )
    assert dogs.get(name="Cleo", age=2)["id"] == 2


def test_get_by_column_sets_last_pk(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert({"id": 5, "name": "Cleo", "age": 4}, pk="id")
    dogs.get(name="Cleo")
    assert dogs.last_pk == 5


def test_get_by_column_compound_pk_sets_last_pk(fresh_db):
    table = fresh_db.table("records")
    table.insert_all(
        [{"a": 1, "b": 2, "note": "x"}, {"a": 1, "b": 3, "note": "y"}],
        pk=("a", "b"),
    )
    assert table.get(note="y")["b"] == 3
    assert table.last_pk == (1, 3)


def test_get_by_column_on_rowid_table(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert({"name": "Cleo", "age": 4})
    assert dogs.get(name="Cleo")["name"] == "Cleo"


def test_get_by_column_not_found(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert({"id": 1, "name": "Cleo", "age": 4}, pk="id")
    with pytest.raises(NotFoundError):
        dogs.get(name="Pancakes")


def test_get_rejects_pk_and_column(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert({"id": 1, "name": "Cleo", "age": 4}, pk="id")
    with pytest.raises(ValueError):
        dogs.get(1, name="Cleo")


def test_get_requires_argument(fresh_db):
    dogs = fresh_db.table("dogs")
    dogs.insert({"id": 1, "name": "Cleo", "age": 4}, pk="id")
    with pytest.raises(TypeError):
        dogs.get()
