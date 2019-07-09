from sqlite_utils.db import Database, RowNotFound
import pytest


def test_get_single_pk(fresh_db):
    cleo = {"id": 1, "name": "Cleo", "age": 4}
    table = fresh_db["dogs"].insert(cleo, pk="id")
    with pytest.raises(RowNotFound):
        table.get(2)
    with pytest.raises(RowNotFound):
        table.get(None)
    assert cleo == table.get(1)


def test_get_compound_pk(fresh_db):
    cleo = {"id1": 1, "id2": 1, "name": "Cleo", "age": 4}
    table = fresh_db["dogs"].insert(cleo, pk=("id1", "id2"))
    with pytest.raises(RowNotFound):
        table.get(2)
    with pytest.raises(RowNotFound):
        table.get([2, 1])
    assert cleo == table.get([1, 1])
