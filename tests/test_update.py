from sqlite_utils.db import Database


def test_update(fresh_db):
    cleo = {"id": 1, "name": "Cleo", "age": 4}
    fresh_db["dogs"].insert(cleo, pk="id")
    assert [cleo] == list(fresh_db["dogs"].rows)
    fresh_db["dogs"].update(1, {"age": 5})
    assert [{"id": 1, "name": "Cleo", "age": 5}] == list(fresh_db["dogs"].rows)
