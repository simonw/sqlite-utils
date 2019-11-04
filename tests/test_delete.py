def test_delete_rowid_table(fresh_db):
    table = fresh_db["table"]
    table.insert({"foo": 1}).last_pk
    rowid = table.insert({"foo": 2}).last_pk
    table.delete(rowid)
    assert [{"foo": 1}] == list(table.rows)


def test_delete_pk_table(fresh_db):
    table = fresh_db["table"]
    table.insert({"id": 1}, pk="id")
    table.insert({"id": 2}, pk="id")
    table.delete(1)
    assert [{"id": 2}] == list(table.rows)


def test_delete_where(fresh_db):
    table = fresh_db["table"]
    for i in range(1, 11):
        table.insert({"id": i}, pk="id")
    assert 10 == table.count
    table.delete_where("id > ?", [5])
    assert 5 == table.count


def test_delete_where_all(fresh_db):
    table = fresh_db["table"]
    for i in range(1, 11):
        table.insert({"id": i}, pk="id")
    assert 10 == table.count
    table.delete_where()
    assert 0 == table.count
