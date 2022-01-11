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


def test_delete_where_analyze(fresh_db):
    table = fresh_db["table"]
    table.insert_all(({"id": i, "i": i} for i in range(10)), pk="id")
    table.create_index(["i"], analyze=True)
    assert "sqlite_stat1" in fresh_db.table_names()
    assert list(fresh_db["sqlite_stat1"].rows) == [
        {"tbl": "table", "idx": "idx_table_i", "stat": "10 1"}
    ]
    table.delete_where("id > ?", [5], analyze=True)
    assert list(fresh_db["sqlite_stat1"].rows) == [
        {"tbl": "table", "idx": "idx_table_i", "stat": "6 1"}
    ]
