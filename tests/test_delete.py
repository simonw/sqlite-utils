import sqlite_utils


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
    assert table.count == 10
    table.delete_where("id > ?", [5])
    assert table.count == 5


def test_delete_where_all(fresh_db):
    table = fresh_db["table"]
    for i in range(1, 11):
        table.insert({"id": i}, pk="id")
    assert table.count == 10
    table.delete_where()
    assert table.count == 0


def test_delete_where_commits(tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite_utils.Database(path)
    db["table"].insert_all([{"id": i} for i in range(5)], pk="id")
    db["table"].delete_where("id > ?", [2])
    # The connection must not be left inside an open transaction,
    # otherwise subsequent atomic() blocks never commit either
    assert not db.in_transaction
    db["table"].insert({"id": 100})
    db.close()
    db2 = sqlite_utils.Database(path)
    assert [r["id"] for r in db2["table"].rows] == [0, 1, 2, 100]
    db2.close()


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
