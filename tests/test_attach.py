from sqlite_utils import Database


def test_attach(tmpdir):
    foo_path = str(tmpdir / "foo.db")
    bar_path = str(tmpdir / "bar.db")
    db = Database(foo_path)
    with db.conn:
        db["foo"].insert({"id": 1, "text": "foo"})
    db2 = Database(bar_path)
    with db2.conn:
        db2["bar"].insert({"id": 1, "text": "bar"})
    db.attach("bar", bar_path)
    assert db.execute(
        "select * from foo union all select * from bar.bar"
    ).fetchall() == [(1, "foo"), (1, "bar")]
