import pytest

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


@pytest.fixture
def db_with_attached(tmpdir):
    foo_path = str(tmpdir / "foo.db")
    bar_path = str(tmpdir / "bar.db")
    db2 = Database(bar_path)
    with db2.conn:
        db2["bar"].insert({"id": 1, "text": "bar"}, pk="id")
    db = Database(foo_path)
    db.attach("bar", bar_path)
    return db


def test_attached_table_exists_and_is_queryable(db_with_attached):
    # Regression test for issue #432: a Table for an attached alias should
    # be first-class, not silently invisible to exists()/rows_where()/etc.
    db = db_with_attached
    table = db["bar.bar"]
    assert table.alias == "bar"
    assert table.name == "bar"
    assert table.exists() is True
    assert list(table.rows_where()) == [{"id": 1, "text": "bar"}]
    assert table.get(1) == {"id": 1, "text": "bar"}
    assert [c.name for c in table.columns] == ["id", "text"]


def test_attached_table_insert_update_delete(db_with_attached):
    db = db_with_attached
    table = db["bar.bar"]
    table.insert({"id": 2, "text": "baz"})
    assert table.count == 2
    table.update(2, {"text": "baz2"})
    assert table.get(2)["text"] == "baz2"
    table.delete_where("id = ?", [2])
    assert table.count == 1
    table.delete(1)
    assert table.count == 0


def test_attached_table_does_not_exist_yet(db_with_attached):
    db = db_with_attached
    table = db["bar.does_not_exist"]
    assert table.exists() is False
    # These should quietly no-op rather than raise or touch the main schema
    assert list(table.rows_where()) == []
    table.delete_where()
    assert "does_not_exist" not in db.table_names()


def test_attached_table_create_raises_clear_error(db_with_attached):
    # Auto-creating tables in an attached schema isn't supported yet - this
    # should raise a clear error rather than silently creating the table in
    # the main schema instead
    db = db_with_attached
    table = db["bar.does_not_exist"]
    with pytest.raises(NotImplementedError):
        table.create({"id": int})
    with pytest.raises(NotImplementedError):
        table.insert({"id": 1})
    assert "does_not_exist" not in db.table_names()


def test_attached_table_unsupported_operations_raise(db_with_attached):
    db = db_with_attached
    table = db["bar.bar"]
    with pytest.raises(NotImplementedError):
        table.transform(rename={"text": "value"})
    with pytest.raises(NotImplementedError):
        table.create_index(["text"])
    with pytest.raises(NotImplementedError):
        table.duplicate("bar2")


def test_temp_table_is_first_class(fresh_db):
    db = fresh_db
    db.execute("create temp table scratch (id integer, value text)")
    db.execute("insert into scratch values (1, 'x')")
    table = db["temp.scratch"]
    assert table.alias == "temp"
    assert table.exists() is True
    assert list(table.rows_where()) == [{"id": 1, "value": "x"}]
    table.insert({"id": 2, "value": "y"})
    assert table.count == 2


def test_dotted_name_without_matching_alias_is_literal(fresh_db):
    # A "." in a table name that doesn't match a real attached/temp schema
    # should be treated as a literal main-schema table name, unchanged from
    # existing behaviour
    db = fresh_db
    db["weird.name"].insert({"id": 1}, pk="id")
    assert db["weird.name"].alias is None
    assert "weird.name" in db.table_names()
