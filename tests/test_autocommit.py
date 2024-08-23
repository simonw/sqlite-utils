import pytest


def test_executescript_rollback(fresh_db):
    if not hasattr(fresh_db.conn, "autocommit"):
        pytest.skip("No autocommit support")

    fresh_db.conn.autocommit = False
    fresh_db.executescript("CREATE TABLE [test_table] ([id] INTEGER);")
    assert fresh_db.table_names() == ["test_table"]
    fresh_db.conn.rollback()
    assert fresh_db.table_names() == []


def test_explicit_create_table_rollback(fresh_db):
    if not hasattr(fresh_db.conn, "autocommit"):
        pytest.skip("No autocommit support")

    fresh_db.conn.autocommit = False
    fresh_db.create_table("test_table", {"id": int})
    assert fresh_db.table_names() == ["test_table"]
    fresh_db.conn.rollback()
    assert fresh_db.table_names() == []


@pytest.mark.parametrize("use_table_factory", [True, False])
def test_create_table_rollback(fresh_db, use_table_factory):
    if not hasattr(fresh_db.conn, "autocommit"):
        pytest.skip("No autocommit support")

    fresh_db.conn.autocommit = False
    if use_table_factory:
        fresh_db.table("test_table").create({"id": int})
    else:
        fresh_db["test_table"].create({"id": int})
    assert fresh_db.table_names() == ["test_table"]
    fresh_db.conn.rollback()
    assert fresh_db.table_names() == []
