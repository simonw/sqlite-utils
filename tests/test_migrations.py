import pytest
import sqlite_utils
from sqlite_utils import Migrations


@pytest.fixture
def migrations():
    migrations = Migrations("test")

    @migrations()
    def m001(db):
        db["dogs"].insert({"name": "Cleo"})

    @migrations()
    def m002(db):
        db["cats"].create({"name": str})
        db.execute("insert into dogs (name) values ('Pancakes')")

    return migrations


@pytest.fixture
def migrations_not_ordered_alphabetically():
    # Names order alphabetically in the wrong direction but this
    # should still be applied correctly.
    migrations = Migrations("test")

    @migrations()
    def m002(db):
        db["dogs"].insert({"name": "Cleo"})

    @migrations()
    def m001(db):
        db["cats"].create({"name": str})
        db.execute("insert into dogs (name) values ('Pancakes')")

    return migrations


@pytest.fixture
def migrations2():
    migrations = Migrations("test2")

    @migrations()
    def m001(db):
        db["dogs2"].insert({"name": "Cleo"})

    return migrations


def test_basic(migrations):
    db = sqlite_utils.Database(memory=True)
    assert db.table_names() == []
    migrations.apply(db)
    assert set(db.table_names()) == {"_sqlite_migrations", "dogs", "cats"}


def test_stop_before(migrations):
    db = sqlite_utils.Database(memory=True)
    assert db.table_names() == []
    migrations.apply(db, stop_before="m002")
    assert set(db.table_names()) == {"_sqlite_migrations", "dogs"}
    migrations.apply(db)
    assert set(db.table_names()) == {"_sqlite_migrations", "dogs", "cats"}


def test_two_migration_sets(migrations, migrations2):
    db = sqlite_utils.Database(memory=True)
    assert db.table_names() == []
    migrations.apply(db)
    migrations2.apply(db)
    assert set(db.table_names()) == {"_sqlite_migrations", "dogs", "cats", "dogs2"}


def test_order_does_not_matter(migrations, migrations_not_ordered_alphabetically):
    db1 = sqlite_utils.Database(memory=True)
    db2 = sqlite_utils.Database(memory=True)
    migrations.apply(db1)
    migrations_not_ordered_alphabetically.apply(db2)
    assert db1.schema == db2.schema


def test_applied_at_is_a_string(migrations):
    db = sqlite_utils.Database(memory=True)
    migrations.apply(db)
    applied = migrations.applied(db)
    assert len(applied) == 2
    for migration in applied:
        # applied_at is the TEXT timestamp straight from the
        # _sqlite_migrations table, e.g. "2026-07-04 12:00:00.000000+00:00"
        assert isinstance(migration.applied_at, str)
        assert migration.applied_at.endswith("+00:00")


def test_failing_migration_rolls_back(migrations):
    @migrations()
    def m003(db):
        db["birds"].create({"name": str})
        db.execute("insert into dogs (name) values ('Dozer')")
        raise ValueError("boom")

    db = sqlite_utils.Database(memory=True)
    with pytest.raises(ValueError):
        migrations.apply(db)
    # m001 and m002 committed before the failure and stay applied
    assert set(db.table_names()) == {"_sqlite_migrations", "dogs", "cats"}
    assert [r["name"] for r in db["dogs"].rows] == ["Cleo", "Pancakes"]
    assert [m.name for m in migrations.applied(db)] == ["m001", "m002"]
    # Everything m003 did was rolled back and it is still pending
    assert [m.name for m in migrations.pending(db)] == ["m003"]


def test_rerun_after_failure_applies_each_migration_once():
    state = {"fail": True}
    migrations = Migrations("test")

    @migrations()
    def m001(db):
        db["dogs"].insert({"name": "Cleo"})

    @migrations()
    def m002(db):
        db["dogs"].insert({"name": "Pancakes"})
        if state["fail"]:
            raise ValueError("boom")

    db = sqlite_utils.Database(memory=True)
    with pytest.raises(ValueError):
        migrations.apply(db)
    state["fail"] = False
    migrations.apply(db)
    # m001 must not have been re-applied, m002 applied exactly once
    assert [r["name"] for r in db["dogs"].rows] == ["Cleo", "Pancakes"]
    assert [m.name for m in migrations.applied(db)] == ["m001", "m002"]


def test_non_transactional_migration_allows_vacuum(tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite_utils.Database(path)
    migrations = Migrations("test")

    @migrations()
    def m001(db):
        db["dogs"].insert({"name": "Cleo"})

    @migrations(transactional=False)
    def m002(db):
        db.execute("VACUUM")

    migrations.apply(db)
    assert [m.name for m in migrations.applied(db)] == ["m001", "m002"]
    db.close()


def test_apply_composes_inside_outer_transaction(migrations):
    db = sqlite_utils.Database(memory=True)
    with pytest.raises(ZeroDivisionError):
        with db.atomic():
            migrations.apply(db)
            raise ZeroDivisionError
    # The outer transaction rolled back, taking the migrations with it
    assert db.table_names() == []


@pytest.mark.parametrize(
    "create_table,pk",
    (
        (
            {
                "migration_set": str,
                "name": str,
                "applied_at": str,
            },
            "name",
        ),
        (
            {
                "migration_set": str,
                "name": str,
                "applied_at": str,
            },
            ("migration_set", "name"),
        ),
    ),
)
def test_upgrades_sqlite_migrations(migrations, create_table, pk):
    db = sqlite_utils.Database(memory=True)
    db["_sqlite_migrations"].create(create_table, pk=pk)
    assert db.table_names() == ["_sqlite_migrations"]
    assert db["_sqlite_migrations"].pks == ([pk] if isinstance(pk, str) else list(pk))
    migrations.apply(db)
    assert db["_sqlite_migrations"].pks == ["id"]
