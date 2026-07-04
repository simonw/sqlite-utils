import pytest
from sqlite_utils import Database


@pytest.fixture
def db_path_tmpdir(tmpdir):
    path = tmpdir / "test.db"
    db = Database(str(path))
    return db, path, tmpdir


def test_enable_disable_wal(db_path_tmpdir):
    db, path, tmpdir = db_path_tmpdir
    assert len(tmpdir.listdir()) == 1
    assert "delete" == db.journal_mode
    assert "test.db-wal" not in [f.basename for f in tmpdir.listdir()]
    db.enable_wal()
    assert "wal" == db.journal_mode
    db["test"].insert({"foo": "bar"})
    assert "test.db-wal" in [f.basename for f in tmpdir.listdir()]
    db.disable_wal()
    assert "delete" == db.journal_mode
    assert "test.db-wal" not in [f.basename for f in tmpdir.listdir()]


def test_enable_wal_inside_transaction_raises(db_path_tmpdir):
    db, path, tmpdir = db_path_tmpdir
    db["test"].insert({"id": 1}, pk="id")
    with pytest.raises(RuntimeError):
        with db.atomic():
            db["test"].insert({"id": 2}, pk="id")
            db.enable_wal()
    # The atomic() block must have rolled back cleanly and the
    # journal mode must be unchanged
    assert db.journal_mode == "delete"
    assert [r["id"] for r in db["test"].rows] == [1]


def test_disable_wal_inside_transaction_raises(db_path_tmpdir):
    db, path, tmpdir = db_path_tmpdir
    db.enable_wal()
    db["test"].insert({"id": 1}, pk="id")
    with pytest.raises(RuntimeError):
        with db.atomic():
            db["test"].insert({"id": 2}, pk="id")
            db.disable_wal()
    assert db.journal_mode == "wal"
    assert [r["id"] for r in db["test"].rows] == [1]


def test_enable_wal_noop_inside_transaction_is_allowed(db_path_tmpdir):
    # Calling enable_wal() when WAL is already enabled is a no-op,
    # so it is fine inside a transaction
    db, path, tmpdir = db_path_tmpdir
    db.enable_wal()
    with db.atomic():
        db["test"].insert({"id": 1}, pk="id")
        db.enable_wal()
    assert [r["id"] for r in db["test"].rows] == [1]
