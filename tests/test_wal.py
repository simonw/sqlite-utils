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


def test_ensure_autocommit_off_sets_autocommit_mode_temporarily(db_path_tmpdir):
    db, path, tmpdir = db_path_tmpdir
    original_isolation_level = db.conn.isolation_level
    with db.ensure_autocommit_off():
        assert db.conn.isolation_level is None
    assert db.conn.isolation_level == original_isolation_level
