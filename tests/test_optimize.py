import pytest
from sqlite_utils import Database
import sqlite3


@pytest.mark.parametrize("fts_version", ["FTS4", "FTS5"])
def test_optimizes_removes_junk_docsize_rows(tmpdir, fts_version):
    # Recreating https://github.com/simonw/sqlite-utils/issues/149
    path = tmpdir / "test.db"
    db = Database(str(path), recursive_triggers=False)
    licenses = [{"key": "apache2", "name": "Apache 2"}, {"key": "bsd", "name": "BSD"}]
    db["licenses"].insert_all(licenses, pk="key", replace=True)
    db["licenses"].enable_fts(["name"], create_triggers=True, fts_version=fts_version)
    assert db["licenses_fts_docsize"].count == 2
    # Bug: insert with replace increases the number of rows in _docsize:
    db["licenses"].insert_all(licenses, pk="key", replace=True)
    assert db["licenses_fts_docsize"].count == 4
    # Optimize should fix this:
    db["licenses_fts"].optimize()
    assert db["licenses_fts_docsize"].count == 2
