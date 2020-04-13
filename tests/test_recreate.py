from sqlite_utils import Database
import sqlite3
import pathlib
import pytest


def test_recreate_ignored_for_in_memory():
    # None of these should raise an exception:
    Database(memory=True, recreate=False)
    Database(memory=True, recreate=True)
    Database(":memory:", recreate=False)
    Database(":memory:", recreate=True)


def test_recreate_not_allowed_for_connection():
    conn = sqlite3.connect(":memory:")
    with pytest.raises(AssertionError):
        db = Database(conn, recreate=True)


@pytest.mark.parametrize(
    "use_path,file_exists", [(True, True), (True, False), (False, True), (False, False)]
)
def test_recreate(tmpdir, use_path, file_exists):
    filepath = str(tmpdir / "data.db")
    if use_path:
        filepath = pathlib.Path(filepath)
    if file_exists:
        Database(filepath)["t1"].insert({"foo": "bar"})
        assert ["t1"] == Database(filepath).table_names()
    Database(filepath, recreate=True)["t2"].insert({"foo": "bar"})
    assert ["t2"] == Database(filepath).table_names()
