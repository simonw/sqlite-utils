from sqlite_utils import Database
import pytest


def test_recursive_triggers():
    db = Database(memory=True)
    assert db.execute("PRAGMA recursive_triggers").fetchone()[0]


def test_recursive_triggers_off():
    db = Database(memory=True, recursive_triggers=False)
    assert not db.execute("PRAGMA recursive_triggers").fetchone()[0]
