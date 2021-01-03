from sqlite_utils import Database
from sqlite_utils import cli
from click.testing import CliRunner
import pytest


def test_enable_counts_specific_table(fresh_db):
    foo = fresh_db["foo"]
    assert fresh_db.table_names() == []
    for i in range(10):
        foo.insert({"name": "item {}".format(i)})
    assert fresh_db.table_names() == ["foo"]
    assert foo.count == 10
    # Now enable counts
    foo.enable_counts()
    assert foo.triggers_dict == {
        "foo_counts_insert": "CREATE TRIGGER [foo_counts_insert] AFTER INSERT ON [foo]\nBEGIN\n    INSERT OR REPLACE INTO [_counts]\n    VALUES (\n        'foo',\n        COALESCE(\n            (SELECT count FROM [_counts] WHERE [table] = 'foo'),\n        0\n        ) + 1\n    );\nEND",
        "foo_counts_delete": "CREATE TRIGGER [foo_counts_delete] AFTER DELETE ON [foo]\nBEGIN\n    INSERT OR REPLACE INTO [_counts]\n    VALUES (\n        'foo',\n        COALESCE(\n            (SELECT count FROM [_counts] WHERE [table] = 'foo'),\n        0\n        ) - 1\n    );\nEND",
    }
    assert fresh_db.table_names() == ["foo", "_counts"]
    assert list(fresh_db["_counts"].rows) == [{"count": 10, "table": "foo"}]
    # Add some items to test the triggers
    for i in range(5):
        foo.insert({"name": "item {}".format(10 + i)})
    assert foo.count == 15
    assert list(fresh_db["_counts"].rows) == [{"count": 15, "table": "foo"}]
    # Delete some items
    foo.delete_where("rowid < 7")
    assert foo.count == 9
    assert list(fresh_db["_counts"].rows) == [{"count": 9, "table": "foo"}]
    foo.delete_where()
    assert foo.count == 0
    assert list(fresh_db["_counts"].rows) == [{"count": 0, "table": "foo"}]


def test_enable_counts_all_tables(fresh_db):
    foo = fresh_db["foo"]
    bar = fresh_db["bar"]
    foo.insert({"name": "Cleo"})
    bar.insert({"name": "Cleo"})
    foo.enable_fts(["name"])
    fresh_db.enable_counts()
    assert set(fresh_db.table_names()) == {
        "foo",
        "bar",
        "foo_fts",
        "foo_fts_data",
        "foo_fts_idx",
        "foo_fts_docsize",
        "foo_fts_config",
        "_counts",
    }
    assert list(fresh_db["_counts"].rows) == [
        {"count": 1, "table": "foo"},
        {"count": 1, "table": "bar"},
        {"count": 3, "table": "foo_fts_data"},
        {"count": 1, "table": "foo_fts_idx"},
        {"count": 1, "table": "foo_fts_docsize"},
        {"count": 1, "table": "foo_fts_config"},
    ]


@pytest.fixture
def counts_db_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = Database(path)
    db["foo"].insert({"name": "bar"})
    db["bar"].insert({"name": "bar"})
    db["bar"].insert({"name": "bar"})
    db["baz"].insert({"name": "bar"})
    return path


@pytest.mark.parametrize(
    "extra_args,expected_triggers",
    [
        (
            [],
            [
                "foo_counts_insert",
                "foo_counts_delete",
                "bar_counts_insert",
                "bar_counts_delete",
                "baz_counts_insert",
                "baz_counts_delete",
            ],
        ),
        (
            ["bar"],
            [
                "bar_counts_insert",
                "bar_counts_delete",
            ],
        ),
    ],
)
def test_cli_enable_counts(counts_db_path, extra_args, expected_triggers):
    db = Database(counts_db_path)
    assert list(db.triggers_dict.keys()) == []
    result = CliRunner().invoke(cli.cli, ["enable-counts", counts_db_path] + extra_args)
    assert result.exit_code == 0
    assert list(db.triggers_dict.keys()) == expected_triggers


def test_uses_counts_after_enable_counts(counts_db_path):
    db = Database(counts_db_path)
    logged = []
    with db.tracer(lambda sql, parameters: logged.append((sql, parameters))):
        assert db["foo"].count == 1
        assert logged == [
            ("select name from sqlite_master where type = 'view'", None),
            ("select count(*) from [foo]", None),
        ]
        logged.clear()
        assert not db.use_counts_table
        db.enable_counts()
        assert db.use_counts_table
        assert db["foo"].count == 1
    assert logged == [
        (
            "CREATE TABLE IF NOT EXISTS [_counts](\n   [table] TEXT PRIMARY KEY,\n   count INTEGER DEFAULT 0\n);",
            None,
        ),
        ("select name from sqlite_master where type = 'table'", None),
        ("select name from sqlite_master where type = 'view'", None),
        ("select name from sqlite_master where type = 'view'", None),
        ("select name from sqlite_master where type = 'view'", None),
        ("select name from sqlite_master where type = 'view'", None),
        ("select sql from sqlite_master where name = ?", ("foo",)),
        ("SELECT quote(:value)", {"value": "foo"}),
        ("select sql from sqlite_master where name = ?", ("bar",)),
        ("SELECT quote(:value)", {"value": "bar"}),
        ("select sql from sqlite_master where name = ?", ("baz",)),
        ("SELECT quote(:value)", {"value": "baz"}),
        ("select sql from sqlite_master where name = ?", ("_counts",)),
        ("select name from sqlite_master where type = 'view'", None),
        ("select [table], count from _counts where [table] in (?)", ["foo"]),
    ]


def test_reset_counts(counts_db_path):
    db = Database(counts_db_path)
    db["foo"].enable_counts()
    db["bar"].enable_counts()
    assert db.cached_counts() == {"foo": 1, "bar": 2}
    # Corrupt the value
    db["_counts"].update("foo", {"count": 3})
    assert db.cached_counts() == {"foo": 3, "bar": 2}
    assert db["foo"].count == 3
    # Reset them
    db.reset_counts()
    assert db.cached_counts() == {"foo": 1, "bar": 2}
    assert db["foo"].count == 1


def test_reset_counts_cli(counts_db_path):
    db = Database(counts_db_path)
    db["foo"].enable_counts()
    db["bar"].enable_counts()
    assert db.cached_counts() == {"foo": 1, "bar": 2}
    db["_counts"].update("foo", {"count": 3})
    result = CliRunner().invoke(cli.cli, ["reset-counts", counts_db_path])
    assert result.exit_code == 0
    assert db.cached_counts() == {"foo": 1, "bar": 2}
