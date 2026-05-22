import pytest
from click.testing import CliRunner
from sqlite_utils import Database, cli


# ---------------------------------------------------------------------------
# Python API tests
# ---------------------------------------------------------------------------


def test_merge_basic(tmpdir):
    """Tables from source databases are created in the destination."""
    dest = Database(str(tmpdir / "dest.db"))
    src1 = Database(str(tmpdir / "src1.db"))
    src2 = Database(str(tmpdir / "src2.db"))

    src1["cats"].insert_all([{"id": 1, "name": "Socks"}, {"id": 2, "name": "Mittens"}], pk="id")
    src2["dogs"].insert_all([{"id": 1, "name": "Rex"}], pk="id")

    dest.merge([str(tmpdir / "src1.db"), str(tmpdir / "src2.db")])

    assert set(dest.table_names()) == {"cats", "dogs"}
    assert list(dest["cats"].rows) == [{"id": 1, "name": "Socks"}, {"id": 2, "name": "Mittens"}]
    assert list(dest["dogs"].rows) == [{"id": 1, "name": "Rex"}]


def test_merge_appends_rows_to_existing_table(tmpdir):
    """Rows from source are appended to existing destination table."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    dest["items"].insert_all([{"id": 1, "name": "a"}], pk="id")
    src["items"].insert_all([{"id": 2, "name": "b"}, {"id": 3, "name": "c"}], pk="id")

    dest.merge([src])

    rows = list(dest["items"].rows)
    assert len(rows) == 3
    assert {"id": 2, "name": "b"} in rows


def test_merge_replace(tmpdir):
    """--replace causes conflicting rows to be overwritten."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    dest["items"].insert_all([{"id": 1, "val": "original"}], pk="id")
    src["items"].insert_all([{"id": 1, "val": "updated"}], pk="id")

    dest.merge([src], replace=True)

    assert list(dest["items"].rows) == [{"id": 1, "val": "updated"}]


def test_merge_ignore(tmpdir):
    """--ignore causes conflicting rows to be silently skipped."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    dest["items"].insert_all([{"id": 1, "val": "original"}], pk="id")
    src["items"].insert_all([{"id": 1, "val": "updated"}, {"id": 2, "val": "new"}], pk="id")

    dest.merge([src], ignore=True)

    rows = {r["id"]: r["val"] for r in dest["items"].rows}
    assert rows[1] == "original"  # not overwritten
    assert rows[2] == "new"       # new row inserted


def test_merge_alter_adds_missing_columns(tmpdir):
    """alter=True adds columns that exist in source but not in destination."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    dest["items"].insert_all([{"id": 1, "name": "a"}], pk="id")
    src["items"].insert_all([{"id": 2, "name": "b", "extra": "bonus"}], pk="id")

    dest.merge([src], alter=True)

    assert "extra" in dest["items"].columns_dict
    row = next(r for r in dest["items"].rows if r["id"] == 2)
    assert row["extra"] == "bonus"


def test_merge_specific_tables(tmpdir):
    """tables= parameter limits which tables are merged."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    src["wanted"].insert_all([{"id": 1}], pk="id")
    src["unwanted"].insert_all([{"id": 99}], pk="id")

    dest.merge([src], tables=["wanted"])

    assert "wanted" in dest.table_names()
    assert "unwanted" not in dest.table_names()


def test_merge_table_not_in_source_is_skipped(tmpdir):
    """Tables listed in tables= that don't exist in a source are silently skipped."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))
    src["existing"].insert({"id": 1})

    # Should not raise even though "missing" doesn't exist in src
    dest.merge([src], tables=["existing", "missing"])

    assert "existing" in dest.table_names()


def test_merge_multiple_sources(tmpdir):
    """Rows from multiple source DBs are all merged into destination."""
    dest = Database(str(tmpdir / "dest.db"))
    srcs = []
    for i in range(3):
        path = str(tmpdir / f"src{i}.db")
        db = Database(path)
        db["nums"].insert({"id": i, "val": i * 10}, pk="id")
        srcs.append(path)

    dest.merge(srcs)

    assert list(sorted(dest["nums"].rows, key=lambda r: r["id"])) == [
        {"id": 0, "val": 0},
        {"id": 1, "val": 10},
        {"id": 2, "val": 20},
    ]


def test_merge_skips_virtual_tables(tmpdir):
    """Virtual tables (e.g. FTS) in source are silently skipped."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    src["docs"].insert_all([{"id": 1, "body": "hello world"}], pk="id")
    src["docs"].enable_fts(["body"])

    dest.merge([src])

    # Normal table merged, FTS virtual table skipped
    assert "docs" in dest.table_names()
    fts_tables = [t for t in dest.table_names() if "fts" in t.lower()]
    assert fts_tables == []


def test_merge_accepts_database_objects(tmpdir):
    """Source can be a Database object instead of a file path."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))
    src["items"].insert({"id": 1, "val": "x"}, pk="id")

    dest.merge([src])

    assert list(dest["items"].rows) == [{"id": 1, "val": "x"}]


def test_merge_returns_self(tmpdir):
    """merge() returns the destination Database for chaining."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))
    src["t"].insert({"x": 1})

    result = dest.merge([src])

    assert result is dest


def test_merge_no_pk_table(tmpdir):
    """Tables without an explicit primary key are merged without conflicts."""
    dest = Database(str(tmpdir / "dest.db"))
    src = Database(str(tmpdir / "src.db"))

    src["log"].insert_all([{"msg": "a"}, {"msg": "b"}])  # no pk

    dest.merge([src])

    assert len(list(dest["log"].rows)) == 2


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


def test_cli_merge_basic(tmpdir):
    """CLI merge creates destination and copies tables from sources."""
    src1_path = str(tmpdir / "src1.db")
    src2_path = str(tmpdir / "src2.db")
    dest_path = str(tmpdir / "dest.db")

    Database(src1_path)["cats"].insert_all([{"id": 1, "name": "Socks"}], pk="id")
    Database(src2_path)["dogs"].insert_all([{"id": 1, "name": "Rex"}], pk="id")

    result = CliRunner().invoke(cli.cli, ["merge", dest_path, src1_path, src2_path])
    assert result.exit_code == 0, result.output

    dest = Database(dest_path)
    assert set(dest.table_names()) == {"cats", "dogs"}


def test_cli_merge_alter(tmpdir):
    """CLI merge --alter adds missing columns."""
    src_path = str(tmpdir / "src.db")
    dest_path = str(tmpdir / "dest.db")

    Database(dest_path)["items"].insert({"id": 1, "name": "a"}, pk="id")
    Database(src_path)["items"].insert({"id": 2, "name": "b", "extra": "x"}, pk="id")

    result = CliRunner().invoke(cli.cli, ["merge", dest_path, src_path, "--alter"])
    assert result.exit_code == 0, result.output
    assert "extra" in Database(dest_path)["items"].columns_dict


def test_cli_merge_replace(tmpdir):
    """CLI merge --replace overwrites conflicting rows."""
    src_path = str(tmpdir / "src.db")
    dest_path = str(tmpdir / "dest.db")

    Database(dest_path)["items"].insert({"id": 1, "val": "old"}, pk="id")
    Database(src_path)["items"].insert({"id": 1, "val": "new"}, pk="id")

    CliRunner().invoke(cli.cli, ["merge", dest_path, src_path, "--replace"])
    assert list(Database(dest_path)["items"].rows) == [{"id": 1, "val": "new"}]


def test_cli_merge_ignore(tmpdir):
    """CLI merge --ignore skips conflicting rows."""
    src_path = str(tmpdir / "src.db")
    dest_path = str(tmpdir / "dest.db")

    Database(dest_path)["items"].insert({"id": 1, "val": "original"}, pk="id")
    Database(src_path)["items"].insert({"id": 1, "val": "new"}, pk="id")

    CliRunner().invoke(cli.cli, ["merge", dest_path, src_path, "--ignore"])
    assert list(Database(dest_path)["items"].rows) == [{"id": 1, "val": "original"}]


def test_cli_merge_table_filter(tmpdir):
    """CLI merge --table limits which tables are merged."""
    src_path = str(tmpdir / "src.db")
    dest_path = str(tmpdir / "dest.db")

    src = Database(src_path)
    src["wanted"].insert({"id": 1})
    src["unwanted"].insert({"id": 2})

    CliRunner().invoke(cli.cli, ["merge", dest_path, src_path, "--table", "wanted"])

    dest = Database(dest_path)
    assert "wanted" in dest.table_names()
    assert "unwanted" not in dest.table_names()
