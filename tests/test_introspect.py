from sqlite_utils.db import Index, View, Database, XIndex, XIndexColumn
import pytest


def test_table_names(existing_db):
    assert ["foo"] == existing_db.table_names()


def test_view_names(fresh_db):
    fresh_db.create_view("foo_view", "select 1")
    assert ["foo_view"] == fresh_db.view_names()


def test_table_names_fts4(existing_db):
    existing_db["woo"].insert({"title": "Hello"}).enable_fts(
        ["title"], fts_version="FTS4"
    )
    existing_db["woo2"].insert({"title": "Hello"}).enable_fts(
        ["title"], fts_version="FTS5"
    )
    assert ["woo_fts"] == existing_db.table_names(fts4=True)
    assert ["woo2_fts"] == existing_db.table_names(fts5=True)


def test_detect_fts(existing_db):
    existing_db["woo"].insert({"title": "Hello"}).enable_fts(
        ["title"], fts_version="FTS4"
    )
    existing_db["woo2"].insert({"title": "Hello"}).enable_fts(
        ["title"], fts_version="FTS5"
    )
    assert "woo_fts" == existing_db["woo"].detect_fts()
    assert "woo_fts" == existing_db["woo_fts"].detect_fts()
    assert "woo2_fts" == existing_db["woo2"].detect_fts()
    assert "woo2_fts" == existing_db["woo2_fts"].detect_fts()
    assert existing_db["foo"].detect_fts() is None


@pytest.mark.parametrize("reverse_order", (True, False))
def test_detect_fts_similar_tables(fresh_db, reverse_order):
    # https://github.com/simonw/sqlite-utils/issues/434
    table1, table2 = ("demo", "demo2")
    if reverse_order:
        table1, table2 = table2, table1

    fresh_db[table1].insert({"title": "Hello"}).enable_fts(
        ["title"], fts_version="FTS4"
    )
    fresh_db[table2].insert({"title": "Hello"}).enable_fts(
        ["title"], fts_version="FTS4"
    )
    assert fresh_db[table1].detect_fts() == "{}_fts".format(table1)
    assert fresh_db[table2].detect_fts() == "{}_fts".format(table2)


def test_tables(existing_db):
    assert 1 == len(existing_db.tables)
    assert "foo" == existing_db.tables[0].name


def test_views(fresh_db):
    fresh_db.create_view("foo_view", "select 1")
    assert 1 == len(fresh_db.views)
    view = fresh_db.views[0]
    assert isinstance(view, View)
    assert "foo_view" == view.name
    assert "<View foo_view (1)>" == repr(view)
    assert {"1": str} == view.columns_dict


def test_count(existing_db):
    assert existing_db["foo"].count == 3
    assert existing_db["foo"].count_where() == 3
    assert existing_db["foo"].execute_count() == 3


def test_count_where(existing_db):
    assert existing_db["foo"].count_where("text != ?", ["two"]) == 2
    assert existing_db["foo"].count_where("text != :t", {"t": "two"}) == 2


def test_columns(existing_db):
    table = existing_db["foo"]
    assert [{"name": "text", "type": "TEXT"}] == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]


def test_table_schema(existing_db):
    assert existing_db["foo"].schema == "CREATE TABLE foo (text TEXT)"


def test_database_schema(existing_db):
    assert existing_db.schema == "CREATE TABLE foo (text TEXT);"


def test_table_repr(fresh_db):
    table = fresh_db["dogs"].insert({"name": "Cleo", "age": 4})
    assert "<Table dogs (name, age)>" == repr(table)
    assert "<Table cats (does not exist yet)>" == repr(fresh_db["cats"])


def test_indexes(fresh_db):
    fresh_db.executescript(
        """
        create table Gosh (c1 text, c2 text, c3 text);
        create index Gosh_c1 on Gosh(c1);
        create index Gosh_c2c3 on Gosh(c2, c3);
    """
    )
    assert [
        Index(
            seq=0,
            name="Gosh_c2c3",
            unique=0,
            origin="c",
            partial=0,
            columns=["c2", "c3"],
        ),
        Index(seq=1, name="Gosh_c1", unique=0, origin="c", partial=0, columns=["c1"]),
    ] == fresh_db["Gosh"].indexes


def test_xindexes(fresh_db):
    fresh_db.executescript(
        """
        create table Gosh (c1 text, c2 text, c3 text);
        create index Gosh_c1 on Gosh(c1);
        create index Gosh_c2c3 on Gosh(c2, c3 desc);
    """
    )
    assert fresh_db["Gosh"].xindexes == [
        XIndex(
            name="Gosh_c2c3",
            columns=[
                XIndexColumn(seqno=0, cid=1, name="c2", desc=0, coll="BINARY", key=1),
                XIndexColumn(seqno=1, cid=2, name="c3", desc=1, coll="BINARY", key=1),
                XIndexColumn(seqno=2, cid=-1, name=None, desc=0, coll="BINARY", key=0),
            ],
        ),
        XIndex(
            name="Gosh_c1",
            columns=[
                XIndexColumn(seqno=0, cid=0, name="c1", desc=0, coll="BINARY", key=1),
                XIndexColumn(seqno=1, cid=-1, name=None, desc=0, coll="BINARY", key=0),
            ],
        ),
    ]


@pytest.mark.parametrize(
    "column,expected_table_guess",
    (
        ("author", "authors"),
        ("author_id", "authors"),
        ("authors", "authors"),
        ("genre", "genre"),
        ("genre_id", "genre"),
    ),
)
def test_guess_foreign_table(fresh_db, column, expected_table_guess):
    fresh_db.create_table("authors", {"name": str})
    fresh_db.create_table("genre", {"name": str})
    assert expected_table_guess == fresh_db["books"].guess_foreign_table(column)


@pytest.mark.parametrize(
    "pk,expected", ((None, ["rowid"]), ("id", ["id"]), (["id", "id2"], ["id", "id2"]))
)
def test_pks(fresh_db, pk, expected):
    fresh_db["foo"].insert_all([{"id": 1, "id2": 2}], pk=pk)
    assert expected == fresh_db["foo"].pks


def test_triggers_and_triggers_dict(fresh_db):
    assert [] == fresh_db.triggers
    authors = fresh_db["authors"]
    authors.insert_all(
        [
            {"name": "Frank Herbert", "famous_works": "Dune"},
            {"name": "Neal Stephenson", "famous_works": "Cryptonomicon"},
        ]
    )
    fresh_db["other"].insert({"foo": "bar"})
    assert authors.triggers == []
    assert authors.triggers_dict == {}
    assert fresh_db["other"].triggers == []
    assert fresh_db.triggers_dict == {}
    authors.enable_fts(
        ["name", "famous_works"], fts_version="FTS4", create_triggers=True
    )
    expected_triggers = {
        ("authors_ai", "authors"),
        ("authors_ad", "authors"),
        ("authors_au", "authors"),
    }
    assert expected_triggers == {(t.name, t.table) for t in fresh_db.triggers}
    assert expected_triggers == {
        (t.name, t.table) for t in fresh_db["authors"].triggers
    }
    expected_triggers = {
        "authors_ai": (
            "CREATE TRIGGER [authors_ai] AFTER INSERT ON [authors] BEGIN\n"
            "  INSERT INTO [authors_fts] (rowid, [name], [famous_works]) VALUES (new.rowid, new.[name], new.[famous_works]);\n"
            "END"
        ),
        "authors_ad": (
            "CREATE TRIGGER [authors_ad] AFTER DELETE ON [authors] BEGIN\n"
            "  INSERT INTO [authors_fts] ([authors_fts], rowid, [name], [famous_works]) VALUES('delete', old.rowid, old.[name], old.[famous_works]);\n"
            "END"
        ),
        "authors_au": (
            "CREATE TRIGGER [authors_au] AFTER UPDATE ON [authors] BEGIN\n"
            "  INSERT INTO [authors_fts] ([authors_fts], rowid, [name], [famous_works]) VALUES('delete', old.rowid, old.[name], old.[famous_works]);\n"
            "  INSERT INTO [authors_fts] (rowid, [name], [famous_works]) VALUES (new.rowid, new.[name], new.[famous_works]);\nEND"
        ),
    }
    assert authors.triggers_dict == expected_triggers
    assert fresh_db["other"].triggers == []
    assert fresh_db["other"].triggers_dict == {}
    assert fresh_db.triggers_dict == expected_triggers


def test_has_counts_triggers(fresh_db):
    authors = fresh_db["authors"]
    authors.insert({"name": "Frank Herbert"})
    assert not authors.has_counts_triggers
    authors.enable_counts()
    assert authors.has_counts_triggers


@pytest.mark.parametrize(
    "sql,expected_name,expected_using",
    [
        (
            """
            CREATE VIRTUAL TABLE foo USING FTS5(name)
            """,
            "foo",
            "FTS5",
        ),
        (
            """
            CREATE VIRTUAL TABLE "foo" USING FTS4(name)
            """,
            "foo",
            "FTS4",
        ),
        (
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS `foo` USING FTS4(name)
            """,
            "foo",
            "FTS4",
        ),
        (
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS `foo` USING fts5(name)
            """,
            "foo",
            "FTS5",
        ),
        (
            """
            CREATE TABLE IF NOT EXISTS `foo` (id integer primary key)
            """,
            "foo",
            None,
        ),
    ],
)
def test_virtual_table_using(fresh_db, sql, expected_name, expected_using):
    fresh_db.execute(sql)
    assert fresh_db[expected_name].virtual_table_using == expected_using


def test_use_rowid(fresh_db):
    fresh_db["rowid_table"].insert({"name": "Cleo"})
    fresh_db["regular_table"].insert({"id": 1, "name": "Cleo"}, pk="id")
    assert fresh_db["rowid_table"].use_rowid
    assert not fresh_db["regular_table"].use_rowid


@pytest.mark.skipif(
    not Database(memory=True).supports_strict,
    reason="Needs SQLite version that supports strict",
)
@pytest.mark.parametrize(
    "create_table,expected_strict",
    (
        ("create table t (id integer) strict", True),
        ("create table t (id integer) STRICT", True),
        ("create table t (id integer primary key) StriCt, WITHOUT ROWID", True),
        ("create table t (id integer primary key) WITHOUT ROWID", False),
        ("create table t (id integer)", False),
    ),
)
def test_table_strict(fresh_db, create_table, expected_strict):
    fresh_db.execute(create_table)
    table = fresh_db["t"]
    assert table.strict == expected_strict


@pytest.mark.parametrize(
    "value",
    (
        1,
        1.3,
        "foo",
        True,
        b"binary",
    ),
)
def test_table_default_values(fresh_db, value):
    fresh_db["default_values"].insert(
        {"nodefault": 1, "value": value}, defaults={"value": value}
    )
    default_values = fresh_db["default_values"].default_values
    assert default_values == {"value": value}
