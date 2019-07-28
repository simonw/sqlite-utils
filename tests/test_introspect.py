from sqlite_utils.db import Index
import pytest


def test_table_names(existing_db):
    assert ["foo"] == existing_db.table_names()


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
    assert None == existing_db["foo"].detect_fts()


def test_tables(existing_db):
    assert 1 == len(existing_db.tables)
    assert "foo" == existing_db.tables[0].name


def test_count(existing_db):
    assert 3 == existing_db["foo"].count


def test_columns(existing_db):
    table = existing_db["foo"]
    assert [{"name": "text", "type": "TEXT"}] == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]


def test_rows(existing_db):
    assert [{"text": "one"}, {"text": "two"}, {"text": "three"}] == list(
        existing_db["foo"].rows
    )


def test_schema(existing_db):
    assert "CREATE TABLE foo (text TEXT)" == existing_db["foo"].schema


def test_table_repr(fresh_db):
    table = fresh_db["dogs"].insert({"name": "Cleo", "age": 4})
    assert "<Table dogs (name, age)>" == repr(table)
    assert "<Table cats (does not exist yet)>" == repr(fresh_db["cats"])


def test_indexes(fresh_db):
    fresh_db.conn.executescript(
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
