from sqlite_utils.db import Index


def test_table_names(existing_db):
    assert ["foo"] == existing_db.table_names


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


def test_schema(existing_db):
    assert "CREATE TABLE foo (text TEXT)" == existing_db["foo"].schema


def test_table_repr(existing_db):
    assert "<Table foo>" == repr(existing_db["foo"])


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
