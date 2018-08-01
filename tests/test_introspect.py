from .fixtures import existing_db


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
