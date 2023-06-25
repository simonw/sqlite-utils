import pytest


@pytest.fixture
def db(fresh_db):
    fresh_db["one_index"].insert({"id": 1, "name": "Cleo"}, pk="id")
    fresh_db["one_index"].create_index(["name"])
    fresh_db["two_indexes"].insert({"id": 1, "name": "Cleo", "species": "dog"}, pk="id")
    fresh_db["two_indexes"].create_index(["name"])
    fresh_db["two_indexes"].create_index(["species"])
    return fresh_db


def test_analyze_whole_database(db):
    assert set(db.table_names()) == {"one_index", "two_indexes"}
    db.analyze()
    assert set(db.table_names()).issuperset(
        {"one_index", "two_indexes", "sqlite_stat1"}
    )
    assert list(db["sqlite_stat1"].rows) == [
        {"tbl": "two_indexes", "idx": "idx_two_indexes_species", "stat": "1 1"},
        {"tbl": "two_indexes", "idx": "idx_two_indexes_name", "stat": "1 1"},
        {"tbl": "one_index", "idx": "idx_one_index_name", "stat": "1 1"},
    ]


@pytest.mark.parametrize("method", ("db_method_with_name", "table_method"))
def test_analyze_one_table(db, method):
    assert set(db.table_names()).issuperset({"one_index", "two_indexes"})
    if method == "db_method_with_name":
        db.analyze("one_index")
    elif method == "table_method":
        db["one_index"].analyze()

    assert set(db.table_names()).issuperset(
        {"one_index", "two_indexes", "sqlite_stat1"}
    )
    assert list(db["sqlite_stat1"].rows) == [
        {"tbl": "one_index", "idx": "idx_one_index_name", "stat": "1 1"}
    ]


def test_analyze_index_by_name(db):
    assert set(db.table_names()) == {"one_index", "two_indexes"}
    db.analyze("idx_two_indexes_species")
    assert set(db.table_names()).issuperset(
        {"one_index", "two_indexes", "sqlite_stat1"}
    )
    assert list(db["sqlite_stat1"].rows) == [
        {"tbl": "two_indexes", "idx": "idx_two_indexes_species", "stat": "1 1"},
    ]
