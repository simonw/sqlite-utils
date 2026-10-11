import pytest


@pytest.fixture
def db(fresh_db):
    fresh_db.table("one_index").insert({"id": 1, "name": "Cleo"}, pk="id")
    fresh_db.table("one_index").create_index(["name"])
    fresh_db.table("two_indexes").insert(
        {"id": 1, "name": "Cleo", "species": "dog"}, pk="id"
    )
    fresh_db.table("two_indexes").create_index(["name"])
    fresh_db.table("two_indexes").create_index(["species"])
    return fresh_db


def test_analyze_whole_database(db):
    assert set(db.table_names()) == {"one_index", "two_indexes"}
    db.analyze()
    assert set(db.table_names()).issuperset(
        {"one_index", "two_indexes", "sqlite_stat1"}
    )
    assert list(db.table("sqlite_stat1").rows) == [
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
        db.table("one_index").analyze()

    assert set(db.table_names()).issuperset(
        {"one_index", "two_indexes", "sqlite_stat1"}
    )
    assert list(db.table("sqlite_stat1").rows) == [
        {"tbl": "one_index", "idx": "idx_one_index_name", "stat": "1 1"}
    ]


def test_analyze_index_by_name(db):
    assert set(db.table_names()) == {"one_index", "two_indexes"}
    db.analyze("idx_two_indexes_species")
    assert set(db.table_names()).issuperset(
        {"one_index", "two_indexes", "sqlite_stat1"}
    )
    assert list(db.table("sqlite_stat1").rows) == [
        {"tbl": "two_indexes", "idx": "idx_two_indexes_species", "stat": "1 1"},
    ]


@pytest.mark.parametrize(
    "values,expected_num_distinct",
    (
        # count(distinct col) skips nulls, which is why a column of nulls
        # reports no distinct values at all
        ([None, None, None], 0),
        ([None, None, "a", "a"], 1),
        (["a", "a", "b", "b"], 2),
    ),
)
def test_analyze_column_num_distinct_ignores_nulls(
    fresh_db, values, expected_num_distinct
):
    fresh_db["t"].insert_all([{"c": value} for value in values])
    assert fresh_db["t"].analyze_column("c").num_distinct == expected_num_distinct


@pytest.mark.parametrize("value_truncate", (None, 80))
def test_analyze_column_most_common_includes_nulls(fresh_db, value_truncate):
    # One distinct non-null value plus nulls used to take the
    # "single distinct value" shortcut, which reported that one value
    # with a count of total_rows and left the nulls out entirely
    fresh_db["t"].insert_all([{"c": None}, {"c": None}, {"c": "a"}, {"c": "a"}])
    details = fresh_db["t"].analyze_column(
        "c", total_rows=4, value_truncate=value_truncate
    )
    assert dict(details.most_common) == {None: 2, "a": 2}


@pytest.mark.parametrize("value_truncate", (None, 80))
def test_analyze_column_most_common_ties_with_nulls(fresh_db, value_truncate):
    # A null and a string with the same count used to be compared
    # directly by the tie-break sort, which raised TypeError
    fresh_db["t"].insert_all(
        [{"c": None}, {"c": None}, {"c": "a"}, {"c": "a"}, {"c": "b"}]
    )
    details = fresh_db["t"].analyze_column("c", value_truncate=value_truncate)
    assert dict(details.most_common) == {None: 2, "a": 2, "b": 1}


def test_analyze_column_least_common_ties_with_nulls(fresh_db):
    # A null and a string share the lowest count, so the tie-break has
    # to order them without comparing None against a string
    fresh_db["t"].insert_all(
        [{"c": None}, {"c": "a"}] + [{"c": c} for c in "bcdefg" for _ in range(2)]
    )
    details = fresh_db["t"].analyze_column("c", common_limit=5)
    assert dict(details.least_common) == {
        None: 1,
        "a": 1,
        "e": 2,
        "f": 2,
        "g": 2,
    }


def test_analyze_column_does_not_render_null_as_the_string_none(fresh_db):
    # analyze-tables passes value_truncate=80, and the str() of a null
    # was indistinguishable from a column that really holds "None"
    fresh_db["t"].insert_all([{"c": None}, {"c": None}, {"c": "None"}])
    details = fresh_db["t"].analyze_column("c", total_rows=3, value_truncate=80)
    assert dict(details.most_common) == {None: 2, "None": 1}
