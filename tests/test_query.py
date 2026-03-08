import types


def test_query(fresh_db):
    fresh_db["dogs"].insert_all([{"name": "Cleo"}, {"name": "Pancakes"}])
    results = fresh_db.query("select * from dogs order by name desc")
    assert isinstance(results, types.GeneratorType)
    assert list(results) == [{"name": "Pancakes"}, {"name": "Cleo"}]


def test_execute_returning_dicts(fresh_db):
    # Like db.query() but returns a list, included for backwards compatibility
    # see https://github.com/simonw/sqlite-utils/issues/290
    fresh_db["test"].insert({"id": 1, "bar": 2}, pk="id")
    assert fresh_db.execute_returning_dicts("select * from test") == [
        {"id": 1, "bar": 2}
    ]


def test_query_duplicate_output_columns_are_suffixed(fresh_db):
    fresh_db.execute("create table one (id integer, value text)")
    fresh_db.execute("create table two (id integer, value text)")
    fresh_db["one"].insert({"id": 1, "value": "left"})
    fresh_db["two"].insert({"id": 2, "value": "right"})

    rows = list(
        fresh_db.query(
            "select one.id, two.id, one.value, two.value from one, two where one.id = 1"
        )
    )

    assert rows == [{"id": 1, "id_2": 2, "value": "left", "value_2": "right"}]
