import pytest


def test_rows(existing_db):
    assert [{"text": "one"}, {"text": "two"}, {"text": "three"}] == list(
        existing_db["foo"].rows
    )


@pytest.mark.parametrize(
    "where,where_args,expected_ids",
    [
        ("name = ?", ["Pancakes"], {2}),
        ("age > ?", [3], {1}),
        ("age > :age", {"age": 3}, {1}),
        ("name is not null", [], {1, 2}),
        ("is_good = ?", [True], {1, 2}),
    ],
)
def test_rows_where(where, where_args, expected_ids, fresh_db):
    table = fresh_db["dogs"]
    table.insert_all(
        [
            {"id": 1, "name": "Cleo", "age": 4, "is_good": True},
            {"id": 2, "name": "Pancakes", "age": 3, "is_good": True},
        ],
        pk="id",
    )
    assert expected_ids == {
        r["id"] for r in table.rows_where(where, where_args, select="id")
    }


@pytest.mark.parametrize(
    "where,order_by,expected_ids",
    [
        (None, None, [1, 2, 3]),
        (None, "id desc", [3, 2, 1]),
        (None, "age", [3, 2, 1]),
        ("id > 1", "age", [3, 2]),
    ],
)
def test_rows_where_order_by(where, order_by, expected_ids, fresh_db):
    table = fresh_db["dogs"]
    table.insert_all(
        [
            {"id": 1, "name": "Cleo", "age": 4},
            {"id": 2, "name": "Pancakes", "age": 3},
            {"id": 3, "name": "Bailey", "age": 2},
        ],
        pk="id",
    )
    assert expected_ids == [r["id"] for r in table.rows_where(where, order_by=order_by)]


@pytest.mark.parametrize(
    "offset,limit,expected",
    [
        (None, 3, [1, 2, 3]),
        (0, 3, [1, 2, 3]),
        (3, 3, [4, 5, 6]),
    ],
)
def test_rows_where_offset_limit(fresh_db, offset, limit, expected):
    table = fresh_db["rows"]
    table.insert_all([{"id": id} for id in range(1, 101)], pk="id")
    assert table.count == 100
    assert expected == [
        r["id"] for r in table.rows_where(offset=offset, limit=limit, order_by="id")
    ]


def test_pks_and_rows_where_rowid(fresh_db):
    table = fresh_db["rowid_table"]
    table.insert_all({"number": i + 10} for i in range(3))
    pks_and_rows = list(table.pks_and_rows_where())
    assert pks_and_rows == [
        (1, {"rowid": 1, "number": 10}),
        (2, {"rowid": 2, "number": 11}),
        (3, {"rowid": 3, "number": 12}),
    ]


def test_pks_and_rows_where_simple_pk(fresh_db):
    table = fresh_db["simple_pk_table"]
    table.insert_all(({"id": i + 10} for i in range(3)), pk="id")
    pks_and_rows = list(table.pks_and_rows_where())
    assert pks_and_rows == [
        (10, {"id": 10}),
        (11, {"id": 11}),
        (12, {"id": 12}),
    ]


def test_pks_and_rows_where_compound_pk(fresh_db):
    table = fresh_db["compound_pk_table"]
    table.insert_all(
        ({"type": "number", "number": i, "plusone": i + 1} for i in range(3)),
        pk=("type", "number"),
    )
    pks_and_rows = list(table.pks_and_rows_where())
    assert pks_and_rows == [
        (("number", 0), {"type": "number", "number": 0, "plusone": 1}),
        (("number", 1), {"type": "number", "number": 1, "plusone": 2}),
        (("number", 2), {"type": "number", "number": 2, "plusone": 3}),
    ]


def test_rows_where_duplicate_select_columns_are_deduped(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/624
    fresh_db["t"].insert({"id": 1, "name": "Cleo"})
    rows = list(fresh_db["t"].rows_where(select="id, id, name"))
    assert rows == [{"id": 1, "id_2": 1, "name": "Cleo"}]


def test_pks_and_rows_where_view(fresh_db):
    # pks_and_rows_where() lives on Queryable so views expose it, but
    # SQLite views have no rowid. Modern SQLite (3.36+) raises an
    # OperationalError from the generated SQL; older versions returned
    # NULL for a view's rowid. Either way it must not fail earlier with
    # an AttributeError from View lacking Table-only properties
    from sqlite_utils.utils import sqlite3

    fresh_db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    fresh_db.create_view("dog_names", "select name from dogs")
    try:
        result = list(fresh_db["dog_names"].pks_and_rows_where())
    except sqlite3.OperationalError:
        pass  # SQLite 3.36+: no such column: rowid
    else:
        # Older SQLite returns NULL rowids for views
        assert result == [(None, {"rowid": None, "name": "Cleo"})]


def test_pks_and_rows_where_compound_pk_declaration_order(fresh_db):
    # Compound pks are returned in PRIMARY KEY declaration order
    fresh_db.execute("create table t (b text, a text, primary key (a, b))")
    fresh_db["t"].insert({"a": "A", "b": "B"})
    pks_and_rows = list(fresh_db["t"].pks_and_rows_where())
    assert pks_and_rows == [(("A", "B"), {"b": "B", "a": "A"})]
