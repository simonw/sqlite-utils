from sqlite_utils.db import Index, View
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
