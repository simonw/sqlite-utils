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
    assert expected_ids == {r["id"] for r in table.rows_where(where, where_args)}
