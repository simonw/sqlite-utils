import pytest

from sqlite_utils import Database


def test_insert_conversion(fresh_db):
    table = fresh_db.table("table")
    table.insert({"foo": "bar"}, conversions={"foo": "upper(?)"})
    assert [{"foo": "BAR"}] == list(table.rows)


def test_insert_all_conversion(fresh_db):
    table = fresh_db.table("table")
    table.insert_all([{"foo": "bar"}], conversions={"foo": "upper(?)"})
    assert [{"foo": "BAR"}] == list(table.rows)


def test_upsert_conversion(fresh_db):
    table = fresh_db.table("table")
    table.upsert({"id": 1, "foo": "bar"}, pk="id", conversions={"foo": "upper(?)"})
    assert [{"id": 1, "foo": "BAR"}] == list(table.rows)
    table.upsert(
        {"id": 1, "bar": "baz"}, pk="id", conversions={"bar": "upper(?)"}, alter=True
    )
    assert [{"id": 1, "foo": "BAR", "bar": "BAZ"}] == list(table.rows)


def test_upsert_all_conversion(fresh_db):
    table = fresh_db.table("table")
    table.upsert_all(
        [{"id": 1, "foo": "bar"}], pk="id", conversions={"foo": "upper(?)"}
    )
    assert [{"id": 1, "foo": "BAR"}] == list(table.rows)


@pytest.mark.parametrize("use_old_upsert", (False, True))
@pytest.mark.parametrize("method", ("upsert", "upsert_all"))
@pytest.mark.parametrize(
    "first,second,conversion,expected_first,expected_second",
    (
        (10, 12, "? * 100", 1000, 1200),
        ("first", "second", "? || '-stored'", "first-stored", "second-stored"),
        (
            '{"name":"first"}',
            '{"name":"second"}',
            "json_extract(?, '$.name')",
            "first",
            "second",
        ),
        ("first", "second", "? || '?'", "first?", "second?"),
    ),
    ids=("multiply", "append", "json-extract", "literal-question-mark"),
)
def test_upsert_conversion_once(
    use_old_upsert, method, first, second, conversion, expected_first, expected_second
):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db["records"]
    for value, expected in (
        (first, expected_first),
        (second, expected_second),
        (second, expected_second),
    ):
        record = {"id": 1, "value": value}
        getattr(table, method)(
            record if method == "upsert" else [record],
            pk="id",
            conversions={"value": conversion},
        )
        assert table.get(1) == {"id": 1, "value": expected}


@pytest.mark.parametrize("use_old_upsert", (False, True))
@pytest.mark.parametrize("batch_size", (1, 100))
def test_upsert_all_converted_values_mixed_insert_update(use_old_upsert, batch_size):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db["prices"]
    table.insert({"id": 1, "amount": 1000, "note": "original"}, pk="id")
    table.upsert_all(
        [
            {"id": 1, "amount": 12, "note": "updated"},
            {"id": 2, "amount": 20, "note": "new"},
        ],
        pk="id",
        conversions={"amount": "? * 100"},
        batch_size=batch_size,
    )
    assert list(table.rows_where(order_by="id")) == [
        {"id": 1, "amount": 1200, "note": "updated"},
        {"id": 2, "amount": 2000, "note": "new"},
    ]


def test_update_conversion(fresh_db):
    table = fresh_db.table("table")
    table.insert({"id": 5, "foo": "bar"}, pk="id")
    table.update(5, {"foo": "baz"}, conversions={"foo": "upper(?)"})
    assert [{"id": 5, "foo": "BAZ"}] == list(table.rows)


def test_table_constructor_conversion(fresh_db):
    table = fresh_db.table("table", conversions={"bar": "upper(?)"})
    table.insert({"bar": "baz"})
    assert [{"bar": "BAZ"}] == list(table.rows)
