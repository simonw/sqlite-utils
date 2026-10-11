import pytest

from sqlite_utils import Database
from sqlite_utils.db import PrimaryKeyRequired


@pytest.mark.parametrize("use_old_upsert", (False, True))
@pytest.mark.parametrize("batch_size", (1, 2, 100))
def test_upsert_all_preserves_omitted_fields(use_old_upsert, batch_size):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("dogs")
    table.insert_all(
        [{"id": 1, "name": "Cleo", "age": 5}, {"id": 2, "name": "Nixie", "age": 6}],
        pk="id",
    )
    table.upsert_all(
        iter([{"id": 1, "age": 7}, {"id": 2, "name": "Nixie II"}]),
        batch_size=batch_size,
    )
    assert list(table.rows_where(order_by="id")) == [
        {"id": 1, "name": "Cleo", "age": 7},
        {"id": 2, "name": "Nixie II", "age": 6},
    ]
    assert table.last_pk is None


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_all_preserves_mixed_field_record_order(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("dogs")
    table.insert({"id": 1, "name": "Cleo", "age": 5}, pk="id")
    db.executescript("""
        create table audit (age integer);
        create trigger record_age after update on dogs begin
            insert into audit (age) values (new.age);
        end;
        """)
    table.upsert_all(
        [{"id": 1, "age": 7}, {"id": 1, "name": "Cleo II"}, {"id": 1, "age": 8}]
    )
    assert table.get(1) == {"id": 1, "name": "Cleo II", "age": 8}
    assert list(db["audit"].rows) == [{"age": 7}, {"age": 7}, {"age": 8}]


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_all_omission_differs_from_explicit_null(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("dogs")
    table.create(
        {"id": int, "name": str, "age": int}, pk="id", defaults={"name": "Unknown"}
    )
    table.insert({"id": 1, "name": "Cleo", "age": 5})
    table.upsert_all([{"id": 1, "name": None}, {"id": 2, "age": 6}])
    assert list(table.rows_where(order_by="id")) == [
        {"id": 1, "name": None, "age": 5},
        {"id": 2, "name": "Unknown", "age": 6},
    ]


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_all_mixed_fields_invalid_pk_rolls_back_batch(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("dogs")
    original = {"id": 1, "name": "Cleo", "age": 5}
    table.insert(original, pk="id")
    with pytest.raises(PrimaryKeyRequired):
        table.upsert_all([{"id": 1, "age": 7}, {"name": "Invalid"}])
    assert list(table.rows) == [original]


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_all_mixed_fields_hash_id(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("dogs")
    table.upsert_all(
        [{"name": "Cleo", "age": 5, "color": "black"}], hash_id_columns=["name"]
    )
    original_id = table.last_pk
    table.upsert_all(
        [{"name": "Cleo", "age": 7}, {"name": "Cleo", "color": "brown"}],
        hash_id_columns=["name"],
    )
    assert list(table.rows) == [
        {"id": original_id, "name": "Cleo", "age": 7, "color": "brown"}
    ]


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_all_mixed_fields_alter_infers_types_from_whole_batch(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("items").create({"id": int}, pk="id")
    table.upsert_all(
        [{"id": 1, "code": 1}, {"id": 2, "code": "001", "note": "leading zeros"}],
        alter=True,
    )
    assert table.columns_dict["code"] is str
    assert list(table.rows_where(order_by="id")) == [
        {"id": 1, "code": "1", "note": None},
        {"id": 2, "code": "001", "note": "leading zeros"},
    ]


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_all_mixed_fields_compound_pk_and_conversions(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("pets")
    table.insert_all(
        [
            {"species": "dog", "id": 1, "name": "Cleo", "age": 5},
            {"species": "cat", "id": 1, "name": "Nixie", "age": 6},
        ],
        pk=("species", "id"),
    )
    table.upsert_all(
        [
            {"species": "dog", "id": 1, "age": 7},
            {"species": "cat", "id": 1, "name": "new"},
        ],
        conversions={"name": "upper(?)"},
    )
    assert table.get(("dog", 1)) == {
        "species": "dog",
        "id": 1,
        "name": "Cleo",
        "age": 7,
    }
    assert table.get(("cat", 1)) == {"species": "cat", "id": 1, "name": "NEW", "age": 6}


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("table")
    table.insert_all([{"id": 1, "name": "Cleo"}], pk="id", replace=True)
    table.upsert({"id": 1, "age": 5}, pk="id", alter=True)
    assert list(table.rows) == [{"id": 1, "name": "Cleo", "age": 5}]
    assert table.last_pk == 1


def test_upsert_all(fresh_db):
    table = fresh_db.table("table")
    table.upsert_all([{"id": 1, "name": "Cleo"}, {"id": 2, "name": "Nixie"}], pk="id")
    table.upsert_all([{"id": 1, "age": 5}, {"id": 2, "age": 5}], pk="id", alter=True)
    assert list(table.rows) == [
        {"id": 1, "name": "Cleo", "age": 5},
        {"id": 2, "name": "Nixie", "age": 5},
    ]
    assert table.last_pk is None


def test_upsert_all_single_column(fresh_db):
    table = fresh_db.table("table")
    table.upsert_all([{"name": "Cleo"}], pk="name")
    assert list(table.rows) == [{"name": "Cleo"}]
    assert table.pks == ["name"]


def test_upsert_all_not_null(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/538
    fresh_db.table("comments").upsert_all(
        [{"id": 1, "name": "Cleo"}],
        pk="id",
        not_null=["name"],
    )
    assert list(fresh_db.table("comments").rows) == [{"id": 1, "name": "Cleo"}]


def test_upsert_error_if_no_pk(fresh_db):
    table = fresh_db.table("table")
    with pytest.raises(PrimaryKeyRequired):
        table.upsert_all([{"id": 1, "name": "Cleo"}])
    with pytest.raises(PrimaryKeyRequired):
        table.upsert({"id": 1, "name": "Cleo"})


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_empty_record_errors(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("table")
    table.insert({"id": 1, "name": "Cleo"}, pk="id")
    with pytest.raises(PrimaryKeyRequired):
        table.upsert({}, pk="id")
    with pytest.raises(PrimaryKeyRequired):
        table.upsert_all([{}, {}], pk="id")
    # No rows can have been inserted
    assert table.count == 1


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_missing_pk_value_errors(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    table = db.table("table")
    table.insert({"id": 1, "name": "Cleo"}, pk="id")
    # Records that omit the pk column entirely
    with pytest.raises(PrimaryKeyRequired):
        table.upsert_all([{"name": "Pancakes"}, {"name": "Marnie"}], pk="id")
    # A record with an explicit None pk value can never conflict
    with pytest.raises(PrimaryKeyRequired):
        table.upsert({"id": None, "name": "Pancakes"}, pk="id")
    assert list(table.rows) == [{"id": 1, "name": "Cleo"}]


def test_upsert_missing_compound_pk_value_errors(fresh_db):
    table = fresh_db.table("table")
    table.insert({"a": "x", "b": "y", "v": 1}, pk=("a", "b"))
    # Missing one component of the detected compound primary key
    with pytest.raises(PrimaryKeyRequired):
        table.upsert({"a": "x", "v": 2})
    assert list(table.rows) == [{"a": "x", "b": "y", "v": 1}]


def test_upsert_error_if_existing_table_has_no_pk(fresh_db):
    table = fresh_db.create_table("table", {"id": int, "name": str})
    with pytest.raises(PrimaryKeyRequired):
        table.upsert({"id": 1, "name": "Cleo"})


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_uses_compound_pk_from_existing_table(use_old_upsert):
    # https://github.com/simonw/sqlite-utils/issues/629
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    db.execute("""
        create table summary (
            Source text,
            Object text,
            Category text,
            Count integer,
            primary key (Source, Object, Category)
        )
        """)
    table = db.table("summary")
    table.upsert(
        {
            "Source": "Client A",
            "Object": "Accounts",
            "Category": "All",
            "Count": 3,
        }
    )
    assert table.last_pk == ("Client A", "Accounts", "All")
    table.upsert(
        {
            "Source": "Client A",
            "Object": "Accounts",
            "Category": "All",
            "Count": 4,
        }
    )
    assert list(table.rows) == [
        {
            "Source": "Client A",
            "Object": "Accounts",
            "Category": "All",
            "Count": 4,
        }
    ]


def test_upsert_with_hash_id(fresh_db):
    table = fresh_db.table("table")
    table.upsert({"foo": "bar"}, hash_id="pk")
    assert [{"pk": "a5e744d0164540d33b1d7ea616c28f2fa97e754a", "foo": "bar"}] == list(
        table.rows
    )
    assert "a5e744d0164540d33b1d7ea616c28f2fa97e754a" == table.last_pk


@pytest.mark.parametrize("hash_id", (None, "custom_id"))
def test_upsert_with_hash_id_columns(fresh_db, hash_id):
    table = fresh_db.table("table")
    table.upsert({"a": 1, "b": 2, "c": 3}, hash_id=hash_id, hash_id_columns=("a", "b"))
    assert list(table.rows) == [
        {
            hash_id or "id": "4acc71e0547112eb432f0a36fb1924c4a738cb49",
            "a": 1,
            "b": 2,
            "c": 3,
        }
    ]
    assert table.last_pk == "4acc71e0547112eb432f0a36fb1924c4a738cb49"
    table.upsert({"a": 1, "b": 2, "c": 4}, hash_id=hash_id, hash_id_columns=("a", "b"))
    assert list(table.rows) == [
        {
            hash_id or "id": "4acc71e0547112eb432f0a36fb1924c4a738cb49",
            "a": 1,
            "b": 2,
            "c": 4,
        }
    ]


def test_upsert_compound_primary_key(fresh_db):
    table = fresh_db.table("table")
    table.upsert_all(
        [
            {"species": "dog", "id": 1, "name": "Cleo", "age": 4},
            {"species": "cat", "id": 1, "name": "Catbag"},
        ],
        pk=("species", "id"),
    )
    assert table.last_pk is None
    table.upsert({"species": "dog", "id": 1, "age": 5}, pk=("species", "id"))
    assert ("dog", 1) == table.last_pk
    assert [
        {"species": "dog", "id": 1, "name": "Cleo", "age": 5},
        {"species": "cat", "id": 1, "name": "Catbag", "age": None},
    ] == list(table.rows)
    # .upsert_all() with a single item should set .last_pk
    table.upsert_all([{"species": "cat", "id": 1, "age": 5}], pk=("species", "id"))
    assert ("cat", 1) == table.last_pk
