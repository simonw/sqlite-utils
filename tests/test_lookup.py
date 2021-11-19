from sqlite_utils.db import Index
import pytest


def test_lookup_new_table(fresh_db):
    species = fresh_db["species"]
    palm_id = species.lookup({"name": "Palm"})
    oak_id = species.lookup({"name": "Oak"})
    cherry_id = species.lookup({"name": "Cherry"})
    assert palm_id == species.lookup({"name": "Palm"})
    assert oak_id == species.lookup({"name": "Oak"})
    assert cherry_id == species.lookup({"name": "Cherry"})
    assert palm_id != oak_id != cherry_id
    # Ensure the correct indexes were created
    assert [
        Index(
            seq=0,
            name="idx_species_name",
            unique=1,
            origin="c",
            partial=0,
            columns=["name"],
        )
    ] == species.indexes


def test_lookup_new_table_compound_key(fresh_db):
    species = fresh_db["species"]
    palm_id = species.lookup({"name": "Palm", "type": "Tree"})
    oak_id = species.lookup({"name": "Oak", "type": "Tree"})
    assert palm_id == species.lookup({"name": "Palm", "type": "Tree"})
    assert oak_id == species.lookup({"name": "Oak", "type": "Tree"})
    assert [
        Index(
            seq=0,
            name="idx_species_name_type",
            unique=1,
            origin="c",
            partial=0,
            columns=["name", "type"],
        )
    ] == species.indexes


def test_lookup_adds_unique_constraint_to_existing_table(fresh_db):
    species = fresh_db.table("species", pk="id")
    palm_id = species.insert({"name": "Palm"}).last_pk
    species.insert({"name": "Oak"})
    assert [] == species.indexes
    assert palm_id == species.lookup({"name": "Palm"})
    assert [
        Index(
            seq=0,
            name="idx_species_name",
            unique=1,
            origin="c",
            partial=0,
            columns=["name"],
        )
    ] == species.indexes


def test_lookup_fails_if_constraint_cannot_be_added(fresh_db):
    species = fresh_db.table("species", pk="id")
    species.insert_all([{"id": 1, "name": "Palm"}, {"id": 2, "name": "Palm"}])
    # This will fail because the name column is not unique
    with pytest.raises(Exception, match="UNIQUE constraint failed"):
        species.lookup({"name": "Palm"})


def test_lookup_with_extra_values(fresh_db):
    species = fresh_db["species"]
    id = species.lookup({"name": "Palm", "type": "Tree"}, {"first_seen": "2020-01-01"})
    assert species.get(id) == {
        "id": 1,
        "name": "Palm",
        "type": "Tree",
        "first_seen": "2020-01-01",
    }
    # A subsequent lookup() should ignore the second dictionary
    id2 = species.lookup({"name": "Palm", "type": "Tree"}, {"first_seen": "2021-02-02"})
    assert id2 == id
    assert species.get(id2) == {
        "id": 1,
        "name": "Palm",
        "type": "Tree",
        "first_seen": "2020-01-01",
    }


def test_lookup_with_extra_insert_parameters(fresh_db):
    other_table = fresh_db["other_table"]
    other_table.insert({"id": 1, "name": "Name"}, pk="id")
    species = fresh_db["species"]
    id = species.lookup(
        {"name": "Palm", "type": "Tree"},
        {
            "first_seen": "2020-01-01",
            "make_not_null": 1,
            "fk_to_other": 1,
            "default_is_dog": "cat",
            "extract_this": "This is extracted",
            "convert_to_upper": "upper",
            "make_this_integer": "2",
            "this_at_front": 1,
        },
        pk="renamed_id",
        foreign_keys=(("fk_to_other", "other_table", "id"),),
        column_order=("this_at_front",),
        not_null={"make_not_null"},
        defaults={"default_is_dog": "dog"},
        extracts=["extract_this"],
        conversions={"convert_to_upper": "upper(?)"},
        columns={"make_this_integer": int},
    )
    assert species.schema == (
        "CREATE TABLE [species] (\n"
        "   [renamed_id] INTEGER PRIMARY KEY,\n"
        "   [this_at_front] INTEGER,\n"
        "   [name] TEXT,\n"
        "   [type] TEXT,\n"
        "   [first_seen] TEXT,\n"
        "   [make_not_null] INTEGER NOT NULL,\n"
        "   [fk_to_other] INTEGER REFERENCES [other_table]([id]),\n"
        "   [default_is_dog] TEXT DEFAULT 'dog',\n"
        "   [extract_this] INTEGER REFERENCES [extract_this]([id]),\n"
        "   [convert_to_upper] TEXT,\n"
        "   [make_this_integer] INTEGER\n"
        ")"
    )
    assert species.get(id) == {
        "renamed_id": id,
        "this_at_front": 1,
        "name": "Palm",
        "type": "Tree",
        "first_seen": "2020-01-01",
        "make_not_null": 1,
        "fk_to_other": 1,
        "default_is_dog": "cat",
        "extract_this": 1,
        "convert_to_upper": "UPPER",
        "make_this_integer": 2,
    }
    assert species.indexes == [
        Index(
            seq=0,
            name="idx_species_name_type",
            unique=1,
            origin="c",
            partial=0,
            columns=["name", "type"],
        )
    ]
