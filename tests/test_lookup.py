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
