from sqlite_utils.db import Index
import pytest


@pytest.mark.parametrize("use_table_factory", [True, False])
def test_extracts(fresh_db, use_table_factory):
    kwargs = dict(extracts={"species_id": "Species"})
    table_kwargs = {}
    insert_kwargs = {}
    if use_table_factory:
        table_kwargs = kwargs
    else:
        insert_kwargs = kwargs
    trees = fresh_db.table("Trees", **table_kwargs)
    trees.insert_all(
        [
            {"id": 1, "species_id": "Oak"},
            {"id": 2, "species_id": "Oak"},
            {"id": 3, "species_id": "Palm"},
        ],
        **insert_kwargs
    )
    # Should now have two tables: Trees and Species
    assert {"Species", "Trees"} == set(fresh_db.table_names())
    assert (
        "CREATE TABLE [Species] (\n   [id] INTEGER PRIMARY KEY,\n   [value] TEXT\n)"
        == fresh_db["Species"].schema
    )
    assert (
        "CREATE TABLE [Trees] (\n   [id] INTEGER,\n   [species_id] INTEGER REFERENCES [Species]([id])\n)"
        == fresh_db["Trees"].schema
    )
    # Should have unique index on Species
    assert [
        Index(
            seq=0,
            name="idx_Species_value",
            unique=1,
            origin="c",
            partial=0,
            columns=["value"],
        )
    ] == fresh_db["Species"].indexes
    # Finally, check the rows
    assert [{"id": 1, "value": "Oak"}, {"id": 2, "value": "Palm"}] == list(
        fresh_db["Species"].rows
    )
    assert [
        {"id": 1, "species_id": 1},
        {"id": 2, "species_id": 1},
        {"id": 3, "species_id": 2},
    ] == list(fresh_db["Trees"].rows)
