from sqlite_utils.db import Index, ForeignKey
import pytest


@pytest.mark.parametrize(
    "kwargs,expected_table",
    [
        (dict(extracts={"species_id": "Species"}), "Species"),
        (dict(extracts=["species_id"]), "species_id"),
        (dict(extracts=("species_id",)), "species_id"),
    ],
)
@pytest.mark.parametrize("use_table_factory", [True, False])
def test_extracts(fresh_db, kwargs, expected_table, use_table_factory):
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
    assert {expected_table, "Trees"} == set(fresh_db.table_names())
    assert (
        "CREATE TABLE [{}] (\n   [id] INTEGER PRIMARY KEY,\n   [value] TEXT\n)".format(
            expected_table
        )
        == fresh_db[expected_table].schema
    )
    assert (
        "CREATE TABLE [Trees] (\n   [id] INTEGER,\n   [species_id] INTEGER REFERENCES [{}]([id])\n)".format(
            expected_table
        )
        == fresh_db["Trees"].schema
    )
    # Should have a foreign key reference
    assert len(fresh_db["Trees"].foreign_keys) == 1
    fk = fresh_db["Trees"].foreign_keys[0]
    assert fk.table == "Trees"
    assert fk.column == "species_id"

    # Should have unique index on Species
    assert [
        Index(
            seq=0,
            name="idx_{}_value".format(expected_table),
            unique=1,
            origin="c",
            partial=0,
            columns=["value"],
        )
    ] == fresh_db[expected_table].indexes
    # Finally, check the rows
    assert [{"id": 1, "value": "Oak"}, {"id": 2, "value": "Palm"}] == list(
        fresh_db[expected_table].rows
    )
    assert [
        {"id": 1, "species_id": 1},
        {"id": 2, "species_id": 1},
        {"id": 3, "species_id": 2},
    ] == list(fresh_db["Trees"].rows)
