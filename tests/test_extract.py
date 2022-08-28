from sqlite_utils.db import InvalidColumns
import itertools
import pytest


@pytest.mark.parametrize("table", [None, "Species"])
@pytest.mark.parametrize("fk_column", [None, "species"])
def test_extract_single_column(fresh_db, table, fk_column):
    expected_table = table or "species"
    expected_fk = fk_column or "{}_id".format(expected_table)
    iter_species = itertools.cycle(["Palm", "Spruce", "Mangrove", "Oak"])
    fresh_db["tree"].insert_all(
        (
            {
                "id": i,
                "name": "Tree {}".format(i),
                "species": next(iter_species),
                "end": 1,
            }
            for i in range(1, 1001)
        ),
        pk="id",
    )
    fresh_db["tree"].extract("species", table=table, fk_column=fk_column)
    assert fresh_db["tree"].schema == (
        'CREATE TABLE "tree" (\n'
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT,\n"
        "   [{}] INTEGER,\n".format(expected_fk)
        + "   [end] INTEGER,\n"
        + "   FOREIGN KEY([{}]) REFERENCES [{}]([id])\n".format(
            expected_fk, expected_table
        )
        + ")"
    )
    assert fresh_db[expected_table].schema == (
        "CREATE TABLE [{}] (\n".format(expected_table)
        + "   [id] INTEGER PRIMARY KEY,\n"
        "   [species] TEXT\n"
        ")"
    )
    assert list(fresh_db[expected_table].rows) == [
        {"id": 1, "species": "Palm"},
        {"id": 2, "species": "Spruce"},
        {"id": 3, "species": "Mangrove"},
        {"id": 4, "species": "Oak"},
    ]
    assert list(itertools.islice(fresh_db["tree"].rows, 0, 4)) == [
        {"id": 1, "name": "Tree 1", expected_fk: 1, "end": 1},
        {"id": 2, "name": "Tree 2", expected_fk: 2, "end": 1},
        {"id": 3, "name": "Tree 3", expected_fk: 3, "end": 1},
        {"id": 4, "name": "Tree 4", expected_fk: 4, "end": 1},
    ]


def test_extract_multiple_columns_with_rename(fresh_db):
    iter_common = itertools.cycle(["Palm", "Spruce", "Mangrove", "Oak"])
    iter_latin = itertools.cycle(["Arecaceae", "Picea", "Rhizophora", "Quercus"])
    fresh_db["tree"].insert_all(
        (
            {
                "id": i,
                "name": "Tree {}".format(i),
                "common_name": next(iter_common),
                "latin_name": next(iter_latin),
            }
            for i in range(1, 1001)
        ),
        pk="id",
    )

    fresh_db["tree"].extract(
        ["common_name", "latin_name"], rename={"common_name": "name"}
    )
    assert fresh_db["tree"].schema == (
        'CREATE TABLE "tree" (\n'
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT,\n"
        "   [common_name_latin_name_id] INTEGER,\n"
        "   FOREIGN KEY([common_name_latin_name_id]) REFERENCES [common_name_latin_name]([id])\n"
        ")"
    )
    assert fresh_db["common_name_latin_name"].schema == (
        "CREATE TABLE [common_name_latin_name] (\n"
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT,\n"
        "   [latin_name] TEXT\n"
        ")"
    )
    assert list(fresh_db["common_name_latin_name"].rows) == [
        {"name": "Palm", "id": 1, "latin_name": "Arecaceae"},
        {"name": "Spruce", "id": 2, "latin_name": "Picea"},
        {"name": "Mangrove", "id": 3, "latin_name": "Rhizophora"},
        {"name": "Oak", "id": 4, "latin_name": "Quercus"},
    ]
    assert list(itertools.islice(fresh_db["tree"].rows, 0, 4)) == [
        {"id": 1, "name": "Tree 1", "common_name_latin_name_id": 1},
        {"id": 2, "name": "Tree 2", "common_name_latin_name_id": 2},
        {"id": 3, "name": "Tree 3", "common_name_latin_name_id": 3},
        {"id": 4, "name": "Tree 4", "common_name_latin_name_id": 4},
    ]


def test_extract_invalid_columns(fresh_db):
    fresh_db["tree"].insert(
        {
            "id": 1,
            "name": "Tree 1",
            "common_name": "Palm",
            "latin_name": "Arecaceae",
        },
        pk="id",
    )
    with pytest.raises(InvalidColumns):
        fresh_db["tree"].extract(["bad_column"])


def test_extract_rowid_table(fresh_db):
    fresh_db["tree"].insert(
        {
            "name": "Tree 1",
            "common_name": "Palm",
            "latin_name": "Arecaceae",
        }
    )
    fresh_db["tree"].extract(["common_name", "latin_name"])
    assert fresh_db["tree"].schema == (
        'CREATE TABLE "tree" (\n'
        "   [name] TEXT,\n"
        "   [common_name_latin_name_id] INTEGER,\n"
        "   FOREIGN KEY([common_name_latin_name_id]) REFERENCES [common_name_latin_name]([id])\n"
        ")"
    )
    assert (
        fresh_db.execute(
            """
        select
            tree.name,
            common_name_latin_name.common_name,
            common_name_latin_name.latin_name
        from tree
            join common_name_latin_name
            on tree.common_name_latin_name_id = common_name_latin_name.id
    """
        ).fetchall()
        == [("Tree 1", "Palm", "Arecaceae")]
    )


def test_reuse_lookup_table(fresh_db):
    fresh_db["species"].insert({"id": 1, "name": "Wolf"}, pk="id")
    fresh_db["sightings"].insert({"id": 10, "species": "Wolf"}, pk="id")
    fresh_db["individuals"].insert(
        {"id": 10, "name": "Terriana", "species": "Fox"}, pk="id"
    )
    fresh_db["sightings"].extract("species", rename={"species": "name"})
    fresh_db["individuals"].extract("species", rename={"species": "name"})
    assert fresh_db["sightings"].schema == (
        'CREATE TABLE "sightings" (\n'
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [species_id] INTEGER,\n"
        "   FOREIGN KEY([species_id]) REFERENCES [species]([id])\n"
        ")"
    )
    assert fresh_db["individuals"].schema == (
        'CREATE TABLE "individuals" (\n'
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT,\n"
        "   [species_id] INTEGER,\n"
        "   FOREIGN KEY([species_id]) REFERENCES [species]([id])\n"
        ")"
    )
    assert list(fresh_db["species"].rows) == [
        {"id": 1, "name": "Wolf"},
        {"id": 2, "name": "Fox"},
    ]


def test_extract_error_on_incompatible_existing_lookup_table(fresh_db):
    fresh_db["species"].insert({"id": 1})
    fresh_db["tree"].insert({"name": "Tree 1", "common_name": "Palm"})
    with pytest.raises(InvalidColumns):
        fresh_db["tree"].extract("common_name", table="species")

    # Try again with incompatible existing column type
    fresh_db["species2"].insert({"id": 1, "common_name": 3.5})
    with pytest.raises(InvalidColumns):
        fresh_db["tree"].extract("common_name", table="species2")


def test_extract_works_with_null_values(fresh_db):
    fresh_db["listens"].insert_all(
        [
            {"id": 1, "track_title": "foo", "album_title": "bar"},
            {"id": 2, "track_title": "baz", "album_title": None},
        ],
        pk="id",
    )
    fresh_db["listens"].extract(
        columns=["album_title"], table="albums", fk_column="album_id"
    )
    assert list(fresh_db["listens"].rows) == [
        {"id": 1, "track_title": "foo", "album_id": 1},
        {"id": 2, "track_title": "baz", "album_id": 2},
    ]
    assert list(fresh_db["albums"].rows) == [
        {"id": 1, "album_title": "bar"},
        {"id": 2, "album_title": None},
    ]
