import pytest
from sqlite_utils.utils import find_spatialite
from sqlite_utils.db import Database
from sqlite_utils.utils import sqlite3

pytestmark = [
    pytest.mark.skipif(
        not find_spatialite(), reason="Could not find SpatiaLite extension"
    ),
    pytest.mark.skipif(
        not hasattr(sqlite3.Connection, "enable_load_extension"),
        reason="sqlite3.Connection missing enable_load_extension",
    ),
]


def test_find_spatialite():
    spatialite = find_spatialite()
    assert spatialite is None or isinstance(spatialite, str)


def test_init_spatialite():
    db = Database(memory=True)
    spatialite = find_spatialite()
    db.init_spatialite(spatialite)
    assert "spatial_ref_sys" in db.table_names()


def test_add_geometry_column():
    db = Database(memory=True)
    spatialite = find_spatialite()
    db.init_spatialite(spatialite)

    # create a table first
    table = db.create_table("locations", {"id": str, "properties": str})
    table.add_geometry_column(
        column_name="geometry",
        geometry_type="Point",
        srid=4326,
        coord_dimension=2,
    )

    assert db["geometry_columns"].get(["locations", "geometry"]) == {
        "f_table_name": "locations",
        "f_geometry_column": "geometry",
        "geometry_type": 1,  # point
        "coord_dimension": 2,
        "srid": 4326,
        "spatial_index_enabled": 0,
    }


def test_create_spatial_index():
    db = Database(memory=True)
    spatialite = find_spatialite()
    assert db.init_spatialite(spatialite)

    # create a table, add a geometry column with default values
    table = db.create_table("locations", {"id": str, "properties": str})
    assert table.add_geometry_column("geometry", "Point")

    # index it
    assert table.create_spatial_index("geometry")

    assert "idx_locations_geometry" in db.table_names()


def test_double_create_spatial_index():
    db = Database(memory=True)
    spatialite = find_spatialite()
    db.init_spatialite(spatialite)

    # create a table, add a geometry column with default values
    table = db.create_table("locations", {"id": str, "properties": str})
    table.add_geometry_column("geometry", "Point")

    # index it, return True
    assert table.create_spatial_index("geometry")

    assert "idx_locations_geometry" in db.table_names()

    # call it again, return False
    assert not table.create_spatial_index("geometry")
