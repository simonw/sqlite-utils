import pytest
from sqlite_utils import gis
from sqlite_utils.db import Database
from sqlite_utils.utils import sqlite3


@pytest.mark.skipif(
    not gis.find_spatialite(), reason="Could not find SpatiaLite extension"
)
@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "enable_load_extension"),
    reason="sqlite3.Connection missing enable_load_extension",
)
def test_find_spatialite():
    spatialite = gis.find_spatialite()
    assert spatialite is None or isinstance(spatialite, str)


@pytest.mark.skipif(
    not gis.find_spatialite(), reason="Could not find SpatiaLite extension"
)
@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "enable_load_extension"),
    reason="sqlite3.Connection missing enable_load_extension",
)
def test_init_spatialite():
    db = Database(memory=True)
    spatialite = gis.find_spatialite()
    gis.init_spatialite(db, spatialite)
    assert "spatial_ref_sys" in db.table_names()


@pytest.mark.skipif(
    not gis.find_spatialite(), reason="Could not find SpatiaLite extension"
)
@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "enable_load_extension"),
    reason="sqlite3.Connection missing enable_load_extension",
)
def test_add_geometry_column():
    db = Database(memory=True)
    spatialite = gis.find_spatialite()
    gis.init_spatialite(db, spatialite)

    # create a table first
    db.create_table("locations", {"id": str, "properties": str})
    gis.add_geometry_column(
        db["locations"],
        geometry_type="Point",
        column_name="geometry",
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


@pytest.mark.skipif(
    not gis.find_spatialite(), reason="Could not find SpatiaLite extension"
)
@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "enable_load_extension"),
    reason="sqlite3.Connection missing enable_load_extension",
)
def test_create_spatial_index():
    db = Database(memory=True)
    spatialite = gis.find_spatialite()
    gis.init_spatialite(db, spatialite)

    # create a table, add a geometry column with default values
    db.create_table("locations", {"id": str, "properties": str})
    gis.add_geometry_column(db["locations"], "Point", "geometry")

    # index it
    gis.create_spatial_index(db["locations"], "geometry")

    assert "idx_locations_geometry" in db.table_names()
