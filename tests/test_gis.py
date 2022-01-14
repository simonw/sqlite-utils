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


# extract of In-N-Out locations via alltheplaces.xyz
locations = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "APyEi_bhUMkAPx6J_I1JrqB5eBo=",
            "properties": {
                "ref": "322",
                "@spider": "innout",
                "addr:full": "2835 W. University Dr.",
                "addr:city": "Denton",
                "addr:state": "TX",
                "addr:postcode": "76201",
                "name": "Denton",
                "website": "http://locations.in-n-out.com/322",
            },
            "geometry": {"type": "Point", "coordinates": [-97.17114, 33.2294]},
        },
        {
            "type": "Feature",
            "id": "_fK8gYjGcdlGA09B3a4a8RyUZD4=",
            "properties": {
                "ref": "255",
                "@spider": "innout",
                "addr:full": "190 E. Stacy Rd.",
                "addr:city": "Allen",
                "addr:state": "TX",
                "addr:postcode": "75002",
                "name": "Allen",
                "website": "http://locations.in-n-out.com/255",
            },
            "geometry": {"type": "Point", "coordinates": [-96.65328, 33.12914]},
        },
        {
            "type": "Feature",
            "id": "aO9MWd_7HNlDTGdFH-Vl7vscZdk=",
            "properties": {
                "ref": "256",
                "@spider": "innout",
                "addr:full": "2800 Preston Rd.",
                "addr:city": "Frisco",
                "addr:state": "TX",
                "addr:postcode": "75034",
                "name": "Frisco",
                "website": "http://locations.in-n-out.com/256",
            },
            "geometry": {"type": "Point", "coordinates": [-96.80456, 33.1018]},
        },
        {
            "type": "Feature",
            "id": "yHaQ--D7A6FXdx-vrLKdVxctV5g=",
            "properties": {
                "ref": "299",
                "@spider": "innout",
                "addr:full": "5298 State Highway 121",
                "addr:city": "The Colony",
                "addr:state": "TX",
                "addr:postcode": "75056",
                "name": "The Colony",
                "website": "http://locations.in-n-out.com/299",
            },
            "geometry": {"type": "Point", "coordinates": [-96.87604, 33.07076]},
        },
        {
            "type": "Feature",
            "id": "sGpWMAPrvimcDCBoE9xO7-tkLmw=",
            "properties": {
                "ref": "329",
                "@spider": "innout",
                "addr:full": "3500 Highway 114",
                "addr:city": "Fort Worth",
                "addr:state": "TX",
                "addr:postcode": "76177",
                "name": "Fort Worth",
                "website": "http://locations.in-n-out.com/329",
            },
            "geometry": {"type": "Point", "coordinates": [-97.28667, 33.02593]},
        },
    ],
}
