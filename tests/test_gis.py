import json
import pytest

from click.testing import CliRunner
from sqlite_utils.cli import cli
from sqlite_utils.db import Database
from sqlite_utils.utils import find_spatialite, sqlite3

try:
    import sqlean
except ImportError:
    sqlean = None


pytestmark = [
    pytest.mark.skipif(
        not find_spatialite(), reason="Could not find SpatiaLite extension"
    ),
    pytest.mark.skipif(
        not hasattr(sqlite3.Connection, "enable_load_extension"),
        reason="sqlite3.Connection missing enable_load_extension",
    ),
    pytest.mark.skipif(
        sqlean is not None, reason="sqlean.py is not compatible with SpatiaLite"
    ),
]


# python API tests
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


# cli tests
@pytest.mark.parametrize("use_spatialite_shortcut", [True, False])
def test_query_load_extension(use_spatialite_shortcut):
    # Without --load-extension:
    result = CliRunner().invoke(cli, [":memory:", "select spatialite_version()"])
    assert result.exit_code == 1
    assert "no such function: spatialite_version" in result.output
    # With --load-extension:
    if use_spatialite_shortcut:
        load_extension = "spatialite"
    else:
        load_extension = find_spatialite()
    result = CliRunner().invoke(
        cli,
        [
            ":memory:",
            "select spatialite_version()",
            "--load-extension={}".format(load_extension),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert ["spatialite_version()"] == list(json.loads(result.output)[0].keys())


def test_cli_create_spatialite(tmpdir):
    # sqlite-utils create test.db --init-spatialite
    db_path = tmpdir / "created.db"
    result = CliRunner().invoke(
        cli, ["create-database", str(db_path), "--init-spatialite"]
    )

    assert 0 == result.exit_code
    assert db_path.exists()
    assert db_path.read_binary()[:16] == b"SQLite format 3\x00"

    db = Database(str(db_path))
    assert "spatial_ref_sys" in db.table_names()


def test_cli_add_geometry_column(tmpdir):
    # create a rowid table with one column
    db_path = tmpdir / "spatial.db"
    db = Database(str(db_path))
    db.init_spatialite()

    table = db["locations"].create({"name": str})

    result = CliRunner().invoke(
        cli,
        [
            "add-geometry-column",
            str(db_path),
            table.name,
            "geometry",
            "--type",
            "POINT",
        ],
    )

    assert 0 == result.exit_code

    assert db["geometry_columns"].get(["locations", "geometry"]) == {
        "f_table_name": "locations",
        "f_geometry_column": "geometry",
        "geometry_type": 1,  # point
        "coord_dimension": 2,
        "srid": 4326,
        "spatial_index_enabled": 0,
    }


def test_cli_add_geometry_column_options(tmpdir):
    # create a rowid table with one column
    db_path = tmpdir / "spatial.db"
    db = Database(str(db_path))
    db.init_spatialite()
    table = db["locations"].create({"name": str})

    result = CliRunner().invoke(
        cli,
        [
            "add-geometry-column",
            str(db_path),
            table.name,
            "geometry",
            "-t",
            "POLYGON",
            "--srid",
            "3857",  # https://epsg.io/3857
            "--not-null",
        ],
    )

    assert 0 == result.exit_code

    assert db["geometry_columns"].get(["locations", "geometry"]) == {
        "f_table_name": "locations",
        "f_geometry_column": "geometry",
        "geometry_type": 3,  # polygon
        "coord_dimension": 2,
        "srid": 3857,
        "spatial_index_enabled": 0,
    }

    column = table.columns[1]
    assert column.notnull


def test_cli_add_geometry_column_invalid_type(tmpdir):
    # create a rowid table with one column
    db_path = tmpdir / "spatial.db"
    db = Database(str(db_path))
    db.init_spatialite()

    table = db["locations"].create({"name": str})

    result = CliRunner().invoke(
        cli,
        [
            "add-geometry-column",
            str(db_path),
            table.name,
            "geometry",
            "--type",
            "NOT-A-TYPE",
        ],
    )

    assert 2 == result.exit_code


def test_cli_create_spatial_index(tmpdir):
    # create a rowid table with one column
    db_path = tmpdir / "spatial.db"
    db = Database(str(db_path))
    db.init_spatialite()

    table = db["locations"].create({"name": str})
    table.add_geometry_column("geometry", "POINT")

    result = CliRunner().invoke(
        cli, ["create-spatial-index", str(db_path), table.name, "geometry"]
    )

    assert 0 == result.exit_code

    assert "idx_locations_geometry" in db.table_names()
