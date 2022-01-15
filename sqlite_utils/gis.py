import os
from .db import Database, Table

SPATIALITE_PATHS = (
    "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
    "/usr/local/lib/mod_spatialite.dylib",
)


def find_spatialite() -> str:
    for path in SPATIALITE_PATHS:
        if os.path.exists(path):
            return path
    return None


def init_spatialite(db: Database, path: str) -> None:
    "Load spatialite extension for a database"
    db.conn.enable_load_extension(True)
    db.conn.load_extension(path)
    # Initialize SpatiaLite if not yet initialized
    if "spatial_ref_sys" in db.table_names():
        return
    db.execute("select InitSpatialMetadata(1)")


def add_geometry_column(
    table: Table,
    geometry_type: str,
    column_name: str = "geometry",
    srid: int = 4326,
    coord_dimension: str = "XY",
    not_null: bool = False,
) -> None:
    "Add a geometry column to a table"
    table.db.execute(
        "SELECT AddGeometryColumn(?, ?, ?, ?, ?, ?);",
        [table.name, column_name, srid, geometry_type, coord_dimension, int(not_null)],
    )


def create_spatial_index(table: Table, column_name: str = "geometry") -> None:
    "Create a spatial index for a table and column"
    table.db.execute("select CreateSpatialIndex(?, ?)", [table.name, column_name])
