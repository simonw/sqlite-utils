import os

SPATIALITE_PATHS = (
    "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
    "/usr/local/lib/mod_spatialite.dylib",
)


def find_spatialite() -> str:
    """
    The ``find_spatialite()`` function searches for the `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__ SQLite extension in some common places. It returns a string path to the location, or ``None`` if SpatiaLite was not found.

    You can use it in code like this:

    .. code-block:: python

        from sqlite_utils import Database
        from sqlite_utils.gis import find_spatialite

        db = Database("mydb.db")
        spatialite = find_spatialite()
        if spatialite:
            db.conn.enable_load_extension(True)
            db.conn.load_extension(spatialite)
    """
    for path in SPATIALITE_PATHS:
        if os.path.exists(path):
            return path
    return None


def init_spatialite(db, path: str) -> bool:
    """
    The ``init_spatialite`` function will load and initialize the Spatialite extension.
    The ``path`` argument should be an absolute path to the compiled extension, which
    can be found using ``find_spatialite``.

    Returns true if Spatialite was successfully initialized.

    .. code-block:: python

        from sqlite_utils.gis import find_spatialite, init_spatialite

        db = Database("mydb.db")
        init_spatialite(db, find_spatialite())

    If you've installed Spatialite somewhere unexpected (for testing an alternate version, for example)
    you can pass in an absolute path:

    .. code-block:: python

        from sqlite_utils.gis import init_spatialite

        db = Database("mydb.db")
        init_spatialite(db, "./local/mod_spatialite.dylib")

    """
    db.conn.enable_load_extension(True)
    db.conn.load_extension(path)
    # Initialize SpatiaLite if not yet initialized
    if "spatial_ref_sys" in db.table_names():
        return False
    cursor = db.execute("select InitSpatialMetadata(1)")
    result = cursor.fetchone()
    return result and bool(result[0])


def add_geometry_column(
    table,
    geometry_type: str,
    column_name: str = "geometry",
    srid: int = 4326,
    coord_dimension: str = "XY",
    not_null: bool = False,
) -> bool:
    """
    In Spatialite, a geometry column can only be added to an existing table.
    To do so, use ``add_geometry_column``, passing in a :ref:`table <reference_db_table>`
    and geometry type.

    By default, this will add a nullable column called ``geometry`` using
    `SRID 4326 <https://spatialreference.org/ref/epsg/wgs-84/>`__. These can be customized using
    the ``column_name`` and ``srid`` arguments.

    Returns True if the column was successfully added, False if not.

    .. code-block:: python

        from sqlite_utils.gis import find_spatialite, init_spatialite, add_geometry_column

        db = Database("mydb.db")
        init_spatialite(db, find_spatialite())

        # the table must exist before adding a geometry column
        db["locations"].create({"name": str})
        add_geometry_column(db["locations"], "POINT")

    """
    cursor = table.db.execute(
        "SELECT AddGeometryColumn(?, ?, ?, ?, ?, ?);",
        [table.name, column_name, srid, geometry_type, coord_dimension, int(not_null)],
    )

    result = cursor.fetchone()
    return result and bool(result[0])


def create_spatial_index(table, column_name: str = "geometry") -> bool:
    """
    A spatial index allows for significantly faster bounding box queries.
    To create on, use ``create_spatial_index`` with a :ref:`table <reference_db_table>`
    and the name of an existing geometry column.

    Returns True if the index was successfully created, False if not. Calling this
    function if an index already exists is a no-op.

    .. code-block:: python

        from sqlite_utils.gis import add_geometry_column, create_spatial_index

        # assuming Spatialite is loaded, create the table, add the column
        db["locations"].create({"name": str})
        add_geometry_column(db["locations"], "POINT", "geometry")

        # now we can index it
        create_spatial_index(db["locations"], "geometry")

        # the spatial index is a virtual table, which we can inspect
        print(db["idx_locations_geometry"].schema)
        # outputs:
        # CREATE VIRTUAL TABLE "idx_locations_geometry" USING rtree(pkid, xmin, xmax, ymin, ymax)

    """
    if f"idx_{table.name}_{column_name}" in table.db.table_names():
        return False

    cursor = table.db.execute(
        "select CreateSpatialIndex(?, ?)", [table.name, column_name]
    )
    result = cursor.fetchone()
    return result and bool(result[0])
