from sqlite_utils.db import ForeignKey, TransformError
from sqlite_utils.utils import OperationalError
import pytest


@pytest.mark.parametrize(
    "params,expected_sql",
    [
        # Identity transform - nothing changes
        (
            {},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change column type
        (
            {"types": {"age": int}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Rename a column
        (
            {"rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [dog_age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Drop a column
        (
            {"drop": ["age"]},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name])\n   SELECT [rowid], [id], [name] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Convert type AND rename column
        (
            {"types": {"age": int}, "rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [dog_age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change primary key
        (
            {"pk": "age"},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT PRIMARY KEY\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change primary key to a compound pk
        (
            {"pk": ("age", "name")},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT,\n   PRIMARY KEY ([age], [name])\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Remove primary key, creating a rowid table
        (
            {"pk": None},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Keeping the table
        (
            {"drop": ["age"], "keep_table": "kept_table"},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name])\n   SELECT [rowid], [id], [name] FROM [dogs];",
                "ALTER TABLE [dogs] RENAME TO [kept_table];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
    ],
)
@pytest.mark.parametrize("use_pragma_foreign_keys", [False, True])
def test_transform_sql_table_with_primary_key(
    fresh_db, params, expected_sql, use_pragma_foreign_keys
):
    captured = []

    def tracer(sql, params):
        return captured.append((sql, params))

    dogs = fresh_db["dogs"]
    if use_pragma_foreign_keys:
        fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")
    sql = dogs.transform_sql(**{**params, **{"tmp_suffix": "suffix"}})
    assert sql == expected_sql
    # Check that .transform() runs without exceptions:
    with fresh_db.tracer(tracer):
        dogs.transform(**params)
    # If use_pragma_foreign_keys, check that we did the right thing
    if use_pragma_foreign_keys:
        assert ("PRAGMA foreign_keys=0;", None) in captured
        assert captured[-2] == ("PRAGMA foreign_key_check;", None)
        assert captured[-1] == ("PRAGMA foreign_keys=1;", None)
    else:
        assert ("PRAGMA foreign_keys=0;", None) not in captured
        assert ("PRAGMA foreign_keys=1;", None) not in captured


@pytest.mark.parametrize(
    "params,expected_sql",
    [
        # Identity transform - nothing changes
        (
            {},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change column type
        (
            {"types": {"age": int}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Rename a column
        (
            {"rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [dog_age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [dog_age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Make ID a primary key
        (
            {"pk": "id"},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([rowid], [id], [name], [age])\n   SELECT [rowid], [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
    ],
)
@pytest.mark.parametrize("use_pragma_foreign_keys", [False, True])
def test_transform_sql_table_with_no_primary_key(
    fresh_db, params, expected_sql, use_pragma_foreign_keys
):
    captured = []

    def tracer(sql, params):
        return captured.append((sql, params))

    dogs = fresh_db["dogs"]
    if use_pragma_foreign_keys:
        fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"})
    sql = dogs.transform_sql(**{**params, **{"tmp_suffix": "suffix"}})
    assert sql == expected_sql
    # Check that .transform() runs without exceptions:
    with fresh_db.tracer(tracer):
        dogs.transform(**params)
    # If use_pragma_foreign_keys, check that we did the right thing
    if use_pragma_foreign_keys:
        assert ("PRAGMA foreign_keys=0;", None) in captured
        assert captured[-2] == ("PRAGMA foreign_key_check;", None)
        assert captured[-1] == ("PRAGMA foreign_keys=1;", None)
    else:
        assert ("PRAGMA foreign_keys=0;", None) not in captured
        assert ("PRAGMA foreign_keys=1;", None) not in captured


def test_transform_sql_with_no_primary_key_to_primary_key_of_id(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"})
    assert (
        dogs.schema
        == "CREATE TABLE [dogs] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT\n)"
    )
    dogs.transform(pk="id")
    # Slight oddity: [dogs] becomes "dogs" during the rename:
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] TEXT\n)'
    )


def test_transform_rename_pk(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")
    dogs.transform(rename={"id": "pk"})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [pk] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] TEXT\n)'
    )


def test_transform_not_null(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")
    dogs.transform(not_null={"name"})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT NOT NULL,\n   [age] TEXT\n)'
    )


def test_transform_remove_a_not_null(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, not_null={"age"}, pk="id")
    dogs.transform(not_null={"name": True, "age": False})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT NOT NULL,\n   [age] TEXT\n)'
    )


@pytest.mark.parametrize("not_null", [{"age"}, {"age": True}])
def test_transform_add_not_null_with_rename(fresh_db, not_null):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")
    dogs.transform(not_null=not_null, rename={"age": "dog_age"})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] TEXT NOT NULL\n)'
    )


def test_transform_defaults(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": 5}, pk="id")
    dogs.transform(defaults={"age": 1})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] INTEGER DEFAULT 1\n)'
    )


def test_transform_defaults_and_rename_column(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": 5}, pk="id")
    dogs.transform(rename={"age": "dog_age"}, defaults={"age": 1})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] INTEGER DEFAULT 1\n)'
    )


def test_remove_defaults(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": 5}, defaults={"age": 1}, pk="id")
    dogs.transform(defaults={"age": None})
    assert (
        dogs.schema
        == 'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] INTEGER\n)'
    )


@pytest.fixture
def authors_db(fresh_db):
    books = fresh_db["books"]
    authors = fresh_db["authors"]
    authors.insert({"id": 5, "name": "Jane McGonical"}, pk="id")
    books.insert(
        {"id": 2, "title": "Reality is Broken", "author_id": 5},
        foreign_keys=("author_id",),
        pk="id",
    )
    return fresh_db


def test_transform_foreign_keys_persist(authors_db):
    assert authors_db["books"].foreign_keys == [
        ForeignKey(
            table="books", column="author_id", other_table="authors", other_column="id"
        )
    ]
    authors_db["books"].transform(rename={"title": "book_title"})
    assert authors_db["books"].foreign_keys == [
        ForeignKey(
            table="books", column="author_id", other_table="authors", other_column="id"
        )
    ]


@pytest.mark.parametrize("use_pragma_foreign_keys", [False, True])
def test_transform_foreign_keys_survive_renamed_column(
    authors_db, use_pragma_foreign_keys
):
    if use_pragma_foreign_keys:
        authors_db.conn.execute("PRAGMA foreign_keys=ON")
    authors_db["books"].transform(rename={"author_id": "author_id_2"})
    assert authors_db["books"].foreign_keys == [
        ForeignKey(
            table="books",
            column="author_id_2",
            other_table="authors",
            other_column="id",
        )
    ]


def _add_country_city_continent(db):
    db["country"].insert({"id": 1, "name": "France"}, pk="id")
    db["continent"].insert({"id": 2, "name": "Europe"}, pk="id")
    db["city"].insert({"id": 24, "name": "Paris"}, pk="id")


_CAVEAU = {
    "id": 32,
    "name": "Caveau de la Huchette",
    "country": 1,
    "continent": 2,
    "city": 24,
}


@pytest.mark.parametrize("use_pragma_foreign_keys", [False, True])
def test_transform_drop_foreign_keys(fresh_db, use_pragma_foreign_keys):
    if use_pragma_foreign_keys:
        fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    # Create table with three foreign keys so we can drop two of them
    _add_country_city_continent(fresh_db)
    fresh_db["places"].insert(
        _CAVEAU,
        foreign_keys=("country", "continent", "city"),
    )
    assert fresh_db["places"].foreign_keys == [
        ForeignKey(
            table="places", column="city", other_table="city", other_column="id"
        ),
        ForeignKey(
            table="places",
            column="continent",
            other_table="continent",
            other_column="id",
        ),
        ForeignKey(
            table="places", column="country", other_table="country", other_column="id"
        ),
    ]
    # Drop two of those foreign keys
    fresh_db["places"].transform(drop_foreign_keys=("country", "continent"))
    # Should be only one foreign key now
    assert fresh_db["places"].foreign_keys == [
        ForeignKey(table="places", column="city", other_table="city", other_column="id")
    ]
    if use_pragma_foreign_keys:
        assert fresh_db.conn.execute("PRAGMA foreign_keys").fetchone()[0]


def test_transform_verify_foreign_keys(fresh_db):
    fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    fresh_db["authors"].insert({"id": 3, "name": "Tina"}, pk="id")
    fresh_db["books"].insert(
        {"id": 1, "title": "Book", "author_id": 3}, pk="id", foreign_keys={"author_id"}
    )
    # Renaming the id column on authors should break everything
    with pytest.raises(OperationalError) as e:
        fresh_db["authors"].transform(rename={"id": "id2"})
    assert e.value.args[0] == 'foreign key mismatch - "books" referencing "authors"'
    # This should have rolled us back
    assert (
        fresh_db["authors"].schema
        == "CREATE TABLE [authors] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n)"
    )
    assert fresh_db.conn.execute("PRAGMA foreign_keys").fetchone()[0]


def test_transform_add_foreign_keys_from_scratch(fresh_db):
    _add_country_city_continent(fresh_db)
    fresh_db["places"].insert(_CAVEAU)
    # Should have no foreign keys
    assert fresh_db["places"].foreign_keys == []
    # Now add them using .transform()
    fresh_db["places"].transform(add_foreign_keys=("country", "continent", "city"))
    # Should now have all three:
    assert fresh_db["places"].foreign_keys == [
        ForeignKey(
            table="places", column="city", other_table="city", other_column="id"
        ),
        ForeignKey(
            table="places",
            column="continent",
            other_table="continent",
            other_column="id",
        ),
        ForeignKey(
            table="places", column="country", other_table="country", other_column="id"
        ),
    ]
    assert fresh_db["places"].schema == (
        'CREATE TABLE "places" (\n'
        "   [id] INTEGER,\n"
        "   [name] TEXT,\n"
        "   [country] INTEGER REFERENCES [country]([id]),\n"
        "   [continent] INTEGER REFERENCES [continent]([id]),\n"
        "   [city] INTEGER REFERENCES [city]([id])\n"
        ")"
    )


@pytest.mark.parametrize(
    "add_foreign_keys",
    (
        ("country", "continent"),
        # Fully specified
        (
            ("country", "country", "id"),
            ("continent", "continent", "id"),
        ),
    ),
)
def test_transform_add_foreign_keys_from_partial(fresh_db, add_foreign_keys):
    _add_country_city_continent(fresh_db)
    fresh_db["places"].insert(
        _CAVEAU,
        foreign_keys=("city",),
    )
    # Should have one foreign keys
    assert fresh_db["places"].foreign_keys == [
        ForeignKey(table="places", column="city", other_table="city", other_column="id")
    ]
    # Now add three more using .transform()
    fresh_db["places"].transform(add_foreign_keys=add_foreign_keys)
    # Should now have all three:
    assert fresh_db["places"].foreign_keys == [
        ForeignKey(
            table="places", column="city", other_table="city", other_column="id"
        ),
        ForeignKey(
            table="places",
            column="continent",
            other_table="continent",
            other_column="id",
        ),
        ForeignKey(
            table="places", column="country", other_table="country", other_column="id"
        ),
    ]


@pytest.mark.parametrize(
    "foreign_keys",
    (
        ("country", "continent"),
        # Fully specified
        (
            ("country", "country", "id"),
            ("continent", "continent", "id"),
        ),
    ),
)
def test_transform_replace_foreign_keys(fresh_db, foreign_keys):
    _add_country_city_continent(fresh_db)
    fresh_db["places"].insert(
        _CAVEAU,
        foreign_keys=("city",),
    )
    assert len(fresh_db["places"].foreign_keys) == 1
    # Replace with two different ones
    fresh_db["places"].transform(foreign_keys=foreign_keys)
    assert fresh_db["places"].schema == (
        'CREATE TABLE "places" (\n'
        "   [id] INTEGER,\n"
        "   [name] TEXT,\n"
        "   [country] INTEGER REFERENCES [country]([id]),\n"
        "   [continent] INTEGER REFERENCES [continent]([id]),\n"
        "   [city] INTEGER\n"
        ")"
    )


@pytest.mark.parametrize("table_type", ("id_pk", "rowid", "compound_pk"))
def test_transform_preserves_rowids(fresh_db, table_type):
    pk = None
    if table_type == "id_pk":
        pk = "id"
    elif table_type == "compound_pk":
        pk = ("id", "name")
    elif table_type == "rowid":
        pk = None
    fresh_db["places"].insert_all(
        [
            {"id": "1", "name": "Paris", "country": "France"},
            {"id": "2", "name": "London", "country": "UK"},
            {"id": "3", "name": "New York", "country": "USA"},
        ],
        pk=pk,
    )
    # Now delete and insert a row to mix up the `rowid` sequence
    fresh_db["places"].delete_where("id = ?", ["2"])
    fresh_db["places"].insert({"id": "4", "name": "London", "country": "UK"})
    previous_rows = list(
        tuple(row) for row in fresh_db.execute("select rowid, id, name from places")
    )
    # Transform it
    fresh_db["places"].transform(column_order=("country", "name"))
    # Should be the same
    next_rows = list(
        tuple(row) for row in fresh_db.execute("select rowid, id, name from places")
    )
    assert previous_rows == next_rows


@pytest.mark.parametrize("strict", (False, True))
def test_transform_strict(fresh_db, strict):
    dogs = fresh_db.table("dogs", strict=strict)
    dogs.insert({"id": 1, "name": "Cleo"})
    assert dogs.strict == strict or not fresh_db.supports_strict
    dogs.transform(not_null={"name"})
    assert dogs.strict == strict or not fresh_db.supports_strict


@pytest.mark.parametrize(
    "indexes, transform_params",
    [
        ([["name"]], {"types": {"age": str}}),
        ([["name"], ["age", "breed"]], {"types": {"age": str}}),
        ([], {"types": {"age": str}}),
        ([["name"]], {"types": {"age": str}, "keep_table": "old_dogs"}),
    ],
)
def test_transform_indexes(fresh_db, indexes, transform_params):
    # https://github.com/simonw/sqlite-utils/issues/633
    # New table should have same indexes as old table after transformation
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": 5, "breed": "Labrador"}, pk="id")

    for index in indexes:
        dogs.create_index(index)

    indexes_before_transform = dogs.indexes

    dogs.transform(**transform_params)

    assert sorted(
        [
            {k: v for k, v in idx._asdict().items() if k != "seq"}
            for idx in dogs.indexes
        ],
        key=lambda x: x["name"],
    ) == sorted(
        [
            {k: v for k, v in idx._asdict().items() if k != "seq"}
            for idx in indexes_before_transform
        ],
        key=lambda x: x["name"],
    ), f"Indexes before transform: {indexes_before_transform}\nIndexes after transform: {dogs.indexes}"
    if "keep_table" in transform_params:
        assert all(
            index.origin == "pk"
            for index in fresh_db[transform_params["keep_table"]].indexes
        )


def test_transform_retains_indexes_with_foreign_keys(fresh_db):
    dogs = fresh_db["dogs"]
    owners = fresh_db["owners"]

    dogs.insert({"id": 1, "name": "Cleo", "owner_id": 1}, pk="id")
    owners.insert({"id": 1, "name": "Alice"}, pk="id")

    dogs.create_index(["name"])

    indexes_before_transform = dogs.indexes

    fresh_db.add_foreign_keys([("dogs", "owner_id", "owners", "id")])  # calls transform

    assert sorted(
        [
            {k: v for k, v in idx._asdict().items() if k != "seq"}
            for idx in dogs.indexes
        ],
        key=lambda x: x["name"],
    ) == sorted(
        [
            {k: v for k, v in idx._asdict().items() if k != "seq"}
            for idx in indexes_before_transform
        ],
        key=lambda x: x["name"],
    ), f"Indexes before transform: {indexes_before_transform}\nIndexes after transform: {dogs.indexes}"


@pytest.mark.parametrize(
    "transform_params",
    [
        {"rename": {"age": "dog_age"}},
        {"drop": ["age"]},
    ],
)
def test_transform_with_indexes_errors(fresh_db, transform_params):
    # Should error with a compound (name, age) index if age is renamed or dropped
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": 5}, pk="id")

    dogs.create_index(["name", "age"])

    with pytest.raises(TransformError) as excinfo:
        dogs.transform(**transform_params)

    assert (
        "Index 'idx_dogs_name_age' column 'age' is not in updated table 'dogs'. "
        "You must manually drop this index prior to running this transformation"
        in str(excinfo.value)
    )


def test_transform_with_unique_constraint_implicit_index(fresh_db):
    dogs = fresh_db["dogs"]
    # Create a table with a UNIQUE constraint on 'name', which creates an implicit index
    fresh_db.execute(
        """
        CREATE TABLE dogs (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            age INTEGER
        );
    """
    )
    dogs.insert({"id": 1, "name": "Cleo", "age": 5})

    # Attempt to transform the table without modifying 'name'
    with pytest.raises(TransformError) as excinfo:
        dogs.transform(types={"age": str})

    assert (
        "Index 'sqlite_autoindex_dogs_1' on table 'dogs' does not have a CREATE INDEX statement."
        in str(excinfo.value)
    )
    assert (
        "You must manually drop this index prior to running this transformation and manually recreate the new index after running this transformation."
        in str(excinfo.value)
    )
