from sqlite_utils.db import ForeignKey
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
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change column type
        (
            {"types": {"age": int}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Rename a column
        (
            {"rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [dog_age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Drop a column
        (
            {"drop": ["age"]},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name])\n   SELECT [id], [name] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Convert type AND rename column
        (
            {"types": {"age": int}, "rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [dog_age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change primary key
        (
            {"pk": "age"},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT PRIMARY KEY\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Change primary key to a compound pk
        (
            {"pk": ("age", "name")},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT,\n   PRIMARY KEY ([age], [name])\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
        # Remove primary key, creating a rowid table
        (
            {"pk": None},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age])\n   SELECT [id], [name], [age] FROM [dogs];",
                "DROP TABLE [dogs];",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs];",
            ],
        ),
    ],
)
@pytest.mark.parametrize("use_pragma_foreign_keys", [False, True])
def test_transform_sql(fresh_db, params, expected_sql, use_pragma_foreign_keys):
    captured = []
    tracer = lambda sql, params: captured.append((sql, params))
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


def test_transform_sql_rowid_to_id(fresh_db):
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


@pytest.mark.parametrize("use_pragma_foreign_keys", [False, True])
def test_transform_drop_foreign_keys(fresh_db, use_pragma_foreign_keys):
    if use_pragma_foreign_keys:
        fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    # Create table with three foreign keys so we can drop two of them
    fresh_db["country"].insert({"id": 1, "name": "France"}, pk="id")
    fresh_db["continent"].insert({"id": 2, "name": "Europe"}, pk="id")
    fresh_db["city"].insert({"id": 24, "name": "Paris"}, pk="id")
    fresh_db["places"].insert(
        {
            "id": 32,
            "name": "Caveau de la Huchette",
            "country": 1,
            "continent": 2,
            "city": 24,
        },
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
