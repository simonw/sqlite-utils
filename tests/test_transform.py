import pytest


@pytest.mark.parametrize(
    "params,expected_sql",
    [
        # Identity transform - nothing changes
        (
            {},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age]) SELECT [id], [name], [age] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        # Change column type
        (
            {"columns": {"age": int}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age]) SELECT [id], [name], [age] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        # Rename a column
        (
            {"rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [dog_age]) SELECT [id], [name], [age] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        # Drop a column
        (
            {"drop": ["age"]},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name]) SELECT [id], [name] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        # Convert type AND rename column
        (
            {"columns": {"age": int}, "rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [dog_age] INTEGER\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [dog_age]) SELECT [id], [name], [age] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        # Change primary key
        (
            {"pk": "age"},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT PRIMARY KEY\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age]) SELECT [id], [name], [age] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        # Change primary key to a compound pk
        (
            {"pk": ("age", "name")},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT,\n   PRIMARY KEY ([age], [name])\n);",
                "INSERT INTO [dogs_new_suffix] ([id], [name], [age]) SELECT [id], [name], [age] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
    ],
)
def test_transform_sql(fresh_db, params, expected_sql):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")
    sql = dogs.transform_sql(**{**params, **{"tmp_suffix": "suffix"}})
    assert sql == expected_sql
    # Check that .transform() runs without exceptions:
    dogs.transform(**params)


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
