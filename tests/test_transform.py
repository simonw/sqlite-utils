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
    params["tmp_suffix"] = "suffix"
    sql = dogs.transform_sql(**params)
    assert sql == expected_sql
