import pytest


@pytest.mark.parametrize(
    "params,expected_sql",
    [
        (
            {},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [age] TEXT\n);\n        ",
                "INSERT INTO [dogs_new_suffix] ([age], [id], [name]) SELECT [age], [id], [name] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
        (
            {"rename": {"age": "dog_age"}},
            [
                "CREATE TABLE [dogs_new_suffix] (\n   [id] INTEGER,\n   [name] TEXT,\n   [dog_age] TEXT\n);\n        ",
                "INSERT INTO [dogs_new_suffix] ([id], [dog_age], [name]) SELECT [id], [dog_age], [name] FROM [dogs]",
                "DROP TABLE [dogs]",
                "ALTER TABLE [dogs_new_suffix] RENAME TO [dogs]",
            ],
        ),
    ],
)
def test_transform_table_sql(fresh_db, params, expected_sql):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")
    params["tmp_suffix"] = "suffix"
    sql = dogs.transform_table_sql(**params)
    assert sql == expected_sql
