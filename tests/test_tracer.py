import pytest
from sqlite_utils import Database


def test_tracer():
    collected = []
    db = Database(
        memory=True, tracer=lambda sql, params: collected.append((sql, params))
    )
    db["dogs"].insert({"name": "Cleopaws"})
    db["dogs"].enable_fts(["name"])
    db["dogs"].search("Cleopaws")
    assert collected == [
        ("PRAGMA recursive_triggers=on;", None),
        ("select name from sqlite_master where type = 'view'", None),
        ("select name from sqlite_master where type = 'table'", None),
        ("CREATE TABLE [dogs] (\n   [name] TEXT\n);\n        ", None),
        ("select name from sqlite_master where type = 'view'", None),
        ("INSERT INTO [dogs] ([name]) VALUES (?);", ["Cleopaws"]),
        ("select name from sqlite_master where type = 'view'", None),
        (
            "CREATE VIRTUAL TABLE [dogs_fts] USING FTS5 (\n    [name],\n    content=[dogs]\n)",
            None,
        ),
        (
            "INSERT INTO [dogs_fts] (rowid, [name])\n    SELECT rowid, [name] FROM [dogs];",
            None,
        ),
        ("select name from sqlite_master where type = 'view'", None),
        (
            'select * from "dogs" where rowid in (\n    select rowid from [dogs_fts]\n    where [dogs_fts] match :search\n)\norder by rowid',
            ("Cleopaws",),
        ),
    ]


def test_with_tracer():
    collected = []
    tracer = lambda sql, params: collected.append((sql, params))

    db = Database(memory=True)

    db["dogs"].insert({"name": "Cleopaws"})
    db["dogs"].enable_fts(["name"])

    assert len(collected) == 0

    with db.tracer(tracer):
        db["dogs"].search("Cleopaws")

    assert len(collected) == 2
    assert collected == [
        ("select name from sqlite_master where type = 'view'", None),
        (
            'select * from "dogs" where rowid in (\n    select rowid from [dogs_fts]\n    where [dogs_fts] match :search\n)\norder by rowid',
            ("Cleopaws",),
        ),
    ]

    # Outside the with block collected should not be appended to
    db["dogs"].insert({"name": "Cleopaws"})
    assert len(collected) == 2
