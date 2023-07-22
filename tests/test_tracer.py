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
        ("select name from sqlite_master where type = 'view'", None),
        ("select name from sqlite_master where type = 'table'", None),
        ("select name from sqlite_master where type = 'view'", None),
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
    ]


def test_with_tracer():
    collected = []

    def tracer(sql, params):
        return collected.append((sql, params))

    db = Database(memory=True)

    db["dogs"].insert({"name": "Cleopaws"})
    db["dogs"].enable_fts(["name"])

    assert len(collected) == 0

    with db.tracer(tracer):
        list(db["dogs"].search("Cleopaws"))

    assert len(collected) == 5
    assert collected == [
        ("select name from sqlite_master where type = 'view'", None),
        (
            (
                "SELECT name FROM sqlite_master\n"
                "    WHERE rootpage = 0\n"
                "    AND (\n"
                "        sql LIKE :like\n"
                "        OR sql LIKE :like2\n"
                "        OR (\n"
                "            tbl_name = :table\n"
                "            AND sql LIKE '%VIRTUAL TABLE%USING FTS%'\n"
                "        )\n"
                "    )",
                {
                    "like": "%VIRTUAL TABLE%USING FTS%content=[dogs]%",
                    "like2": '%VIRTUAL TABLE%USING FTS%content="dogs"%',
                    "table": "dogs",
                },
            )
        ),
        ("select name from sqlite_master where type = 'view'", None),
        ("select sql from sqlite_master where name = ?", ("dogs_fts",)),
        (
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [dogs]\n"
                ")\n"
                "select\n"
                "    [original].*\n"
                "from\n"
                "    [original]\n"
                "    join [dogs_fts] on [original].rowid = [dogs_fts].rowid\n"
                "where\n"
                "    [dogs_fts] match :query\n"
                "order by\n"
                "    [dogs_fts].rank"
            ),
            {"query": "Cleopaws"},
        ),
    ]

    # Outside the with block collected should not be appended to
    db["dogs"].insert({"name": "Cleopaws"})
    assert len(collected) == 5
