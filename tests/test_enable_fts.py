search_records = [
    {"text": "tanuki are tricksters", "country": "Japan", "not_searchable": "foo"},
    {"text": "racoons are trash pandas", "country": "USA", "not_searchable": "bar"},
]


def test_enable_fts(fresh_db):
    table = fresh_db["searchable"]
    table.insert_all(search_records)
    assert ["searchable"] == fresh_db.table_names()
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert [
        "searchable",
        "searchable_fts",
        "searchable_fts_segments",
        "searchable_fts_segdir",
        "searchable_fts_docsize",
        "searchable_fts_stat",
    ] == fresh_db.table_names()
    assert [("tanuki are tricksters", "Japan", "foo")] == table.search("tanuki")
    assert [("racoons are trash pandas", "USA", "bar")] == table.search("usa")
    assert [] == table.search("bar")


def test_populate_fts(fresh_db):
    table = fresh_db["populatable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert [] == table.search("trash pandas")
    table.insert(search_records[1])
    assert [] == table.search("trash pandas")
    # Now run populate_fts to make this record available
    table.populate_fts(["text", "country"])
    assert [("racoons are trash pandas", "USA", "bar")] == table.search("usa")


def test_optimize_fts(fresh_db):
    for fts_version in ("4", "5"):
        table_name = "searchable_{}".format(fts_version)
        table = fresh_db[table_name]
        table.insert_all(search_records)
        table.enable_fts(["text", "country"], fts_version="FTS{}".format(fts_version))
    # You can call optimize successfully against the tables OR their _fts equivalents:
    for table_name in (
        "searchable_4",
        "searchable_5",
        "searchable_4_fts",
        "searchable_5_fts",
    ):
        fresh_db[table_name].optimize()
