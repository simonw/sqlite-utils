from .fixtures import fresh_db
import pytest

search_records = [
    {"text": "tanuki are tricksters", "country": "Japan", "not_searchable": "foo"},
    {"text": "racoons are trash pandas", "country": "USA", "not_searchable": "bar"},
]


def test_enable_fts(fresh_db):
    table = fresh_db["searchable"]
    table.insert_all(search_records)
    assert ["searchable"] == fresh_db.tables
    table.enable_fts(["text", "country"])
    assert [
        "searchable",
        "searchable_fts",
        "searchable_fts_segments",
        "searchable_fts_segdir",
        "searchable_fts_docsize",
        "searchable_fts_stat",
    ] == fresh_db.tables
    assert [("tanuki are tricksters", "Japan", "foo")] == table.search("tanuki")
    assert [("racoons are trash pandas", "USA", "bar")] == table.search("usa")
    assert [] == table.search("bar")


def test_populate_fts(fresh_db):
    table = fresh_db["populatable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"])
    assert [] == table.search("trash pandas")
    table.insert(search_records[1])
    assert [] == table.search("trash pandas")
    # Now run populate_fts to make this record available
    table.populate_fts(["text", "country"])
    assert [("racoons are trash pandas", "USA", "bar")] == table.search("usa")
