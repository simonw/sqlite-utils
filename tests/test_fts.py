import pytest

search_records = [
    {
        "text": "tanuki are running tricksters",
        "country": "Japan",
        "not_searchable": "foo",
    },
    {
        "text": "racoons are biting trash pandas",
        "country": "USA",
        "not_searchable": "bar",
    },
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
    assert [("tanuki are running tricksters", "Japan", "foo")] == table.search("tanuki")
    assert [("racoons are biting trash pandas", "USA", "bar")] == table.search("usa")
    assert [] == table.search("bar")


def test_enable_fts_escape_table_names(fresh_db):
    # Table names with restricted chars are handled correctly.
    # colons and dots are restricted characters for table names.
    table = fresh_db["http://example.com"]
    table.insert_all(search_records)
    assert ["http://example.com"] == fresh_db.table_names()
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert [
        "http://example.com",
        "http://example.com_fts",
        "http://example.com_fts_segments",
        "http://example.com_fts_segdir",
        "http://example.com_fts_docsize",
        "http://example.com_fts_stat",
    ] == fresh_db.table_names()
    assert [("tanuki are running tricksters", "Japan", "foo")] == table.search("tanuki")
    assert [("racoons are biting trash pandas", "USA", "bar")] == table.search("usa")
    assert [] == table.search("bar")


def test_enable_fts_table_names_containing_spaces(fresh_db):
    table = fresh_db["test"]
    table.insert({"column with spaces": "in its name"})
    table.enable_fts(["column with spaces"])
    assert [
        "test",
        "test_fts",
        "test_fts_data",
        "test_fts_idx",
        "test_fts_docsize",
        "test_fts_config",
    ] == fresh_db.table_names()


def test_populate_fts(fresh_db):
    table = fresh_db["populatable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert [] == table.search("trash pandas")
    table.insert(search_records[1])
    assert [] == table.search("trash pandas")
    # Now run populate_fts to make this record available
    table.populate_fts(["text", "country"])
    assert [("racoons are biting trash pandas", "USA", "bar")] == table.search("usa")


def test_populate_fts_escape_table_names(fresh_db):
    # Restricted characters such as colon and dots should be escaped.
    table = fresh_db["http://example.com"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert [] == table.search("trash pandas")
    table.insert(search_records[1])
    assert [] == table.search("trash pandas")
    # Now run populate_fts to make this record available
    table.populate_fts(["text", "country"])
    assert [("racoons are biting trash pandas", "USA", "bar")] == table.search("usa")


def test_fts_tokenize(fresh_db):
    for fts_version in ("4", "5"):
        table_name = "searchable_{}".format(fts_version)
        table = fresh_db[table_name]
        table.insert_all(search_records)
        # Test without porter stemming
        table.enable_fts(
            ["text", "country"],
            fts_version="FTS{}".format(fts_version),
        )
        assert [] == table.search("bite")
        # Test WITH stemming
        table.disable_fts()
        table.enable_fts(
            ["text", "country"],
            fts_version="FTS{}".format(fts_version),
            tokenize="porter",
        )
        assert [("racoons are biting trash pandas", "USA", "bar")] == table.search(
            "bite"
        )


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


def test_enable_fts_w_triggers(fresh_db):
    table = fresh_db["searchable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], fts_version="FTS4", create_triggers=True)
    assert [("tanuki are running tricksters", "Japan", "foo")] == table.search("tanuki")
    table.insert(search_records[1])
    # Triggers will auto-populate FTS virtual table, not need to call populate_fts()
    assert [("racoons are biting trash pandas", "USA", "bar")] == table.search("usa")
    assert [] == table.search("bar")


@pytest.mark.parametrize("create_triggers", [True, False])
def test_disable_fts(fresh_db, create_triggers):
    table = fresh_db["searchable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], create_triggers=create_triggers)
    assert {
        "searchable",
        "searchable_fts",
        "searchable_fts_data",
        "searchable_fts_idx",
        "searchable_fts_docsize",
        "searchable_fts_config",
    } == set(fresh_db.table_names())
    if create_triggers:
        expected_triggers = {"searchable_ai", "searchable_ad", "searchable_au"}
    else:
        expected_triggers = set()
    assert expected_triggers == set(
        r[0]
        for r in fresh_db.execute(
            "select name from sqlite_master where type = 'trigger'"
        ).fetchall()
    )
    # Now run .disable_fts() and confirm it worked
    table.disable_fts()
    assert (
        0
        == fresh_db.execute(
            "select count(*) from sqlite_master where type = 'trigger'"
        ).fetchone()[0]
    )
    assert ["searchable"] == fresh_db.table_names()
