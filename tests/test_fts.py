import pytest
from sqlite_utils import Database
from sqlite_utils.utils import sqlite3

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
    assert [
        {
            "rowid": 1,
            "text": "tanuki are running tricksters",
            "country": "Japan",
            "not_searchable": "foo",
        }
    ] == list(table.search("tanuki"))
    assert [
        {
            "rowid": 2,
            "text": "racoons are biting trash pandas",
            "country": "USA",
            "not_searchable": "bar",
        }
    ] == list(table.search("usa"))
    assert [] == list(table.search("bar"))


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
    assert [
        {
            "rowid": 1,
            "text": "tanuki are running tricksters",
            "country": "Japan",
            "not_searchable": "foo",
        }
    ] == list(table.search("tanuki"))
    assert [
        {
            "rowid": 2,
            "text": "racoons are biting trash pandas",
            "country": "USA",
            "not_searchable": "bar",
        }
    ] == list(table.search("usa"))
    assert [] == list(table.search("bar"))


def test_search_limit_offset(fresh_db):
    table = fresh_db["t"]
    table.insert_all(search_records)
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert len(list(table.search("are"))) == 2
    assert len(list(table.search("are", limit=1))) == 1
    assert list(table.search("are", limit=1, order_by="rowid"))[0]["rowid"] == 1
    assert (
        list(table.search("are", limit=1, offset=1, order_by="rowid"))[0]["rowid"] == 2
    )


@pytest.mark.parametrize("fts_version", ("FTS4", "FTS5"))
def test_search_where(fresh_db, fts_version):
    table = fresh_db["t"]
    table.insert_all(search_records)
    table.enable_fts(["text", "country"], fts_version=fts_version)
    results = list(
        table.search("are", where="country = :country", where_args={"country": "Japan"})
    )
    assert results == [
        {
            "rowid": 1,
            "text": "tanuki are running tricksters",
            "country": "Japan",
            "not_searchable": "foo",
        }
    ]


def test_search_where_args_disallows_query(fresh_db):
    table = fresh_db["t"]
    with pytest.raises(ValueError) as ex:
        list(
            table.search(
                "x", where="author = :query", where_args={"query": "not allowed"}
            )
        )
    assert (
        ex.value.args[0]
        == "'query' is a reserved key and cannot be passed to where_args for .search()"
    )


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
    assert [] == list(table.search("trash pandas"))
    table.insert(search_records[1])
    assert [] == list(table.search("trash pandas"))
    # Now run populate_fts to make this record available
    table.populate_fts(["text", "country"])
    rows = list(table.search("usa"))
    assert [
        {
            "rowid": 2,
            "text": "racoons are biting trash pandas",
            "country": "USA",
            "not_searchable": "bar",
        }
    ] == rows


def test_populate_fts_escape_table_names(fresh_db):
    # Restricted characters such as colon and dots should be escaped.
    table = fresh_db["http://example.com"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], fts_version="FTS4")
    assert [] == list(table.search("trash pandas"))
    table.insert(search_records[1])
    assert [] == list(table.search("trash pandas"))
    # Now run populate_fts to make this record available
    table.populate_fts(["text", "country"])
    assert [
        {
            "rowid": 2,
            "text": "racoons are biting trash pandas",
            "country": "USA",
            "not_searchable": "bar",
        }
    ] == list(table.search("usa"))


@pytest.mark.parametrize("fts_version", ("4", "5"))
def test_fts_tokenize(fresh_db, fts_version):
    table_name = "searchable_{}".format(fts_version)
    table = fresh_db[table_name]
    table.insert_all(search_records)
    # Test without porter stemming
    table.enable_fts(
        ["text", "country"],
        fts_version="FTS{}".format(fts_version),
    )
    assert [] == list(table.search("bite"))
    # Test WITH stemming
    table.disable_fts()
    table.enable_fts(
        ["text", "country"],
        fts_version="FTS{}".format(fts_version),
        tokenize="porter",
    )
    rows = list(table.search("bite", order_by="rowid"))
    assert len(rows) == 1
    assert {
        "rowid": 2,
        "text": "racoons are biting trash pandas",
        "country": "USA",
        "not_searchable": "bar",
    }.items() <= rows[0].items()


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


def test_enable_fts_with_triggers(fresh_db):
    table = fresh_db["searchable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"], fts_version="FTS4", create_triggers=True)
    rows1 = list(table.search("tanuki"))
    assert len(rows1) == 1
    assert rows1 == [
        {
            "rowid": 1,
            "text": "tanuki are running tricksters",
            "country": "Japan",
            "not_searchable": "foo",
        }
    ]
    table.insert(search_records[1])
    # Triggers will auto-populate FTS virtual table, not need to call populate_fts()
    rows2 = list(table.search("usa"))
    assert rows2 == [
        {
            "rowid": 2,
            "text": "racoons are biting trash pandas",
            "country": "USA",
            "not_searchable": "bar",
        }
    ]
    assert [] == list(table.search("bar"))


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


def test_rebuild_fts(fresh_db):
    table = fresh_db["searchable"]
    table.insert(search_records[0])
    table.enable_fts(["text", "country"])
    # Run a search
    rows = list(table.search("are"))
    assert len(rows) == 1
    assert {
        "rowid": 1,
        "text": "tanuki are running tricksters",
        "country": "Japan",
        "not_searchable": "foo",
    }.items() <= rows[0].items()
    # Insert another record
    table.insert(search_records[1])
    # This should NOT show up in searches
    assert len(list(table.search("are"))) == 1
    # Running rebuild_fts() should fix it
    table.rebuild_fts()
    rows2 = list(table.search("are"))
    assert len(rows2) == 2


@pytest.mark.parametrize("invalid_table", ["does_not_exist", "not_searchable"])
def test_rebuild_fts_invalid(fresh_db, invalid_table):
    fresh_db["not_searchable"].insert({"foo": "bar"})
    # Raise OperationalError on invalid table
    with pytest.raises(sqlite3.OperationalError):
        fresh_db[invalid_table].rebuild_fts()


@pytest.mark.parametrize("fts_version", ["FTS4", "FTS5"])
def test_rebuild_removes_junk_docsize_rows(tmpdir, fts_version):
    # Recreating https://github.com/simonw/sqlite-utils/issues/149
    path = tmpdir / "test.db"
    db = Database(str(path), recursive_triggers=False)
    licenses = [{"key": "apache2", "name": "Apache 2"}, {"key": "bsd", "name": "BSD"}]
    db["licenses"].insert_all(licenses, pk="key", replace=True)
    db["licenses"].enable_fts(["name"], create_triggers=True, fts_version=fts_version)
    assert db["licenses_fts_docsize"].count == 2
    # Bug: insert with replace increases the number of rows in _docsize:
    db["licenses"].insert_all(licenses, pk="key", replace=True)
    assert db["licenses_fts_docsize"].count == 4
    # rebuild should fix this:
    db["licenses_fts"].rebuild_fts()
    assert db["licenses_fts_docsize"].count == 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"columns": ["title"]},
        {"fts_version": "FTS4"},
        {"create_triggers": True},
        {"tokenize": "porter"},
    ],
)
def test_enable_fts_replace(kwargs):
    db = Database(memory=True)
    db["books"].insert(
        {
            "id": 1,
            "title": "Habits of Australian Marsupials",
            "author": "Marlee Hawkins",
        },
        pk="id",
    )
    db["books"].enable_fts(["title", "author"])
    assert not db["books"].triggers
    assert db["books_fts"].columns_dict.keys() == {"title", "author"}
    assert "FTS5" in db["books_fts"].schema
    assert "porter" not in db["books_fts"].schema
    # Now modify the FTS configuration
    should_have_changed_columns = "columns" in kwargs
    if "columns" not in kwargs:
        kwargs["columns"] = ["title", "author"]
    db["books"].enable_fts(**kwargs, replace=True)
    # Check that the new configuration is correct
    if should_have_changed_columns:
        assert db["books_fts"].columns_dict.keys() == set(["title"])
    if "create_triggers" in kwargs:
        assert db["books"].triggers
    if "fts_version" in kwargs:
        assert "FTS4" in db["books_fts"].schema
    if "tokenize" in kwargs:
        assert "porter" in db["books_fts"].schema


def test_enable_fts_replace_does_nothing_if_args_the_same():
    queries = []
    db = Database(memory=True, tracer=lambda sql, params: queries.append((sql, params)))
    db["books"].insert(
        {
            "id": 1,
            "title": "Habits of Australian Marsupials",
            "author": "Marlee Hawkins",
        },
        pk="id",
    )
    db["books"].enable_fts(["title", "author"], create_triggers=True)
    queries.clear()
    # Running that again shouldn't run much SQL:
    db["books"].enable_fts(["title", "author"], create_triggers=True, replace=True)
    # The only SQL that executed should be select statements
    assert all(q[0].startswith("select ") for q in queries)


def test_enable_fts_error_message_on_views():
    db = Database(memory=True)
    db.create_view("hello", "select 1 + 1")
    with pytest.raises(NotImplementedError) as e:
        db["hello"].enable_fts()
        assert e.value.args[0] == "enable_fts() is supported on tables but not on views"


@pytest.mark.parametrize(
    "kwargs,fts,expected",
    [
        (
            {},
            "FTS5",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].*\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    [books_fts].rank"
            ),
        ),
        (
            {"columns": ["title"], "order_by": "rowid", "limit": 10},
            "FTS5",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        [title]\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].[title]\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    rowid\n"
                "limit 10"
            ),
        ),
        (
            {"where": "author = :author"},
            "FTS5",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                "    where author = :author\n"
                ")\n"
                "select\n"
                "    [original].*\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    [books_fts].rank"
            ),
        ),
        (
            {"columns": ["title"]},
            "FTS4",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        [title]\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].[title]\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    rank_bm25(matchinfo([books_fts], 'pcnalx'))"
            ),
        ),
        (
            {"offset": 1, "limit": 1},
            "FTS4",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].*\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    rank_bm25(matchinfo([books_fts], 'pcnalx'))\n"
                "limit 1 offset 1"
            ),
        ),
        (
            {"limit": 2},
            "FTS4",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].*\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    rank_bm25(matchinfo([books_fts], 'pcnalx'))\n"
                "limit 2"
            ),
        ),
        (
            {"where": "author = :author"},
            "FTS4",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                "    where author = :author\n"
                ")\n"
                "select\n"
                "    [original].*\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    rank_bm25(matchinfo([books_fts], 'pcnalx'))"
            ),
        ),
        (
            {"include_rank": True},
            "FTS5",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].*,\n"
                "    [books_fts].rank rank\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    [books_fts].rank"
            ),
        ),
        (
            {"include_rank": True},
            "FTS4",
            (
                "with original as (\n"
                "    select\n"
                "        rowid,\n"
                "        *\n"
                "    from [books]\n"
                ")\n"
                "select\n"
                "    [original].*,\n"
                "    rank_bm25(matchinfo([books_fts], 'pcnalx')) rank\n"
                "from\n"
                "    [original]\n"
                "    join [books_fts] on [original].rowid = [books_fts].rowid\n"
                "where\n"
                "    [books_fts] match :query\n"
                "order by\n"
                "    rank_bm25(matchinfo([books_fts], 'pcnalx'))"
            ),
        ),
    ],
)
def test_search_sql(kwargs, fts, expected):
    db = Database(memory=True)
    db["books"].insert(
        {
            "title": "Habits of Australian Marsupials",
            "author": "Marlee Hawkins",
        }
    )
    db["books"].enable_fts(["title", "author"], fts_version=fts)
    sql = db["books"].search_sql(**kwargs)
    assert sql == expected


@pytest.mark.parametrize(
    "input,expected",
    (
        ("dog", '"dog"'),
        ("cat,", '"cat,"'),
        ("cat's", '"cat\'s"'),
        ("dog.", '"dog."'),
        ("cat dog", '"cat" "dog"'),
        # If a phrase is already double quoted, leave it so
        ('"cat dog"', '"cat dog"'),
        ('"cat dog" fish', '"cat dog" "fish"'),
        # Sensibly handle unbalanced double quotes
        ('cat"', '"cat"'),
        ('"cat dog" "fish', '"cat dog" "fish"'),
    ),
)
def test_quote_fts_query(fresh_db, input, expected):
    table = fresh_db["searchable"]
    table.insert_all(search_records)
    table.enable_fts(["text", "country"])
    quoted = fresh_db.quote_fts(input)
    assert quoted == expected
    # Executing query does not crash.
    list(table.search(quoted))


def test_search_quote(fresh_db):
    table = fresh_db["searchable"]
    table.insert_all(search_records)
    table.enable_fts(["text", "country"])
    query = "cat's"
    with pytest.raises(sqlite3.OperationalError):
        list(table.search(query))
    # No exception with quote=True
    list(table.search(query, quote=True))
