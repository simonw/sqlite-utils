import pytest


def _generator(values):
    yield from values


@pytest.fixture(params=["FTS4", "FTS5"])
def searchable_table(fresh_db, request):
    table = fresh_db["books"]
    table.insert_all(
        [
            {"title": "sqlite first", "author": "A", "notes": "one"},
            {"title": "sqlite second", "author": "B", "notes": "two"},
            {"title": "sqlite third", "author": "C", "notes": "three"},
        ]
    )
    table.enable_fts(["title"], fts_version=request.param)
    return table


@pytest.mark.parametrize("entrypoint", ["search_sql", "search"])
@pytest.mark.parametrize("names", [[], ["title"], ["author", "title"]])
@pytest.mark.parametrize(
    "factory",
    [list, tuple, iter, _generator],
    ids=["list", "tuple", "iterator", "generator"],
)
def test_search_column_iterables(searchable_table, entrypoint, names, factory):
    columns = factory(names)
    kwargs = dict(
        columns=columns,
        order_by='"original".rowid',
        limit=2,
        offset=1,
        where="rowid >= :minimum",
    )
    if entrypoint == "search_sql":
        sql = searchable_table.search_sql(**kwargs)
        actual = list(searchable_table.db.query(sql, {"query": "sqlite", "minimum": 1}))
    else:
        actual = list(
            searchable_table.search("sqlite", where_args={"minimum": 1}, **kwargs)
        )

    expected = [
        {"rowid": 2, "title": "sqlite second", "author": "B", "notes": "two"},
        {"rowid": 3, "title": "sqlite third", "author": "C", "notes": "three"},
    ]
    if names:
        expected = [{name: row[name] for name in names} for row in expected]
    assert actual == expected
    assert [list(row) for row in actual] == [list(row) for row in expected]
    if isinstance(columns, (list, tuple)):
        assert list(columns) == names


def test_search_none_columns_still_returns_all_columns(searchable_table):
    results = list(searchable_table.search("sqlite", columns=None, order_by="rowid"))
    assert len(results) == 3
    assert list(results[0]) == ["rowid", "title", "author", "notes"]


def test_search_iterator_with_rank(searchable_table):
    results = list(
        searchable_table.search("sqlite", columns=iter(["title"]), include_rank=True)
    )
    assert {row["title"] for row in results} == {
        "sqlite first",
        "sqlite second",
        "sqlite third",
    }
    assert all(list(row) == ["title", "rank"] for row in results)
    assert all(isinstance(row["rank"], float) for row in results)


@pytest.mark.parametrize("fts", ["FTS4", "FTS5"])
def test_search_iterator_quoted_columns_and_alias_collision(fresh_db, fts):
    table = fresh_db["original"]
    table.insert({"text": "sqlite", 'with "quote': "A", "with space": "B"})
    table.enable_fts(["text"], fts_version=fts)
    columns = ('with "quote', "with space")
    expected = [{'with "quote': "A", "with space": "B"}]
    assert list(table.search("sqlite", columns=columns)) == expected
    assert list(table.search("sqlite", columns=iter(columns))) == expected
