def test_enable_counts(fresh_db):
    foo = fresh_db["foo"]
    assert fresh_db.table_names() == []
    for i in range(10):
        foo.insert({"name": "item {}".format(i)})
    assert fresh_db.table_names() == ["foo"]
    assert foo.count == 10
    # Now enable counts
    foo.enable_counts()
    assert foo.triggers_dict == {
        "foo_counts_insert": "CREATE TRIGGER [foo_counts_insert] AFTER INSERT ON [foo]\nBEGIN\n    INSERT OR REPLACE INTO [_counts]\n    VALUES (\n        'foo',\n        COALESCE(\n            (SELECT count FROM [_counts] WHERE [table] = 'foo'),\n        0\n        ) + 1\n    );\nEND",
        "foo_counts_delete": "CREATE TRIGGER [foo_counts_delete] AFTER DELETE ON [foo]\nBEGIN\n    INSERT OR REPLACE INTO [_counts]\n    VALUES (\n        'foo',\n        COALESCE(\n            (SELECT count FROM [_counts] WHERE [table] = 'foo'),\n        0\n        ) - 1\n    );\nEND",
    }
    assert fresh_db.table_names() == ["foo", "_counts"]
    assert list(fresh_db["_counts"].rows) == [{"count": 10, "table": "foo"}]
    # Add some items to test the triggers
    for i in range(5):
        foo.insert({"name": "item {}".format(10 + i)})
    assert foo.count == 15
    assert list(fresh_db["_counts"].rows) == [{"count": 15, "table": "foo"}]
    # Delete some items
    foo.delete_where("rowid < 7")
    assert foo.count == 9
    assert list(fresh_db["_counts"].rows) == [{"count": 9, "table": "foo"}]
    foo.delete_where()
    assert foo.count == 0
    assert list(fresh_db["_counts"].rows) == [{"count": 0, "table": "foo"}]
