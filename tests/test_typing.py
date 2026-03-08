import typing

from sqlite_utils.db import Database, Table, View


def test_database_getitem_return_type_hint_is_table():
    assert typing.get_type_hints(Database.__getitem__)["return"] is Table


def test_database_getitem_still_returns_view_for_views(fresh_db):
    fresh_db["items"].insert({"name": "one"})
    fresh_db.create_view("items_view", "select * from items")
    assert isinstance(fresh_db["items_view"], View)
