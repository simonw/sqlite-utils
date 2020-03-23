import pytest
from collections import OrderedDict
from sqlite_utils.utils import suggest_column_types


@pytest.mark.parametrize(
    "records,types",
    [
        ([{"a": 1}], {"a": int}),
        ([{"a": 1}, {"a": None}], {"a": int}),
        ([{"a": "baz"}], {"a": str}),
        ([{"a": "baz"}, {"a": None}], {"a": str}),
        ([{"a": 1.2}], {"a": float}),
        ([{"a": 1.2}, {"a": None}], {"a": float}),
        ([{"a": [1]}], {"a": str}),
        ([{"a": [1]}, {"a": None}], {"a": str}),
        ([{"a": (1,)}], {"a": str}),
        ([{"a": {"b": 1}}], {"a": str}),
        ([{"a": {"b": 1}}, {"a": None}], {"a": str}),
        ([{"a": OrderedDict({"b": 1})}], {"a": str}),
        ([{"a": 1}, {"a": 1.1}], {"a": float}),
        ([{"a": b"b"}], {"a": bytes}),
        ([{"a": b"b"}, {"a": None}], {"a": bytes}),
        ([{"a": "a", "b": None}], {"a": str, "b": str}),
    ],
)
def test_suggest_column_types(records, types):
    assert types == suggest_column_types(records)
