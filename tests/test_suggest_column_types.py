import pytest
from collections import OrderedDict
from sqlite_utils.utils import suggest_column_types


@pytest.mark.parametrize(
    "records,types",
    [
        ([{"a": 1}], {"a": int}),
        ([{"a": "baz"}], {"a": str}),
        ([{"a": 1.2}], {"a": float}),
        ([{"a": [1]}], {"a": str}),
        ([{"a": (1,)}], {"a": str}),
        ([{"a": {"b": 1}}], {"a": str}),
        ([{"a": OrderedDict({"b": 1})}], {"a": str}),
        ([{"a": 1}, {"a": 1.1}], {"a": float}),
        ([{"a": b"b"}], {"a": bytes}),
    ],
)
def test_suggest_column_types(records, types):
    assert types == suggest_column_types(records)
