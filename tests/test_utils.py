from sqlite_utils import utils
import csv
import io
import pytest


@pytest.mark.parametrize(
    "input,expected,should_be_is",
    [
        ({}, None, True),
        ({"foo": "bar"}, None, True),
        (
            {"content": {"$base64": True, "encoded": "aGVsbG8="}},
            {"content": b"hello"},
            False,
        ),
    ],
)
def test_decode_base64_values(input, expected, should_be_is):
    actual = utils.decode_base64_values(input)
    if should_be_is:
        assert actual is input
    else:
        assert actual == expected


@pytest.mark.parametrize(
    "size,expected",
    (
        (1, [["a"], ["b"], ["c"], ["d"]]),
        (2, [["a", "b"], ["c", "d"]]),
        (3, [["a", "b", "c"], ["d"]]),
        (4, [["a", "b", "c", "d"]]),
    ),
)
def test_chunks(size, expected):
    input = ["a", "b", "c", "d"]
    chunks = list(map(list, utils.chunks(input, size)))
    assert chunks == expected


def test_hash_record():
    expected = "d383e7c0ba88f5ffcdd09be660de164b3847401a"
    assert utils.hash_record({"name": "Cleo", "twitter": "CleoPaws"}) == expected
    assert (
        utils.hash_record(
            {"name": "Cleo", "twitter": "CleoPaws", "age": 7}, keys=("name", "twitter")
        )
        == expected
    )
    assert (
        utils.hash_record({"name": "Cleo", "twitter": "CleoPaws", "age": 7}) != expected
    )


def test_maximize_csv_field_size_limit():
    # Reset to default in case other tests have changed it
    csv.field_size_limit(utils.ORIGINAL_CSV_FIELD_SIZE_LIMIT)
    long_value = "a" * 131073
    long_csv = "id,text\n1,{}".format(long_value)
    fp = io.BytesIO(long_csv.encode("utf-8"))
    # Using rows_from_file should error
    with pytest.raises(csv.Error):
        rows, _ = utils.rows_from_file(fp, utils.Format.CSV)
        list(rows)
    # But if we call maximize_csv_field_size_limit() first it should be OK:
    utils.maximize_csv_field_size_limit()
    fp2 = io.BytesIO(long_csv.encode("utf-8"))
    rows2, _ = utils.rows_from_file(fp2, utils.Format.CSV)
    rows_list2 = list(rows2)
    assert len(rows_list2) == 1
    assert rows_list2[0]["id"] == "1"
    assert rows_list2[0]["text"] == long_value


@pytest.mark.parametrize(
    "input,expected",
    (
        ({"foo": {"bar": 1}}, {"foo_bar": 1}),
        ({"foo": {"bar": [1, 2, {"baz": 3}]}}, {"foo_bar": [1, 2, {"baz": 3}]}),
        ({"foo": {"bar": 1, "baz": {"three": 3}}}, {"foo_bar": 1, "foo_baz_three": 3}),
    ),
)
def test_flatten(input, expected):
    assert utils.flatten(input) == expected
