import csv
import io

import pytest

from sqlite_utils import utils


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
    long_csv = f"id,text\n1,{long_value}"
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


@pytest.mark.parametrize(
    "input,expected",
    (
        ([], []),
        (["id", "name"], ["id", "name"]),
        (["id", "id"], ["id", "id_2"]),
        (["id", "id", "id"], ["id", "id_2", "id_3"]),
        # A renamed duplicate must not clobber a real column called id_2
        (["id", "id", "id_2"], ["id", "id_3", "id_2"]),
        (["id_2", "id", "id"], ["id_2", "id", "id_3"]),
        (["id", "id", "id_2", "id_2"], ["id", "id_3", "id_2", "id_2_2"]),
    ),
)
def test_dedupe_keys(input, expected):
    assert utils.dedupe_keys(input) == expected


def test_dedupe_keys_accepts_generator_input():
    assert utils.dedupe_keys(key for key in ["id", "id", "id_2"]) == [
        "id",
        "id_3",
        "id_2",
    ]


def test_dedupe_keys_handles_repeated_suffix_bases():
    assert utils.dedupe_keys(["id", "id", "id_2", "id_2", "id_2_2", "id_2_2"]) == [
        "id",
        "id_3",
        "id_2",
        "id_2_3",
        "id_2_2",
        "id_2_2_2",
    ]


def test_dedupe_keys_scales_without_reformatting_all_suffixes():
    class CountingStr(str):
        calls = 0

        def __format__(self, format_spec):
            type(self).calls += 1
            return super().__format__(format_spec)

    keys = [CountingStr("id") for _ in range(2000)]
    result = utils.dedupe_keys(keys)

    assert len(result) == 2000
    assert len(set(result)) == 2000
    assert result[0] == "id"
    assert result[-1] == "id_2000"
    assert CountingStr.calls <= 2000
