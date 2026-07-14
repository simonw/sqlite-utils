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


# Regression tests for #439: progress bar against multi-byte encodings


def _collect_updates(rows):
    """Iterate the wrapper, capturing every update() value."""
    return list(rows)


def _make_temp(content_bytes, tmp_path, name):
    path = tmp_path / name
    path.write_bytes(content_bytes)
    return path


def test_updatewrapper_utf8_reports_byte_lengths(tmp_path):
    # Sanity: ASCII / UTF-8 still hits 100% (this was already correct,
    # but we want a baseline to protect.)
    raw = b"a,b\n1,2\n3,4\n"
    path = _make_temp(raw, tmp_path, "in.csv")
    updates = []
    with open(path, "rb") as fp:
        wrapper = utils.UpdateWrapper(io.TextIOWrapper(fp, encoding="utf-8"), updates.append)
        _collect_updates(wrapper)
    assert sum(updates) == len(raw)


def test_updatewrapper_utf16le_reports_byte_lengths(tmp_path):
    # Without the fix this test fails: the bar only reaches len(decoded)
    # which is half the raw byte length for UTF-16-LE.
    raw = "a,b\n1,2\n3,4\n".encode("utf-16-le")
    path = _make_temp(raw, tmp_path, "in.csv")
    updates = []
    with open(path, "rb") as fp:
        wrapper = utils.UpdateWrapper(io.TextIOWrapper(fp, encoding="utf-16-le"), updates.append)
        _collect_updates(wrapper)
    assert sum(updates) == len(raw)


def test_updatewrapper_utf16le_with_bom_reaches_total_bytes(tmp_path):
    # BOM-prefixed UTF-16. The BOM byte is consumed by the TextIOWrapper
    # before iteration starts; we should still account for the full file
    # size so the bar reaches 100%.
    raw = "﻿" + "a,b\n1,2\n3,4\n"
    raw_bytes = raw.encode("utf-16-le")
    path = _make_temp(raw_bytes, tmp_path, "in.csv")
    updates = []
    with open(path, "rb") as fp:
        wrapper = utils.UpdateWrapper(io.TextIOWrapper(fp, encoding="utf-16"), updates.append)
        _collect_updates(wrapper)
    assert sum(updates) == len(raw_bytes)


def test_updatewrapper_through_buffered_reader(tmp_path):
    # The --sniff path wraps the raw file in io.BufferedReader before the
    # TextIOWrapper. Progress reporting must still resolve to the binary
    # file's byte count.
    raw = "a,b\n1,2\n3,4\n".encode("utf-16-le")
    path = _make_temp(raw, tmp_path, "in.csv")
    updates = []
    with open(path, "rb") as fp:
        buffered = io.BufferedReader(fp, buffer_size=4096)
        wrapper = utils.UpdateWrapper(
            io.TextIOWrapper(buffered, encoding="utf-16-le"), updates.append
        )
        _collect_updates(wrapper)
    assert sum(updates) == len(raw)


def test_updatewrapper_binary_file_unchanged(tmp_path):
    # If the wrapped object is itself a raw binary file (no .buffer attr),
    # we should keep the old behaviour: iterate yields bytes and len() is
    # already the byte count.
    raw = b"a,b\n1,2\n3,4\n"
    path = _make_temp(raw, tmp_path, "in.csv")
    updates = []
    with open(path, "rb") as fp:
        wrapper = utils.UpdateWrapper(fp, updates.append)
        _collect_updates(wrapper)
    assert sum(updates) == len(raw)


def test_updatewrapper_read_path_utf16le(tmp_path):
    # The .read() path is used by the JSON loader (not the CSV iterator),
    # but must agree with the iterator path on byte accounting.
    raw = '{"a": 1}'.encode("utf-16-le")
    path = _make_temp(raw, tmp_path, "in.json")
    updates = []
    with open(path, "rb") as fp:
        wrapper = utils.UpdateWrapper(io.TextIOWrapper(fp, encoding="utf-16-le"), updates.append)
        wrapper.read()
    assert sum(updates) == len(raw)
