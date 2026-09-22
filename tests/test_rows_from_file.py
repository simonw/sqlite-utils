import io
import json
from io import BytesIO, StringIO

import pytest

from sqlite_utils.utils import Format, RowError, rows_from_file


@pytest.mark.parametrize(
    "input,expected_format",
    (
        (b"id,name\n1,Cleo", Format.CSV),
        (b"id\tname\n1\tCleo", Format.TSV),
        (b'[{"id": "1", "name": "Cleo"}]', Format.JSON),
    ),
)
def test_rows_from_file_detect_format(input, expected_format):
    rows, format = rows_from_file(BytesIO(input))
    assert format == expected_format
    rows_list = list(rows)
    assert rows_list == [{"id": "1", "name": "Cleo"}]


@pytest.mark.parametrize("input", (b"", b" \n\t"))
def test_rows_from_file_empty_input(input):
    rows, format = rows_from_file(BytesIO(input))
    assert format == Format.CSV
    assert list(rows) == []


@pytest.mark.parametrize(
    "ignore_extras,extras_key,expected",
    (
        (True, None, [{"id": "1", "name": "Cleo"}]),
        (False, "_rest", [{"id": "1", "name": "Cleo", "_rest": ["oops"]}]),
        # expected of None means expect an error:
        (False, False, None),
    ),
)
def test_rows_from_file_extra_fields_strategies(ignore_extras, extras_key, expected):
    try:
        rows, _format = rows_from_file(
            BytesIO(b"id,name\r\n1,Cleo,oops"),
            format=Format.CSV,
            ignore_extras=ignore_extras,
            extras_key=extras_key,
        )
        list_rows = list(rows)
    except RowError:
        if expected is None:
            # This is fine,
            return
        else:
            # We did not expect an error
            raise
    assert list_rows == expected


def test_rows_from_file_error_on_string_io():
    with pytest.raises(TypeError) as ex:
        rows_from_file(StringIO("id,name\r\n1,Cleo"))  # type: ignore[arg-type]
    assert ex.value.args == (
        "rows_from_file() requires a file-like object that supports peek(), such as io.BytesIO",
    )


@pytest.fixture
def buffered_readers(monkeypatch):
    # Keep wrappers alive so these checks cannot pass due to garbage collection.
    readers = []
    original = io.BufferedReader

    def buffered_reader(*args, **kwargs):
        reader = original(*args, **kwargs)
        readers.append(reader)
        return reader

    monkeypatch.setattr(io, "BufferedReader", buffered_reader)
    yield readers
    for reader in readers:
        reader.close()


@pytest.mark.parametrize(
    "content, expected_format, expected_rows",
    [
        (b'[{"id": 1}]', Format.JSON, [{"id": 1}]),
        (b'{"id": 1}', Format.JSON, [{"id": 1}]),
        (b"[]", Format.JSON, []),
        (b"", Format.CSV, []),
        (b" \n\t", Format.CSV, []),
    ],
)
def test_detect_format_closes_eager_reader(
    tmp_path, buffered_readers, content, expected_format, expected_rows
):
    path = tmp_path / "input"
    path.write_bytes(content)
    with path.open("rb") as fp:
        rows, detected = rows_from_file(fp)
        assert detected == expected_format
        assert list(rows) == expected_rows
        assert len(buffered_readers) == 1
        assert buffered_readers[0].closed


@pytest.mark.parametrize("content", [b"[", b'{"id":'])
def test_detect_format_closes_reader_on_invalid_json(
    tmp_path, buffered_readers, content
):
    path = tmp_path / "input.json"
    path.write_bytes(content)
    with path.open("rb") as fp:
        with pytest.raises(json.JSONDecodeError):
            rows_from_file(fp)
        assert len(buffered_readers) == 1
        assert buffered_readers[0].closed


@pytest.mark.parametrize(
    "delimiter, expected_format", [(b",", Format.CSV), (b"\t", Format.TSV)]
)
def test_detect_format_keeps_streaming_reader_open(
    buffered_readers, delimiter, expected_format
):
    content = b"id" + delimiter + b"name\n" + (b"1" + delimiter + b"Cleo\n") * 2000
    rows, detected = rows_from_file(BytesIO(content))
    assert detected == expected_format
    assert len(buffered_readers) == 1
    assert not buffered_readers[0].closed
    assert len(list(rows)) == 2000
    assert buffered_readers[0].closed
