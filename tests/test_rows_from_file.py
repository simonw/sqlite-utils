from sqlite_utils.utils import rows_from_file, Format, RowError
from io import BytesIO, StringIO
import pytest


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
        rows, format = rows_from_file(
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
        rows_from_file(StringIO("id,name\r\n1,Cleo"))
    assert ex.value.args == (
        "rows_from_file() requires a file-like object that supports peek(), such as io.BytesIO",
    )
