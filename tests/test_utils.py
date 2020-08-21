from sqlite_utils import utils
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


def test_find_spatialite():
    spatialite = utils.find_spatialite()
    assert spatialite is None or isinstance(spatialite, str)
