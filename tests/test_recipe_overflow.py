import pytest

from sqlite_utils import recipes


OVERFLOWING_DATE = "999999999999999999999999999999-01-01"


@pytest.mark.parametrize("fn", ("parsedate", "parsedatetime"))
def test_dateparse_overflow_raises_by_default(fn):
    with pytest.raises(OverflowError):
        getattr(recipes, fn)(OVERFLOWING_DATE)


@pytest.mark.parametrize("fn", ("parsedate", "parsedatetime"))
@pytest.mark.parametrize(
    "errors,expected",
    (
        (recipes.IGNORE, OVERFLOWING_DATE),
        (recipes.SET_NULL, None),
    ),
)
def test_dateparse_overflow_respects_errors(fn, errors, expected):
    assert getattr(recipes, fn)(OVERFLOWING_DATE, errors=errors) == expected
