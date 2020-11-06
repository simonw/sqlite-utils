import pytest
import sys
from unittest.mock import MagicMock


def test_register_function(fresh_db):
    @fresh_db.register_function
    def reverse_string(s):
        return "".join(reversed(list(s)))

    result = fresh_db.execute('select reverse_string("hello")').fetchone()[0]
    assert result == "olleh"


def test_register_function_multiple_arguments(fresh_db):
    @fresh_db.register_function
    def a_times_b_plus_c(a, b, c):
        return a * b + c

    result = fresh_db.execute("select a_times_b_plus_c(2, 3, 4)").fetchone()[0]
    assert result == 10


def test_register_function_deterministic(fresh_db):
    @fresh_db.register_function(deterministic=True)
    def to_lower(s):
        return s.lower()

    result = fresh_db.execute("select to_lower('BOB')").fetchone()[0]
    assert result == "bob"


@pytest.mark.skipif(
    sys.version_info < (3, 8), reason="deterministic=True was added in Python 3.8"
)
def test_register_function_deterministic_registered(fresh_db):
    fresh_db.conn = MagicMock()
    fresh_db.conn.create_function = MagicMock()

    @fresh_db.register_function(deterministic=True)
    def to_lower_2(s):
        return s.lower()

    fresh_db.conn.create_function.assert_called_with(
        "to_lower_2", 1, to_lower_2, deterministic=True
    )


def test_register_function_replace(fresh_db):
    @fresh_db.register_function()
    def one():
        return "one"

    assert "one" == fresh_db.execute("select one()").fetchone()[0]

    # This will fail to replace the function:
    @fresh_db.register_function()
    def one():
        return "two"

    assert "one" == fresh_db.execute("select one()").fetchone()[0]

    # This will replace it
    @fresh_db.register_function(replace=True)
    def one():
        return "two"

    assert "two" == fresh_db.execute("select one()").fetchone()[0]
