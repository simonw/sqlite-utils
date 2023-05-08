# flake8: noqa
import pytest
import sys
from unittest.mock import MagicMock, call
from sqlite_utils.utils import sqlite3


def test_register_function(fresh_db):
    @fresh_db.register_function
    def reverse_string(s):
        return "".join(reversed(list(s)))

    result = fresh_db.execute('select reverse_string("hello")').fetchone()[0]
    assert result == "olleh"


def test_register_function_custom_name(fresh_db):
    @fresh_db.register_function(name="revstr")
    def reverse_string(s):
        return "".join(reversed(list(s)))

    result = fresh_db.execute('select revstr("hello")').fetchone()[0]
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


def test_register_function_deterministic_tries_again_if_exception_raised(fresh_db):
    fresh_db.conn = MagicMock()
    fresh_db.conn.create_function = MagicMock()

    @fresh_db.register_function(deterministic=True)
    def to_lower_2(s):
        return s.lower()

    fresh_db.conn.create_function.assert_called_with(
        "to_lower_2", 1, to_lower_2, deterministic=True
    )

    first = True

    def side_effect(*args, **kwargs):
        # Raise exception only first time this is called
        nonlocal first
        if first:
            first = False
            raise sqlite3.NotSupportedError()

    # But if sqlite3.NotSupportedError is raised, it tries again
    fresh_db.conn.create_function.reset_mock()
    fresh_db.conn.create_function.side_effect = side_effect

    @fresh_db.register_function(deterministic=True)
    def to_lower_3(s):
        return s.lower()

    # Should have been called once with deterministic=True and once without
    assert fresh_db.conn.create_function.call_args_list == [
        call("to_lower_3", 1, to_lower_3, deterministic=True),
        call("to_lower_3", 1, to_lower_3),
    ]


def test_register_function_replace(fresh_db):
    @fresh_db.register_function()
    def one():
        return "one"

    assert "one" == fresh_db.execute("select one()").fetchone()[0]

    # This will silently fail to replaec the function
    @fresh_db.register_function()
    def one():  # noqa
        return "two"

    assert "one" == fresh_db.execute("select one()").fetchone()[0]

    # This will replace it
    @fresh_db.register_function(replace=True)
    def one():  # noqa
        return "two"

    assert "two" == fresh_db.execute("select one()").fetchone()[0]
