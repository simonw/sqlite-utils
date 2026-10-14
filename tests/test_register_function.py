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
    # Save the original connection so we can close it later
    original_conn = fresh_db.conn
    fresh_db.conn = MagicMock()
    fresh_db.conn.create_function = MagicMock()

    try:

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
    finally:
        # Close the original connection that was replaced with the mock
        original_conn.close()


def test_register_function_replace(fresh_db):
    @fresh_db.register_function()
    def one():  # pyright: ignore[reportRedeclaration]
        return "one"

    assert "one" == fresh_db.execute("select one()").fetchone()[0]

    # This will silently fail to replaec the function
    @fresh_db.register_function()
    def one():  # pyright: ignore[reportRedeclaration]
        return "two"

    assert "one" == fresh_db.execute("select one()").fetchone()[0]

    # This will replace it
    @fresh_db.register_function(replace=True)
    def one():  # pyright: ignore[reportRedeclaration]
        return "two"

    assert "two" == fresh_db.execute("select one()").fetchone()[0]


@pytest.mark.parametrize("deterministic", (False, True))
def test_deregister_function(fresh_db, deterministic):
    calls = []

    def temporary(value):
        calls.append(value)
        return value * 2

    fresh_db.register_function(temporary, deterministic=deterministic)
    assert fresh_db.execute("select temporary(3)").fetchone()[0] == 6
    assert fresh_db.register_function(temporary, deregister=True) is None
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.execute("select temporary(4)").fetchone()
    assert calls == [3]

    fresh_db.register_function(temporary)
    assert fresh_db.execute("select temporary(5)").fetchone()[0] == 10
    assert calls == [3, 5]


@pytest.mark.parametrize("removal_name", ("custom_name", "CUSTOM_NAME"))
def test_deregister_function_custom_name(fresh_db, removal_name):
    def temporary(value):
        return value * 2

    fresh_db.register_function(temporary)
    fresh_db.register_function(temporary, name="custom_name")
    fresh_db.register_function(temporary, name=removal_name, deregister=True)
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.execute("select custom_name(3)").fetchone()
    assert fresh_db.execute("select temporary(3)").fetchone()[0] == 6
    fresh_db.register_function(temporary, name="custom_name")
    assert fresh_db.execute("select custom_name(4)").fetchone()[0] == 8


def test_deregister_function_preserves_other_arity(fresh_db):
    def one_arg(value):
        return value + 1

    def two_args(left, right):
        return left + right

    fresh_db.register_function(one_arg, name="temporary")
    fresh_db.register_function(two_args, name="temporary")
    fresh_db.register_function(one_arg, name="temporary", deregister=True)
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.execute("select temporary(1)").fetchone()
    assert fresh_db.execute("select temporary(1, 2)").fetchone()[0] == 3


def test_deregister_function_preserves_non_ascii_names(fresh_db):
    def temporary(value):
        return value * 2

    fresh_db.register_function(temporary, name="FÜ")
    fresh_db.register_function(temporary, name="fü")
    fresh_db.register_function(temporary, name="fÜ", deregister=True)
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.execute('select "FÜ"(1)').fetchone()
    assert fresh_db.execute('select "fü"(2)').fetchone()[0] == 4
    # A surviving registration must still suppress replacement by default.
    fresh_db.register_function(lambda value: 0, name="fü")
    assert fresh_db.execute('select "fü"(2)').fetchone()[0] == 4


def test_deregister_function_idempotent(fresh_db):
    def temporary():
        return 1

    # Removing an unregistered function or removing it twice is harmless.
    fresh_db.register_function(temporary, deregister=True)
    fresh_db.register_function(temporary)
    fresh_db.register_function(temporary, deregister=True)
    fresh_db.register_function(temporary, deregister=True)
    fresh_db.register_function(temporary)
    assert fresh_db.execute("select temporary()").fetchone()[0] == 1


def test_deregister_function_failure_preserves_cache(fresh_db):
    def temporary(value):
        return value * 2

    fresh_db.register_function(temporary)
    original_conn = fresh_db.conn
    fresh_db.conn = MagicMock()
    fresh_db.conn.create_function.side_effect = sqlite3.OperationalError("busy")
    try:
        with pytest.raises(sqlite3.OperationalError, match="busy"):
            fresh_db.register_function(temporary, deregister=True)
        fresh_db.conn.create_function.assert_called_once_with("temporary", 1, None)
    finally:
        fresh_db.conn = original_conn
    fresh_db.register_function(lambda value: 0, name="temporary")
    assert fresh_db.execute("select temporary(3)").fetchone()[0] == 6


def test_deregister_function_decorator(fresh_db):
    def temporary():
        return 1

    fresh_db.register_function(temporary)
    assert fresh_db.register_function(deregister=True)(temporary) is temporary
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.execute("select temporary()").fetchone()
    fresh_db.register_function()(temporary)
    assert fresh_db.execute("select temporary()").fetchone()[0] == 1
