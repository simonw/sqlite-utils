import sqlite3

import hypothesis.strategies as st
import pytest
from hypothesis import given

from sqlite_utils.create_table_parser import Check, ParseError, parse_checks


def test_parse_column_and_table_checks():
    sql = """
        CREATE TABLE people (
            age INTEGER CONSTRAINT positive CHECK (age > 0),
            status TEXT CHECK(status IN ('active', 'inactive')),
            CONSTRAINT adult CHECK(age >= 18)
        )
    """
    assert parse_checks(sql) == [
        Check("age > 0", name="positive", column="age"),
        Check(
            "status IN ('active', 'inactive')",
            column="status",
            options=["active", "inactive"],
        ),
        Check("age >= 18", name="adult"),
    ]
    checks = parse_checks(sql)
    assert checks[0].sql == "CONSTRAINT positive CHECK (age > 0)"
    assert sql[checks[0].start : checks[0].end] == checks[0].sql
    assert checks[1].sql == "CHECK(status IN ('active', 'inactive'))"
    assert sql[checks[2].start : checks[2].end] == checks[2].sql


def test_comments_are_trivia_not_constraints():
    sql = """
        CREATE /* fake CHECK (nope), ( */ TABLE t (
            a INTEGER /* CHECK (a < 0), phantom */,
            b INTEGER CHECK /* between keyword and expression */ (b > 0),
            /* CHECK (also_fake) */ CONSTRAINT upper CHECK(b < 10)
        )
    """
    sqlite3.connect(":memory:").execute(sql)
    assert parse_checks(sql) == [
        Check("b > 0", column="b"),
        Check("b < 10", name="upper"),
    ]


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("value IN ('one', 'two')", ["one", "two"]),
        ("((value IN ('one', 'two')))", ["one", "two"]),
        ("value NOT IN ('one', 'two')", None),
        ("value IN ('one', 'two') OR enabled", None),
        ("other IN ('one', 'two')", None),
        ("value IN (lower('one'), 'two')", None),
        ('value IN ("other")', None),
    ],
)
def test_options_only_for_exact_literal_in_check(expression, expected):
    sql = f"CREATE TABLE t(value TEXT CHECK({expression}), enabled INTEGER, other TEXT)"
    sqlite3.connect(":memory:").execute(sql)
    assert parse_checks(sql)[0].options == expected


@pytest.mark.parametrize("column", ["💩x", "e\u0301"])
def test_unquoted_unicode_identifiers(column):
    sql = f"CREATE TABLE t({column} INTEGER CHECK({column} > 0))"
    sqlite3.connect(":memory:").execute(sql)
    assert parse_checks(sql) == [Check(f"{column} > 0", column=column)]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CHECK(x > 0)",
        "CREATE TABLE t(x INTEGER CHECK(x > 0)",
        "CREATE TABLE t(x TEXT CHECK(x != 'unterminated))",
        "CREATE TABLE t(x INTEGER /* unterminated)",
    ],
)
def test_invalid_sql_raises_parse_error(sql):
    with pytest.raises(ParseError):
        parse_checks(sql)


def test_virtual_table_has_no_checks():
    assert (
        parse_checks("CREATE /* comment */ VIRTUAL TABLE search USING fts5(text)") == []
    )


comment_or_space = st.sampled_from(
    [
        " ",
        "\n  ",
        "/* comment with , ( ) and CHECK(fake) */",
        "-- comment with , ( ) and CHECK(fake)\n",
    ]
)


@given(gaps=st.lists(comment_or_space, min_size=5, max_size=5))
def test_comments_and_whitespace_can_separate_check_tokens(gaps):
    sql = (
        f"CREATE{gaps[0]}TABLE{gaps[1]}t{gaps[2]}("
        f"value INTEGER CHECK{gaps[3]}(value{gaps[4]}> 0))"
    )
    connection = sqlite3.connect(":memory:")
    connection.execute(sql)
    stored_sql = connection.execute(
        "select sql from sqlite_schema where name = 't'"
    ).fetchone()[0]
    assert parse_checks(stored_sql) == [Check(f"value{gaps[4]}> 0", column="value")]


safe_string_text = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cc", "Cs"),
        blacklist_characters=("'",),
    ),
    max_size=40,
)


@given(value=safe_string_text)
def test_check_like_text_inside_strings_is_opaque(value):
    sql = f"CREATE TABLE t(value TEXT CHECK(value != '{value}'))"
    connection = sqlite3.connect(":memory:")
    connection.execute(sql)
    stored_sql = connection.execute(
        "select sql from sqlite_schema where name = 't'"
    ).fetchone()[0]
    checks = parse_checks(stored_sql)
    assert len(checks) == 1
    assert checks[0].column == "value"
    assert checks[0].check == f"value != '{value}'"
