"""Tests for reading compound (multi-column) foreign keys - issue #594."""

import pytest
from sqlite_utils import Database

COMPOUND_SCHEMA = """
CREATE TABLE departments (
    campus_name TEXT NOT NULL,
    dept_code TEXT NOT NULL,
    dept_name TEXT,
    PRIMARY KEY (campus_name, dept_code)
);
CREATE TABLE courses (
    course_code TEXT PRIMARY KEY,
    course_name TEXT,
    campus_name TEXT NOT NULL,
    dept_code TEXT NOT NULL,
    FOREIGN KEY (campus_name, dept_code)
    REFERENCES departments(campus_name, dept_code)
);
"""


@pytest.fixture
def compound_db():
    db = Database(memory=True)
    db.executescript(COMPOUND_SCHEMA)
    return db


def test_compound_foreign_key(compound_db):
    fks = compound_db["courses"].foreign_keys
    assert len(fks) == 1
    fk = fks[0]
    assert fk.is_compound is True
    assert fk.table == "courses"
    assert fk.other_table == "departments"
    assert fk.columns == ["campus_name", "dept_code"]
    assert fk.other_columns == ["campus_name", "dept_code"]
    # Scalar column/other_column can't sensibly hold a compound key
    assert fk.column is None
    assert fk.other_column is None


def test_single_foreign_key_gets_columns_fields(fresh_db):
    fresh_db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    fresh_db["books"].insert({"title": "Hedgehogs", "author_id": 1})
    fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    fk = fresh_db["books"].foreign_keys[0]
    assert fk.is_compound is False
    assert fk.column == "author_id"
    assert fk.other_column == "id"
    assert fk.columns == ["author_id"]
    assert fk.other_columns == ["id"]


def test_foreign_key_no_longer_unpacks_as_tuple(fresh_db):
    # Clean break in 4.0: ForeignKey is a dataclass, not a namedtuple, so the
    # old tuple unpacking and indexing patterns now fail hard.
    fresh_db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    fresh_db["books"].insert({"title": "Hedgehogs", "author_id": 1})
    fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    fk = fresh_db["books"].foreign_keys[0]
    with pytest.raises(TypeError):
        table, column, other_table, other_column = fk
    with pytest.raises(TypeError):
        fk[0]


def test_foreign_keys_are_sortable(fresh_db):
    fresh_db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    fresh_db["categories"].insert({"id": 1, "name": "Wildlife"}, pk="id")
    fresh_db["books"].insert({"title": "Hedgehogs", "author_id": 1, "category_id": 1})
    fresh_db.add_foreign_keys(
        [
            ("books", "author_id", "authors", "id"),
            ("books", "category_id", "categories", "id"),
        ]
    )
    fks = sorted(fresh_db["books"].foreign_keys)
    assert fks[0].column == "author_id"
    assert fks[1].column == "category_id"
