"""Tests for compound (multi-column) foreign keys - issue #594."""

import pytest
from sqlite_utils import Database
from sqlite_utils.db import AlterError, ForeignKey
from sqlite_utils.utils import sqlite3

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
    assert fk.columns == ("campus_name", "dept_code")
    assert fk.other_columns == ("campus_name", "dept_code")
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
    assert fk.columns == ("author_id",)
    assert fk.other_columns == ("id",)


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


def test_mixed_compound_and_single_foreign_keys_are_sortable():
    # compound FKs have column=None, which must not break sorting
    # against single-column FKs (None < str raises TypeError)
    db = Database(memory=True)
    db.executescript("""
    CREATE TABLE departments (
        campus_name TEXT NOT NULL,
        dept_code TEXT NOT NULL,
        PRIMARY KEY (campus_name, dept_code)
    );
    CREATE TABLE accreditations (id INTEGER PRIMARY KEY);
    CREATE TABLE courses (
        course_code TEXT PRIMARY KEY,
        campus_name TEXT NOT NULL,
        dept_code TEXT NOT NULL,
        accreditation_id INTEGER REFERENCES accreditations(id),
        FOREIGN KEY (campus_name, dept_code)
        REFERENCES departments(campus_name, dept_code)
    );
    """)
    fks = db["courses"].foreign_keys
    assert len(fks) == 2
    assert {fk.is_compound for fk in fks} == {True, False}
    fks_sorted = sorted(fks)
    assert fks_sorted[0].other_table == "accreditations"
    assert fks_sorted[1].other_table == "departments"


@pytest.fixture
def departments_db():
    db = Database(memory=True)
    db.create_table(
        "departments",
        {"campus_name": str, "dept_code": str, "dept_name": str},
        pk=("campus_name", "dept_code"),
    )
    return db


EXPECTED_COURSES_SCHEMA = (
    'CREATE TABLE "courses" (\n'
    '   "course_code" TEXT PRIMARY KEY,\n'
    '   "campus_name" TEXT,\n'
    '   "dept_code" TEXT,\n'
    '   FOREIGN KEY ("campus_name", "dept_code") '
    'REFERENCES "departments"("campus_name", "dept_code")\n'
    ")"
)


@pytest.mark.parametrize(
    "foreign_keys",
    (
        [
            ForeignKey(
                table="courses",
                column=None,
                other_table="departments",
                other_column=None,
                columns=("campus_name", "dept_code"),
                other_columns=("campus_name", "dept_code"),
                is_compound=True,
            )
        ],
        [(("campus_name", "dept_code"), "departments", ("campus_name", "dept_code"))],
        # Two-item form guesses the other table's primary key:
        [(("campus_name", "dept_code"), "departments")],
        # Lists work too, though tuples are the documented form:
        [(["campus_name", "dept_code"], "departments", ["campus_name", "dept_code"])],
    ),
)
def test_create_table_with_compound_foreign_key(departments_db, foreign_keys):
    departments_db.create_table(
        "courses",
        {"course_code": str, "campus_name": str, "dept_code": str},
        pk="course_code",
        foreign_keys=foreign_keys,
    )
    assert departments_db["courses"].schema == EXPECTED_COURSES_SCHEMA
    fks = departments_db["courses"].foreign_keys
    assert len(fks) == 1
    fk = fks[0]
    assert fk.is_compound is True
    assert fk.columns == ("campus_name", "dept_code")
    assert fk.other_table == "departments"
    assert fk.other_columns == ("campus_name", "dept_code")


def test_create_table_compound_foreign_key_enforced(departments_db):
    departments_db.execute("PRAGMA foreign_keys = ON")
    departments_db.create_table(
        "courses",
        {"course_code": str, "campus_name": str, "dept_code": str},
        pk="course_code",
        foreign_keys=[(("campus_name", "dept_code"), "departments")],
    )
    departments_db["departments"].insert(
        {"campus_name": "Berkeley", "dept_code": "CS", "dept_name": "Computer Science"}
    )
    departments_db["courses"].insert(
        {"course_code": "CS101", "campus_name": "Berkeley", "dept_code": "CS"}
    )
    with pytest.raises(sqlite3.IntegrityError):
        departments_db.execute(
            "insert into courses (course_code, campus_name, dept_code) "
            "values ('X1', 'Nowhere', 'NOPE')"
        )


def test_create_table_compound_foreign_key_missing_other_column(departments_db):
    with pytest.raises(AlterError):
        departments_db.create_table(
            "courses",
            {"course_code": str, "campus_name": str, "dept_code": str},
            pk="course_code",
            foreign_keys=[
                (("campus_name", "dept_code"), "departments", ("campus_name", "nope"))
            ],
        )


def test_transform_preserves_compound_foreign_key(compound_db):
    compound_db["courses"].transform(rename={"course_name": "title"})
    fks = compound_db["courses"].foreign_keys
    assert len(fks) == 1
    fk = fks[0]
    assert fk.is_compound is True
    assert fk.columns == ("campus_name", "dept_code")
    assert fk.other_table == "departments"
    assert fk.other_columns == ("campus_name", "dept_code")


def test_transform_rename_member_column_updates_compound_foreign_key(compound_db):
    compound_db["courses"].transform(rename={"campus_name": "campus"})
    fks = compound_db["courses"].foreign_keys
    assert len(fks) == 1
    fk = fks[0]
    assert fk.is_compound is True
    assert fk.columns == ("campus", "dept_code")
    # Referenced columns in the other table are unchanged
    assert fk.other_columns == ("campus_name", "dept_code")


def test_transform_drop_member_column_drops_compound_foreign_key(compound_db):
    # Matches single-column behavior: dropping the column silently
    # drops the foreign key that used it
    compound_db["courses"].transform(drop={"dept_code"})
    assert compound_db["courses"].foreign_keys == []
    assert "FOREIGN KEY" not in compound_db["courses"].schema


@pytest.mark.parametrize(
    "drop_foreign_keys",
    (
        # A bare column name matches any foreign key it participates in:
        ["campus_name"],
        # A tuple must match the full compound key:
        [("campus_name", "dept_code")],
    ),
)
def test_transform_drop_compound_foreign_key(compound_db, drop_foreign_keys):
    compound_db["courses"].transform(drop_foreign_keys=drop_foreign_keys)
    assert compound_db["courses"].foreign_keys == []
    # The columns themselves survive
    assert {"campus_name", "dept_code"} <= set(
        compound_db["courses"].columns_dict.keys()
    )


@pytest.fixture
def courses_db(departments_db):
    departments_db.create_table(
        "courses",
        {"course_code": str, "campus_name": str, "dept_code": str},
        pk="course_code",
    )
    return departments_db


def test_add_compound_foreign_key(courses_db):
    t = courses_db["courses"].add_foreign_key(
        ("campus_name", "dept_code"), "departments", ("campus_name", "dept_code")
    )
    # Returns self
    assert t.name == "courses"
    fks = courses_db["courses"].foreign_keys
    assert len(fks) == 1
    fk = fks[0]
    assert fk.is_compound is True
    assert fk.columns == ("campus_name", "dept_code")
    assert fk.other_table == "departments"
    assert fk.other_columns == ("campus_name", "dept_code")


def test_add_compound_foreign_key_guesses_other_columns(courses_db):
    # Lists work here too, though tuples are the documented form
    courses_db["courses"].add_foreign_key(["campus_name", "dept_code"], "departments")
    fk = courses_db["courses"].foreign_keys[0]
    assert fk.other_columns == ("campus_name", "dept_code")


def test_add_compound_foreign_key_error_if_already_exists(courses_db):
    courses_db["courses"].add_foreign_key(("campus_name", "dept_code"), "departments")
    with pytest.raises(AlterError) as ex:
        courses_db["courses"].add_foreign_key(
            ("campus_name", "dept_code"), "departments"
        )
    assert "already exists" in ex.value.args[0]
    # ignore=True should not raise
    courses_db["courses"].add_foreign_key(
        ("campus_name", "dept_code"), "departments", ignore=True
    )


def test_add_compound_foreign_key_error_if_column_missing(courses_db):
    with pytest.raises(AlterError):
        courses_db["courses"].add_foreign_key(("campus_name", "nope"), "departments")


def test_db_add_foreign_keys_compound(courses_db):
    courses_db.add_foreign_keys(
        [
            (
                "courses",
                ("campus_name", "dept_code"),
                "departments",
                ("campus_name", "dept_code"),
            )
        ]
    )
    fk = courses_db["courses"].foreign_keys[0]
    assert fk.is_compound is True
    assert fk.columns == ("campus_name", "dept_code")


def test_index_foreign_keys_compound_creates_composite_index(compound_db):
    compound_db.index_foreign_keys()
    index_columns = [i.columns for i in compound_db["courses"].indexes]
    assert ["campus_name", "dept_code"] in index_columns
    # No separate single-column indexes for the members
    assert ["campus_name"] not in index_columns
    assert ["dept_code"] not in index_columns


def test_foreign_key_captures_on_delete_and_on_update():
    db = Database(memory=True)
    db.executescript("""
    CREATE TABLE authors (id INTEGER PRIMARY KEY);
    CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        author_id INTEGER REFERENCES authors(id)
            ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """)
    fk = db["books"].foreign_keys[0]
    assert fk.on_delete == "CASCADE"
    assert fk.on_update == "RESTRICT"


def test_foreign_key_on_delete_defaults_to_no_action(fresh_db):
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["books"].insert({"id": 1, "author_id": 1}, pk="id")
    fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    fk = fresh_db["books"].foreign_keys[0]
    assert fk.on_delete == "NO ACTION"
    assert fk.on_update == "NO ACTION"


def test_create_table_foreign_key_with_on_delete(fresh_db):
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db.create_table(
        "books",
        {"id": int, "author_id": int},
        pk="id",
        foreign_keys=[
            ForeignKey(
                table="books",
                column="author_id",
                other_table="authors",
                other_column="id",
                on_delete="CASCADE",
            )
        ],
    )
    assert "ON DELETE CASCADE" in fresh_db["books"].schema
    assert fresh_db["books"].foreign_keys[0].on_delete == "CASCADE"


def test_transform_preserves_on_delete_cascade():
    db = Database(memory=True)
    db.executescript("""
    CREATE TABLE authors (id INTEGER PRIMARY KEY);
    CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        title TEXT,
        author_id INTEGER REFERENCES authors(id) ON DELETE CASCADE
    );
    """)
    db["books"].transform(rename={"title": "book_title"})
    fk = db["books"].foreign_keys[0]
    assert fk.on_delete == "CASCADE"
    assert fk.on_update == "NO ACTION"
    assert "ON DELETE CASCADE" in db["books"].schema


def test_transform_preserves_compound_foreign_key_on_delete():
    db = Database(memory=True)
    db.executescript("""
    CREATE TABLE departments (
        campus_name TEXT NOT NULL,
        dept_code TEXT NOT NULL,
        PRIMARY KEY (campus_name, dept_code)
    );
    CREATE TABLE courses (
        course_code TEXT PRIMARY KEY,
        campus_name TEXT NOT NULL,
        dept_code TEXT NOT NULL,
        FOREIGN KEY (campus_name, dept_code)
        REFERENCES departments(campus_name, dept_code) ON DELETE CASCADE
    );
    """)
    db["courses"].transform(rename={"course_code": "code"})
    fk = db["courses"].foreign_keys[0]
    assert fk.is_compound is True
    assert fk.on_delete == "CASCADE"
    assert "ON DELETE CASCADE" in db["courses"].schema


def test_implicit_primary_key_reference_is_resolved():
    # REFERENCES authors (no column) has "to" of None in the pragma -
    # it should be resolved to the primary key of the other table
    db = Database(memory=True)
    db.executescript("""
    CREATE TABLE authors (author_id INTEGER PRIMARY KEY);
    CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        author_id INTEGER REFERENCES authors
    );
    """)
    fk = db["books"].foreign_keys[0]
    assert fk.is_compound is False
    assert fk.other_column == "author_id"
    assert fk.other_columns == ("author_id",)


def test_implicit_compound_primary_key_reference_is_resolved():
    db = Database(memory=True)
    db.executescript("""
    CREATE TABLE departments (
        campus_name TEXT NOT NULL,
        dept_code TEXT NOT NULL,
        PRIMARY KEY (campus_name, dept_code)
    );
    CREATE TABLE courses (
        course_code TEXT PRIMARY KEY,
        campus_name TEXT NOT NULL,
        dept_code TEXT NOT NULL,
        FOREIGN KEY (campus_name, dept_code) REFERENCES departments
    );
    """)
    fk = db["courses"].foreign_keys[0]
    assert fk.is_compound is True
    assert fk.other_columns == ("campus_name", "dept_code")


def test_foreign_key_normalizes_list_columns_to_tuples():
    # Compound columns passed as lists are normalized to tuples, so they
    # compare equal to introspected ForeignKeys
    fk = ForeignKey(
        table="courses",
        column=None,
        other_table="departments",
        other_column=None,
        columns=["campus_name", "dept_code"],
        other_columns=["campus_name", "dept_code"],
        is_compound=True,
    )
    assert fk.columns == ("campus_name", "dept_code")
    assert fk.other_columns == ("campus_name", "dept_code")


def test_add_foreign_keys_preserves_actions(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/594 review finding:
    # ForeignKey objects passed to db.add_foreign_keys() were flattened
    # to plain tuples, losing on_delete/on_update
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["books"].insert({"id": 1, "author_id": 1}, pk="id")
    fresh_db.add_foreign_keys(
        [ForeignKey("books", "author_id", "authors", "id", on_delete="CASCADE")]
    )
    fk = fresh_db["books"].foreign_keys[0]
    assert fk.on_delete == "CASCADE"
    assert "ON DELETE CASCADE" in fresh_db["books"].schema


def test_add_foreign_keys_preserves_actions_compound(courses_db):
    courses_db.add_foreign_keys(
        [
            ForeignKey(
                table="courses",
                column=None,
                other_table="departments",
                other_column=None,
                columns=("campus_name", "dept_code"),
                other_columns=("campus_name", "dept_code"),
                is_compound=True,
                on_delete="CASCADE",
            )
        ]
    )
    fk = courses_db["courses"].foreign_keys[0]
    assert fk.is_compound is True
    assert fk.on_delete == "CASCADE"
    assert "ON DELETE CASCADE" in courses_db["courses"].schema


def test_add_foreign_key_on_delete_on_update(fresh_db):
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["books"].insert({"id": 1, "author_id": 1}, pk="id")
    fresh_db["books"].add_foreign_key(
        "author_id", "authors", "id", on_delete="CASCADE", on_update="RESTRICT"
    )
    fk = fresh_db["books"].foreign_keys[0]
    assert fk.on_delete == "CASCADE"
    assert fk.on_update == "RESTRICT"
    assert "ON UPDATE RESTRICT ON DELETE CASCADE" in fresh_db["books"].schema
    # The cascade should actually fire
    fresh_db.execute("PRAGMA foreign_keys = ON")
    fresh_db.execute("delete from authors where id = 1")
    assert fresh_db["books"].count == 0


def test_add_compound_foreign_key_on_delete(courses_db):
    courses_db["courses"].add_foreign_key(
        ("campus_name", "dept_code"), "departments", on_delete="SET NULL"
    )
    fk = courses_db["courses"].foreign_keys[0]
    assert fk.is_compound is True
    assert fk.on_delete == "SET NULL"
    assert "ON DELETE SET NULL" in courses_db["courses"].schema


def test_implicit_compound_foreign_key_resolves_pk_declaration_order(fresh_db):
    # The other table's PRIMARY KEY declares its columns in a different
    # order to the table's column order. SQLite resolves the implicit
    # "REFERENCES other" using PRIMARY KEY declaration order, so the
    # introspected other_columns must too
    fresh_db.execute("create table other (b text, a text, primary key (a, b))")
    fresh_db.execute(
        "create table child (x text, y text, foreign key (x, y) references other)"
    )
    fk = fresh_db["child"].foreign_keys[0]
    assert fk.other_columns == ("a", "b")


def test_transform_implicit_compound_foreign_key_stays_valid(fresh_db):
    # transform() rewrites the implicit FK with explicit columns - they
    # must be in PRIMARY KEY declaration order or valid data fails the
    # foreign key check with an IntegrityError
    fresh_db.execute("create table other (b text, a text, primary key (a, b))")
    fresh_db.execute(
        "create table child (x text, y text, foreign key (x, y) references other)"
    )
    fresh_db.execute("PRAGMA foreign_keys = ON")
    fresh_db["other"].insert({"a": "A", "b": "B"})
    fresh_db["child"].insert({"x": "A", "y": "B"})
    fresh_db["child"].transform(types={"x": str})
    assert fresh_db["child"].foreign_keys[0].other_columns == ("a", "b")
    # The constraint still points the right way around
    fresh_db["child"].insert({"x": "A", "y": "B"})
    with pytest.raises(sqlite3.IntegrityError):
        fresh_db["child"].insert({"x": "B", "y": "A"})


def test_create_compound_foreign_key_guesses_pk_declaration_order(fresh_db):
    fresh_db.execute("create table other (b text, a text, primary key (a, b))")
    fresh_db["other"].insert({"a": "A", "b": "B"})
    fresh_db["child"].create(
        {"id": int, "x": str, "y": str},
        pk="id",
        foreign_keys=[(("x", "y"), "other")],
    )
    assert fresh_db["child"].foreign_keys[0].other_columns == ("a", "b")
    fresh_db.execute("PRAGMA foreign_keys = ON")
    fresh_db["child"].insert({"id": 1, "x": "A", "y": "B"})
    with pytest.raises(sqlite3.IntegrityError):
        fresh_db["child"].insert({"id": 2, "x": "B", "y": "A"})


def test_add_compound_foreign_key_guesses_pk_declaration_order(fresh_db):
    fresh_db.execute("create table other (b text, a text, primary key (a, b))")
    fresh_db["child"].insert({"id": 1, "x": "A", "y": "B"}, pk="id")
    fresh_db["child"].add_foreign_key(("x", "y"), "other")
    assert fresh_db["child"].foreign_keys[0].other_columns == ("a", "b")


def test_foreign_keys_are_hashable(fresh_db):
    # set() over foreign_keys worked with the 3.x namedtuple and must
    # keep working with the dataclass
    fresh_db["p"].insert({"id": 1}, pk="id")
    fresh_db["c"].insert(
        {"id": 1, "pid": 1}, pk="id", foreign_keys=[("pid", "p", "id")]
    )
    fks = set(fresh_db["c"].foreign_keys)
    assert len(fks) == 1
    assert ForeignKey("c", "pid", "p", "id") in fks
    # Usable as dict keys too
    assert {fk: True for fk in fks}


def test_foreign_key_is_immutable():
    import dataclasses

    fk = ForeignKey("c", "pid", "p", "id")
    with pytest.raises(dataclasses.FrozenInstanceError):
        fk.table = "other"


def test_foreign_key_equality_and_hash_include_actions():
    # Two foreign keys differing only in ON DELETE behavior are different
    # constraints - they compare unequal and hash separately
    plain = ForeignKey("c", "pid", "p", "id")
    cascade = ForeignKey("c", "pid", "p", "id", on_delete="CASCADE")
    assert plain != cascade
    assert len({plain, cascade}) == 2
    assert plain == ForeignKey("c", "pid", "p", "id")


def test_create_table_mixed_foreign_keys_list(fresh_db):
    # 3.x accepted a mix of ForeignKey objects, tuples and bare column
    # strings in foreign_keys= (ForeignKey was a namedtuple, so it passed
    # the tuple check) - keep accepting the mix
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["publishers"].insert({"id": 1}, pk="id")
    fresh_db["books"].create(
        {"id": int, "author_id": int, "publisher_id": int},
        pk="id",
        foreign_keys=[
            ForeignKey("books", "author_id", "authors", "id"),
            ("publisher_id", "publishers", "id"),
        ],
    )
    fks = {fk.column: fk.other_table for fk in fresh_db["books"].foreign_keys}
    assert fks == {"author_id": "authors", "publisher_id": "publishers"}


def test_create_table_mixed_foreign_keys_with_string(fresh_db):
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["publishers"].insert({"id": 1}, pk="id")
    fresh_db["books"].create(
        {"id": int, "author_id": int, "publisher_id": int},
        pk="id",
        foreign_keys=[
            "author_id",  # bare column, table and column guessed
            ("publisher_id", "publishers", "id"),
        ],
    )
    fks = {fk.column: fk.other_table for fk in fresh_db["books"].foreign_keys}
    assert fks == {"author_id": "authors", "publisher_id": "publishers"}


def test_add_foreign_keys_existing_with_different_actions_errors(fresh_db):
    # Requesting an existing foreign key with different ON DELETE/ON UPDATE
    # actions was silently skipped, dropping the requested change
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["books"].insert(
        {"id": 1, "author_id": 1},
        pk="id",
        foreign_keys=[("author_id", "authors", "id")],
    )
    with pytest.raises(AlterError) as ex:
        fresh_db.add_foreign_keys(
            [ForeignKey("books", "author_id", "authors", "id", on_delete="CASCADE")]
        )
    assert "ON DELETE" in str(ex.value)
    assert fresh_db["books"].foreign_keys[0].on_delete == "NO ACTION"


def test_add_foreign_keys_identical_existing_is_noop(fresh_db):
    # An exact match, including actions, is silently skipped so repeated
    # calls stay idempotent
    fresh_db["authors"].insert({"id": 1}, pk="id")
    fresh_db["books"].insert({"id": 1, "author_id": 1}, pk="id")
    fresh_db["books"].add_foreign_key("author_id", "authors", "id", on_delete="CASCADE")
    fresh_db.add_foreign_keys(
        [ForeignKey("books", "author_id", "authors", "id", on_delete="CASCADE")]
    )
    fks = fresh_db["books"].foreign_keys
    assert len(fks) == 1
    assert fks[0].on_delete == "CASCADE"


def test_add_foreign_keys_compound_column_count_mismatch_errors(fresh_db):
    # Previously the extra other-column was silently discarded, creating
    # a single-column foreign key to just ("id")
    fresh_db["departments"].insert(
        {"campus": "north", "code": "cs"}, pk=("campus", "code")
    )
    fresh_db["courses"].insert({"id": 1, "campus": "north"}, pk="id")
    with pytest.raises(ValueError) as ex:
        fresh_db.add_foreign_keys(
            [("courses", ("campus",), "departments", ("campus", "code"))]
        )
    assert "same number of columns" in str(ex.value)
    assert fresh_db["courses"].foreign_keys == []
