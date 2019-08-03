from sqlite_utils.db import ForeignKey
import pytest


def test_insert_m2m_single(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo"}, pk="id").m2m(
        "humans", {"id": 1, "name": "Natalie D"}, pk="id"
    )
    assert {"dogs_humans", "humans", "dogs"} == set(fresh_db.table_names())
    humans = fresh_db["humans"]
    dogs_humans = fresh_db["dogs_humans"]
    assert [{"id": 1, "name": "Natalie D"}] == list(humans.rows)
    assert [{"humans_id": 1, "dogs_id": 1}] == list(dogs_humans.rows)


def test_insert_m2m_list(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo"}, pk="id").m2m(
        "humans",
        [{"id": 1, "name": "Natalie D"}, {"id": 2, "name": "Simon W"}],
        pk="id",
    )
    assert {"dogs", "humans", "dogs_humans"} == set(fresh_db.table_names())
    humans = fresh_db["humans"]
    dogs_humans = fresh_db["dogs_humans"]
    assert [{"humans_id": 1, "dogs_id": 1}, {"humans_id": 2, "dogs_id": 1}] == list(
        dogs_humans.rows
    )
    assert [{"id": 1, "name": "Natalie D"}, {"id": 2, "name": "Simon W"}] == list(
        humans.rows
    )
    assert [
        ForeignKey(
            table="dogs_humans", column="dogs_id", other_table="dogs", other_column="id"
        ),
        ForeignKey(
            table="dogs_humans",
            column="humans_id",
            other_table="humans",
            other_column="id",
        ),
    ] == dogs_humans.foreign_keys


def test_m2m_lookup(fresh_db):
    people = fresh_db.table("people", pk="id")
    people.insert({"name": "Wahyu"}).m2m("tags", lookup={"tag": "Coworker"})
    people_tags = fresh_db["people_tags"]
    tags = fresh_db["tags"]
    assert people_tags.exists
    assert tags.exists
    assert [
        ForeignKey(
            table="people_tags",
            column="people_id",
            other_table="people",
            other_column="id",
        ),
        ForeignKey(
            table="people_tags", column="tags_id", other_table="tags", other_column="id"
        ),
    ] == people_tags.foreign_keys
    assert [{"people_id": 1, "tags_id": 1}] == list(people_tags.rows)
    assert [{"id": 1, "name": "Wahyu"}] == list(people.rows)
    assert [{"id": 1, "tag": "Coworker"}] == list(tags.rows)


def test_m2m_requires_either_records_or_lookup(fresh_db):
    people = fresh_db.table("people", pk="id").insert({"name": "Wahyu"})
    with pytest.raises(AssertionError):
        people.m2m("tags")
    with pytest.raises(AssertionError):
        people.m2m("tags", {"tag": "hello"}, lookup={"foo": "bar"})


def test_m2m_explicit_table_name_argument(fresh_db):
    people = fresh_db.table("people", pk="id")
    people.insert({"name": "Wahyu"}).m2m(
        "tags", lookup={"tag": "Coworker"}, m2m_table="tagged"
    )
    assert fresh_db["tags"].exists
    assert fresh_db["tagged"].exists
    assert not fresh_db["people_tags"].exists


def test_uses_existing_m2m_table_if_exists(fresh_db):
    # Code should look for an existing toble with fks to both tables
    # and use that if it exists.
    assert False


def test_requires_explicit_m2m_table_if_multiple_options(fresh_db):
    # If the code scans for m2m tables and finds more than one candidate
    # it should require that the m2m_table=x argument is used
    assert False
