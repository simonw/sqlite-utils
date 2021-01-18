from sqlite_utils.db import ForeignKey, NoObviousTable
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


def test_insert_m2m_alter(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"id": 1, "name": "Cleo"}, pk="id").m2m(
        "humans", {"id": 1, "name": "Natalie D"}, pk="id"
    )
    dogs.update(1).m2m(
        "humans", {"id": 2, "name": "Simon W", "nerd": True}, pk="id", alter=True
    )
    assert list(fresh_db["humans"].rows) == [
        {"id": 1, "name": "Natalie D", "nerd": None},
        {"id": 2, "name": "Simon W", "nerd": 1},
    ]
    assert list(fresh_db["dogs_humans"].rows) == [
        {"humans_id": 1, "dogs_id": 1},
        {"humans_id": 2, "dogs_id": 1},
    ]


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


def test_insert_m2m_iterable(fresh_db):
    iterable_records = ({"id": 1, "name": "Phineas"}, {"id": 2, "name": "Ferb"})

    def iterable():
        for record in iterable_records:
            yield record

    platypuses = fresh_db["platypuses"]
    platypuses.insert({"id": 1, "name": "Perry"}, pk="id").m2m(
        "humans",
        iterable(),
        pk="id",
    )

    assert {"platypuses", "humans", "humans_platypuses"} == set(fresh_db.table_names())
    humans = fresh_db["humans"]
    humans_platypuses = fresh_db["humans_platypuses"]
    assert [
        {"humans_id": 1, "platypuses_id": 1},
        {"humans_id": 2, "platypuses_id": 1},
    ] == list(humans_platypuses.rows)
    assert [{"id": 1, "name": "Phineas"}, {"id": 2, "name": "Ferb"}] == list(
        humans.rows
    )
    assert [
        ForeignKey(
            table="humans_platypuses",
            column="platypuses_id",
            other_table="platypuses",
            other_column="id",
        ),
        ForeignKey(
            table="humans_platypuses",
            column="humans_id",
            other_table="humans",
            other_column="id",
        ),
    ] == humans_platypuses.foreign_keys


def test_m2m_with_table_objects(fresh_db):
    dogs = fresh_db.table("dogs", pk="id")
    humans = fresh_db.table("humans", pk="id")
    dogs.insert({"id": 1, "name": "Cleo"}).m2m(
        humans, [{"id": 1, "name": "Natalie D"}, {"id": 2, "name": "Simon W"}]
    )
    expected_tables = {"dogs", "humans", "dogs_humans"}
    assert expected_tables == set(fresh_db.table_names())
    assert 1 == dogs.count
    assert 2 == humans.count
    assert 2 == fresh_db["dogs_humans"].count


def test_m2m_lookup(fresh_db):
    people = fresh_db.table("people", pk="id")
    people.insert({"name": "Wahyu"}).m2m("tags", lookup={"tag": "Coworker"})
    people_tags = fresh_db["people_tags"]
    tags = fresh_db["tags"]
    assert people_tags.exists()
    assert tags.exists()
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
    assert not fresh_db["people_tags"].exists()


def test_m2m_table_candidates(fresh_db):
    fresh_db.create_table("one", {"id": int, "name": str}, pk="id")
    fresh_db.create_table("two", {"id": int, "name": str}, pk="id")
    fresh_db.create_table("three", {"id": int, "name": str}, pk="id")
    # No candidates at first
    assert [] == fresh_db.m2m_table_candidates("one", "two")
    # Create a candidate
    fresh_db.create_table(
        "one_m2m_two", {"one_id": int, "two_id": int}, foreign_keys=["one_id", "two_id"]
    )
    assert ["one_m2m_two"] == fresh_db.m2m_table_candidates("one", "two")
    # Add another table and there should be two candidates
    fresh_db.create_table(
        "one_m2m_two_and_three",
        {"one_id": int, "two_id": int, "three_id": int},
        foreign_keys=["one_id", "two_id", "three_id"],
    )
    assert {"one_m2m_two", "one_m2m_two_and_three"} == set(
        fresh_db.m2m_table_candidates("one", "two")
    )


def test_uses_existing_m2m_table_if_exists(fresh_db):
    # Code should look for an existing table with fks to both tables
    # and use that if it exists.
    people = fresh_db.create_table("people", {"id": int, "name": str}, pk="id")
    fresh_db["tags"].lookup({"tag": "Coworker"})
    fresh_db.create_table(
        "tagged",
        {"people_id": int, "tags_id": int},
        foreign_keys=["people_id", "tags_id"],
    )
    people.insert({"name": "Wahyu"}).m2m("tags", lookup={"tag": "Coworker"})
    assert fresh_db["tags"].exists()
    assert fresh_db["tagged"].exists()
    assert not fresh_db["people_tags"].exists()
    assert not fresh_db["tags_people"].exists()
    assert [{"people_id": 1, "tags_id": 1}] == list(fresh_db["tagged"].rows)


def test_requires_explicit_m2m_table_if_multiple_options(fresh_db):
    # If the code scans for m2m tables and finds more than one candidate
    # it should require that the m2m_table=x argument is used
    people = fresh_db.create_table("people", {"id": int, "name": str}, pk="id")
    fresh_db["tags"].lookup({"tag": "Coworker"})
    fresh_db.create_table(
        "tagged",
        {"people_id": int, "tags_id": int},
        foreign_keys=["people_id", "tags_id"],
    )
    fresh_db.create_table(
        "tagged2",
        {"people_id": int, "tags_id": int},
        foreign_keys=["people_id", "tags_id"],
    )
    with pytest.raises(NoObviousTable):
        people.insert({"name": "Wahyu"}).m2m("tags", lookup={"tag": "Coworker"})
