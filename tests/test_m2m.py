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
