import pytest
from sqlite_utils.utils import column_affinity

EXAMPLES = [
    # Examples from https://www.sqlite.org/datatype3.html#affinity_name_examples
    ("INT", int),
    ("INTEGER", int),
    ("TINYINT", int),
    ("SMALLINT", int),
    ("MEDIUMINT", int),
    ("BIGINT", int),
    ("UNSIGNED BIG INT", int),
    ("INT2", int),
    ("INT8", int),
    ("CHARACTER(20)", str),
    ("VARCHAR(255)", str),
    ("VARYING CHARACTER(255)", str),
    ("NCHAR(55)", str),
    ("NATIVE CHARACTER(70)", str),
    ("NVARCHAR(100)", str),
    ("TEXT", str),
    ("CLOB", str),
    ("BLOB", bytes),
    ("REAL", float),
    ("DOUBLE", float),
    ("DOUBLE PRECISION", float),
    ("FLOAT", float),
    # Numeric, treated as float:
    ("NUMERIC", float),
    ("DECIMAL(10,5)", float),
    ("BOOLEAN", float),
    ("DATE", float),
    ("DATETIME", float),
]


@pytest.mark.parametrize("column_def,expected_type", EXAMPLES)
def test_column_affinity(column_def, expected_type):
    assert expected_type is column_affinity(column_def)


@pytest.mark.parametrize("column_def,expected_type", EXAMPLES)
def test_columns_dict(fresh_db, column_def, expected_type):
    fresh_db.execute("create table foo (col {})".format(column_def))
    assert {"col": expected_type} == fresh_db["foo"].columns_dict
