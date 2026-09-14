import csv
import itertools
from collections.abc import Mapping
from typing import Any, Sequence, TextIO, Type, Union

CsvDialect = Union[str, csv.Dialect, Type[csv.Dialect]]


def rows_to_csv_file(
    cursor: Any,
    file: TextIO,
    *,
    header: bool = True,
    dialect: CsvDialect = "excel",
    **writer_kwargs: Any,
) -> None:
    """Write the rows from a DB-API cursor to a CSV-compatible text stream.

    ``cursor`` should be a DB-API cursor whose ``description`` attribute
    contains the result column metadata. Pass ``dialect="excel-tab"`` to
    produce TSV output, or provide any dialect accepted by ``csv.writer``.

    Additional keyword arguments are forwarded to ``csv.writer``.
    """
    description: Union[Sequence[Sequence[Any]], None] = cursor.description
    if description is None:
        raise ValueError("Cursor does not have result columns")

    columns = [column[0] for column in description]
    row_iterator = iter(cursor)
    sentinel = object()
    first_row = next(row_iterator, sentinel)
    if isinstance(first_row, Mapping) and len(set(columns)) != len(columns):
        raise ValueError("Mapping rows cannot represent duplicate column names")

    writer = csv.writer(file, dialect=dialect, **writer_kwargs)
    if header:
        writer.writerow(columns)

    def normalize_row(row):
        if isinstance(row, Mapping):
            return [row[column] for column in columns]
        return row

    if first_row is not sentinel:
        writer.writerows(
            normalize_row(row) for row in itertools.chain((first_row,), row_iterator)
        )
