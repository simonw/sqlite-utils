import csv
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

    writer = csv.writer(file, dialect=dialect, **writer_kwargs)
    if header:
        writer.writerow([column[0] for column in description])
    writer.writerows(cursor)
