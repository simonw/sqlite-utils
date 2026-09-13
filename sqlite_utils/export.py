import csv
from typing import Any, Iterable, Optional, Sequence, TextIO, Union


CsvDialect = Union[str, csv.Dialect]


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
    writer = csv.writer(file, dialect=dialect, **writer_kwargs)
    if header:
        description: Optional[Sequence[Sequence[Any]]] = cursor.description
        if description is None:
            raise ValueError("Cursor does not have result columns")
        writer.writerow([column[0] for column in description])
    writer.writerows(_rows(cursor))


def _rows(cursor: Iterable[Sequence[Any]]) -> Iterable[Sequence[Any]]:
    """Keep row iteration lazy so large query results are streamed."""
    for row in cursor:
        yield row
