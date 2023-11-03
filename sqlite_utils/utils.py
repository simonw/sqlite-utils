import base64
import contextlib
import csv
import enum
import hashlib
import io
import itertools
import json
import os
import sys
from . import recipes
from typing import Dict, cast, BinaryIO, Iterable, Optional, Tuple, Type

import click

try:
    import pysqlite3 as sqlite3  # noqa: F401
    from pysqlite3 import dbapi2  # noqa: F401

    OperationalError = dbapi2.OperationalError
except ImportError:
    try:
        import sqlean as sqlite3  # noqa: F401
        from sqlean import dbapi2  # noqa: F401

        OperationalError = dbapi2.OperationalError
    except ImportError:
        import sqlite3  # noqa: F401
        from sqlite3 import dbapi2  # noqa: F401

        OperationalError = dbapi2.OperationalError


SPATIALITE_PATHS = (
    "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
    "/usr/lib/aarch64-linux-gnu/mod_spatialite.so",
    "/usr/local/lib/mod_spatialite.dylib",
    "/usr/local/lib/mod_spatialite.so",
    "/opt/homebrew/lib/mod_spatialite.dylib",
)

# Mainly so we can restore it if needed in the tests:
ORIGINAL_CSV_FIELD_SIZE_LIMIT = csv.field_size_limit()


def maximize_csv_field_size_limit():
    """
    Increase the CSV field size limit to the maximum possible.
    """
    # https://stackoverflow.com/a/15063941
    field_size_limit = sys.maxsize

    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)


def find_spatialite() -> Optional[str]:
    """
    The ``find_spatialite()`` function searches for the `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__
    SQLite extension in some common places. It returns a string path to the location, or ``None`` if SpatiaLite was not found.

    You can use it in code like this:

    .. code-block:: python

        from sqlite_utils import Database
        from sqlite_utils.utils import find_spatialite

        db = Database("mydb.db")
        spatialite = find_spatialite()
        if spatialite:
            db.conn.enable_load_extension(True)
            db.conn.load_extension(spatialite)

        # or use with db.init_spatialite like this
        db.init_spatialite(find_spatialite())

    """
    for path in SPATIALITE_PATHS:
        if os.path.exists(path):
            return path
    return None


def suggest_column_types(records):
    all_column_types = {}
    for record in records:
        for key, value in record.items():
            all_column_types.setdefault(key, set()).add(type(value))
    return types_for_column_types(all_column_types)


def types_for_column_types(all_column_types):
    column_types = {}
    for key, types in all_column_types.items():
        # Ignore null values if at least one other type present:
        if len(types) > 1:
            types.discard(None.__class__)
        if {None.__class__} == types:
            t = str
        elif len(types) == 1:
            t = list(types)[0]
            # But if it's a subclass of list / tuple / dict, use str
            # instead as we will be storing it as JSON in the table
            for superclass in (list, tuple, dict):
                if issubclass(t, superclass):
                    t = str
        elif {int, bool}.issuperset(types):
            t = int
        elif {int, float, bool}.issuperset(types):
            t = float
        elif {bytes, str}.issuperset(types):
            t = bytes
        else:
            t = str
        column_types[key] = t
    return column_types


def column_affinity(column_type):
    # Implementation of SQLite affinity rules from
    # https://www.sqlite.org/datatype3.html#determination_of_column_affinity
    assert isinstance(column_type, str)
    column_type = column_type.upper().strip()
    if column_type == "":
        return str  # We differ from spec, which says it should be BLOB
    if "INT" in column_type:
        return int
    if "CHAR" in column_type or "CLOB" in column_type or "TEXT" in column_type:
        return str
    if "BLOB" in column_type:
        return bytes
    if "REAL" in column_type or "FLOA" in column_type or "DOUB" in column_type:
        return float
    # Default is 'NUMERIC', which we currently also treat as float
    return float


def decode_base64_values(doc):
    # Looks for '{"$base64": true..., "encoded": ...}' values and decodes them
    to_fix = [
        k
        for k in doc
        if isinstance(doc[k], dict)
        and doc[k].get("$base64") is True
        and "encoded" in doc[k]
    ]
    if not to_fix:
        return doc
    return dict(doc, **{k: base64.b64decode(doc[k]["encoded"]) for k in to_fix})


class UpdateWrapper:
    def __init__(self, wrapped, update):
        self._wrapped = wrapped
        self._update = update

    def __iter__(self):
        for line in self._wrapped:
            self._update(len(line))
            yield line

    def read(self, size=-1):
        data = self._wrapped.read(size)
        self._update(len(data))
        return data


@contextlib.contextmanager
def file_progress(file, silent=False, **kwargs):
    if silent:
        yield file
        return
    # file.fileno() throws an exception in our test suite
    try:
        fileno = file.fileno()
    except io.UnsupportedOperation:
        yield file
        return
    if fileno == 0:  # 0 means stdin
        yield file
    else:
        file_length = os.path.getsize(file.name)
        with click.progressbar(length=file_length, **kwargs) as bar:
            yield UpdateWrapper(file, bar.update)


class Format(enum.Enum):
    CSV = 1
    TSV = 2
    JSON = 3
    NL = 4


class RowsFromFileError(Exception):
    pass


class RowsFromFileBadJSON(RowsFromFileError):
    pass


class RowError(Exception):
    pass


def _extra_key_strategy(
    reader: Iterable[dict],
    ignore_extras: Optional[bool] = False,
    extras_key: Optional[str] = None,
) -> Iterable[dict]:
    # Logic for handling CSV rows with more values than there are headings
    for row in reader:
        # DictReader adds a 'None' key with extra row values
        if None not in row:
            yield row
        elif ignore_extras:
            # ignoring row.pop(none) because of this issue:
            # https://github.com/simonw/sqlite-utils/issues/440#issuecomment-1155358637
            row.pop(None)  # type: ignore
            yield row
        elif not extras_key:
            extras = row.pop(None)  # type: ignore
            raise RowError(
                "Row {} contained these extra values: {}".format(row, extras)
            )
        else:
            row[extras_key] = row.pop(None)  # type: ignore
            yield row


def rows_from_file(
    fp: BinaryIO,
    format: Optional[Format] = None,
    dialect: Optional[Type[csv.Dialect]] = None,
    encoding: Optional[str] = None,
    ignore_extras: Optional[bool] = False,
    extras_key: Optional[str] = None,
) -> Tuple[Iterable[dict], Format]:
    """
    Load a sequence of dictionaries from a file-like object containing one of four different formats.

    .. code-block:: python

        from sqlite_utils.utils import rows_from_file
        import io

        rows, format = rows_from_file(io.StringIO("id,name\\n1,Cleo")))
        print(list(rows), format)
        # Outputs [{'id': '1', 'name': 'Cleo'}] Format.CSV

    This defaults to attempting to automatically detect the format of the data, or you can pass in an
    explicit format using the format= option.

    Returns a tuple of ``(rows_generator, format_used)`` where ``rows_generator`` can be iterated over
    to return dictionaries, while ``format_used`` is a value from the ``sqlite_utils.utils.Format`` enum:

    .. code-block:: python

        class Format(enum.Enum):
            CSV = 1
            TSV = 2
            JSON = 3
            NL = 4

    If a CSV or TSV file includes rows with more fields than are declared in the header a
    ``sqlite_utils.utils.RowError`` exception will be raised when you loop over the generator.

    You can instead ignore the extra data by passing ``ignore_extras=True``.

    Or pass ``extras_key="rest"`` to put those additional values in a list in a key called ``rest``.

    :param fp: a file-like object containing binary data
    :param format: the format to use - omit this to detect the format
    :param dialect: the CSV dialect to use - omit this to detect the dialect
    :param encoding: the character encoding to use when reading CSV/TSV data
    :param ignore_extras: ignore any extra fields on rows
    :param extras_key: put any extra fields in a list with this key
    """
    if ignore_extras and extras_key:
        raise ValueError("Cannot use ignore_extras= and extras_key= together")
    if format == Format.JSON:
        decoded = json.load(fp)
        if isinstance(decoded, dict):
            decoded = [decoded]
        if not isinstance(decoded, list):
            raise RowsFromFileBadJSON("JSON must be a list or a dictionary")
        return decoded, Format.JSON
    elif format == Format.NL:
        return (json.loads(line) for line in fp if line.strip()), Format.NL
    elif format == Format.CSV:
        use_encoding: str = encoding or "utf-8-sig"
        decoded_fp = io.TextIOWrapper(fp, encoding=use_encoding)
        if dialect is not None:
            reader = csv.DictReader(decoded_fp, dialect=dialect)
        else:
            reader = csv.DictReader(decoded_fp)
        return _extra_key_strategy(reader, ignore_extras, extras_key), Format.CSV
    elif format == Format.TSV:
        rows = rows_from_file(
            fp, format=Format.CSV, dialect=csv.excel_tab, encoding=encoding
        )[0]
        return (
            _extra_key_strategy(rows, ignore_extras, extras_key),
            Format.TSV,
        )
    elif format is None:
        # Detect the format, then call this recursively
        buffered = io.BufferedReader(cast(io.RawIOBase, fp), buffer_size=4096)
        try:
            first_bytes = buffered.peek(2048).strip()
        except AttributeError:
            # Likely the user passed a TextIO when this needs a BytesIO
            raise TypeError(
                "rows_from_file() requires a file-like object that supports peek(), such as io.BytesIO"
            )
        if first_bytes.startswith(b"[") or first_bytes.startswith(b"{"):
            # TODO: Detect newline-JSON
            return rows_from_file(buffered, format=Format.JSON)
        else:
            dialect = csv.Sniffer().sniff(
                first_bytes.decode(encoding or "utf-8-sig", "ignore")
            )
            rows, _ = rows_from_file(
                buffered, format=Format.CSV, dialect=dialect, encoding=encoding
            )
            # Make sure we return the format we detected
            format = Format.TSV if dialect.delimiter == "\t" else Format.CSV
            return _extra_key_strategy(rows, ignore_extras, extras_key), format
    else:
        raise RowsFromFileError("Bad format")


class TypeTracker:
    """
    Wrap an iterator of dictionaries and keep track of which SQLite column
    types are the most likely fit for each of their keys.

    Example usage:

    .. code-block:: python

        from sqlite_utils.utils import TypeTracker
        import sqlite_utils

        db = sqlite_utils.Database(memory=True)
        tracker = TypeTracker()
        rows = [{"id": "1", "name": "Cleo", "id": "2", "name": "Cardi"}]
        db["creatures"].insert_all(tracker.wrap(rows))
        print(tracker.types)
        # Outputs {'id': 'integer', 'name': 'text'}
        db["creatures"].transform(types=tracker.types)
    """

    def __init__(self):
        self.trackers = {}

    def wrap(self, iterator: Iterable[dict]) -> Iterable[dict]:
        """
        Use this to loop through an existing iterator, tracking the column types
        as part of the iteration.

        :param iterator: The iterator to wrap
        """
        for row in iterator:
            for key, value in row.items():
                tracker = self.trackers.setdefault(key, ValueTracker())
                tracker.evaluate(value)
            yield row

    @property
    def types(self) -> Dict[str, str]:
        """
        A dictionary mapping column names to their detected types. This can be passed
        to the ``db[table_name].transform(types=tracker.types)`` method.
        """
        return {key: tracker.guessed_type for key, tracker in self.trackers.items()}


class ValueTracker:
    def __init__(self):
        self.couldbe = {key: getattr(self, "test_" + key) for key in self.get_tests()}

    @classmethod
    def get_tests(cls):
        return [
            key.split("test_")[-1]
            for key in cls.__dict__.keys()
            if key.startswith("test_")
        ]

    def test_integer(self, value):
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    def test_float(self, value):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def __repr__(self) -> str:
        return self.guessed_type + ": possibilities = " + repr(self.couldbe)

    @property
    def guessed_type(self):
        options = set(self.couldbe.keys())
        # Return based on precedence
        for key in self.get_tests():
            if key in options:
                return key
        return "text"

    def evaluate(self, value):
        if not value or not self.couldbe:
            return
        not_these = []
        for name, test in self.couldbe.items():
            if not test(value):
                not_these.append(name)
        for key in not_these:
            del self.couldbe[key]


class NullProgressBar:
    def __init__(self, *args):
        self.args = args

    def __iter__(self):
        yield from self.args[0]

    def update(self, value):
        pass


@contextlib.contextmanager
def progressbar(*args, **kwargs):
    silent = kwargs.pop("silent")
    if silent:
        yield NullProgressBar(*args)
    else:
        with click.progressbar(*args, **kwargs) as bar:
            yield bar


def _compile_code(code, imports, variable="value"):
    globals = {"r": recipes, "recipes": recipes}
    # If user defined a convert() function, return that
    try:
        exec(code, globals)
        return globals["convert"]
    except (AttributeError, SyntaxError, NameError, KeyError, TypeError):
        pass

    # Try compiling their code as a function instead
    body_variants = [code]
    # If single line and no 'return', try adding the return
    if "\n" not in code and not code.strip().startswith("return "):
        body_variants.insert(0, "return {}".format(code))

    code_o = None
    for variant in body_variants:
        new_code = ["def fn({}):".format(variable)]
        for line in variant.split("\n"):
            new_code.append("    {}".format(line))
        try:
            code_o = compile("\n".join(new_code), "<string>", "exec")
            break
        except SyntaxError:
            # Try another variant, e.g. for 'return row["column"] = 1'
            continue

    if code_o is None:
        raise SyntaxError("Could not compile code")

    for import_ in imports:
        globals[import_.split(".")[0]] = __import__(import_)
    exec(code_o, globals)
    return globals["fn"]


def chunks(sequence: Iterable, size: int) -> Iterable[Iterable]:
    """
    Iterate over chunks of the sequence of the given size.

    :param sequence: Any Python iterator
    :param size: The size of each chunk
    """
    iterator = iter(sequence)
    for item in iterator:
        yield itertools.chain([item], itertools.islice(iterator, size - 1))


def hash_record(record: Dict, keys: Optional[Iterable[str]] = None):
    """
    ``record`` should be a Python dictionary. Returns a sha1 hash of the
    keys and values in that record.

    If ``keys=`` is provided, uses just those keys to generate the hash.

    Example usage::

        from sqlite_utils.utils import hash_record

        hashed = hash_record({"name": "Cleo", "twitter": "CleoPaws"})
        # Or with the keys= option:
        hashed = hash_record(
            {"name": "Cleo", "twitter": "CleoPaws", "age": 7},
            keys=("name", "twitter")
        )

    :param record: Record to generate a hash for
    :param keys: Subset of keys to use for that hash
    """
    to_hash = record
    if keys is not None:
        to_hash = {key: record[key] for key in keys}
    return hashlib.sha1(
        json.dumps(to_hash, separators=(",", ":"), sort_keys=True, default=repr).encode(
            "utf8"
        )
    ).hexdigest()


def _flatten(d):
    for key, value in d.items():
        if isinstance(value, dict):
            for key2, value2 in _flatten(value):
                yield key + "_" + key2, value2
        else:
            yield key, value


def flatten(row: dict) -> dict:
    """
    Turn a nested dict e.g. ``{"a": {"b": 1}}`` into a flat dict: ``{"a_b": 1}``

    :param row: A Python dictionary, optionally with nested dictionaries
    """
    return dict(_flatten(row))
