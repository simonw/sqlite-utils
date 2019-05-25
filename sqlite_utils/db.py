import sqlite3
from collections import namedtuple
import datetime
import hashlib
import itertools
import json
import pathlib

try:
    import numpy as np
except ImportError:
    np = None

Column = namedtuple(
    "Column", ("cid", "name", "type", "notnull", "default_value", "is_pk")
)
ForeignKey = namedtuple(
    "ForeignKey", ("table", "column", "other_table", "other_column")
)
Index = namedtuple("Index", ("seq", "name", "unique", "origin", "partial", "columns"))

COLUMN_TYPE_MAPPING = {
    float: "FLOAT",
    int: "INTEGER",
    bool: "INTEGER",
    str: "TEXT",
    bytes.__class__: "BLOB",
    bytes: "BLOB",
    datetime.datetime: "TEXT",
    datetime.date: "TEXT",
    datetime.time: "TEXT",
    None.__class__: "TEXT",
    # SQLite explicit types
    "TEXT": "TEXT",
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
    "BLOB": "BLOB",
    "text": "TEXT",
    "integer": "INTEGER",
    "float": "FLOAT",
    "blob": "BLOB",
}
# If numpy is available, add more types
if np:
    COLUMN_TYPE_MAPPING.update(
        {
            np.int8: "INTEGER",
            np.int16: "INTEGER",
            np.int32: "INTEGER",
            np.int64: "INTEGER",
            np.uint8: "INTEGER",
            np.uint16: "INTEGER",
            np.uint32: "INTEGER",
            np.uint64: "INTEGER",
            np.float16: "FLOAT",
            np.float32: "FLOAT",
            np.float64: "FLOAT",
        }
    )


REVERSE_COLUMN_TYPE_MAPPING = {
    "TEXT": str,
    "BLOB": bytes,
    "INTEGER": int,
    "FLOAT": float,
}


class AlterError(Exception):
    pass


class Database:
    def __init__(self, filename_or_conn):
        if isinstance(filename_or_conn, str):
            self.conn = sqlite3.connect(filename_or_conn)
        elif isinstance(filename_or_conn, pathlib.Path):
            self.conn = sqlite3.connect(str(filename_or_conn))
        else:
            self.conn = filename_or_conn

    def __getitem__(self, table_name):
        return Table(self, table_name)

    def __repr__(self):
        return "<Database {}>".format(self.conn)

    def table_names(self, fts4=False, fts5=False):
        where = ["type = 'table'"]
        if fts4:
            where.append("sql like '%FTS4%'")
        if fts5:
            where.append("sql like '%FTS5%'")
        sql = "select name from sqlite_master where {}".format(" AND ".join(where))
        return [r[0] for r in self.conn.execute(sql).fetchall()]

    @property
    def tables(self):
        return [self[name] for name in self.table_names()]

    def execute_returning_dicts(self, sql, params=None):
        cursor = self.conn.execute(sql, params or tuple())
        keys = [d[0] for d in cursor.description]
        return [dict(zip(keys, row)) for row in cursor.fetchall()]

    def create_table(
        self, name, columns, pk=None, foreign_keys=None, column_order=None, hash_id=None
    ):
        foreign_keys = foreign_keys or []
        foreign_keys_by_name = {fk[0]: fk for fk in foreign_keys}
        column_items = list(columns.items())
        if column_order is not None:
            column_items.sort(
                key=lambda p: column_order.index(p[0]) if p[0] in column_order else 999
            )
        if hash_id:
            column_items.insert(0, (hash_id, str))
            pk = hash_id
        # Sanity check foreign_keys point to existing tables
        for _, fk_other_table, fk_other_column in foreign_keys:
            if not any(
                c for c in self[fk_other_table].columns if c.name == fk_other_column
            ):
                raise AlterError(
                    "No such column: {}.{}".format(fk_other_table, fk_other_column)
                )
        extra = ""
        columns_sql = ",\n".join(
            "   [{col_name}] {col_type}{primary_key}{references}".format(
                col_name=col_name,
                col_type=COLUMN_TYPE_MAPPING[col_type],
                primary_key=" PRIMARY KEY" if (pk == col_name) else "",
                references=(
                    " REFERENCES [{other_table}]([{other_column}])".format(
                        other_table=foreign_keys_by_name[col_name][1],
                        other_column=foreign_keys_by_name[col_name][2],
                    )
                    if col_name in foreign_keys_by_name
                    else ""
                ),
            )
            for col_name, col_type in column_items
        )
        sql = """CREATE TABLE [{table}] (
{columns_sql}
){extra};
        """.format(
            table=name, columns_sql=columns_sql, extra=extra
        )
        self.conn.execute(sql)
        return self[name]

    def create_view(self, name, sql):
        self.conn.execute(
            """
            CREATE VIEW {name} AS {sql}
        """.format(
                name=name, sql=sql
            )
        )

    def vacuum(self):
        self.conn.execute("VACUUM;")


class Table:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.exists = self.name in self.db.table_names()

    def __repr__(self):
        return "<Table {}{}>".format(
            self.name, " (does not exist yet)" if not self.exists else ""
        )

    @property
    def count(self):
        return self.db.conn.execute(
            "select count(*) from [{}]".format(self.name)
        ).fetchone()[0]

    @property
    def columns(self):
        if not self.exists:
            return []
        rows = self.db.conn.execute(
            "PRAGMA table_info([{}])".format(self.name)
        ).fetchall()
        return [Column(*row) for row in rows]

    @property
    def columns_dict(self):
        "Returns {column: python-type} dictionary"
        return {
            column.name: REVERSE_COLUMN_TYPE_MAPPING[column.type]
            for column in self.columns
        }

    @property
    def rows(self):
        if not self.exists:
            return []
        cursor = self.db.conn.execute("select * from [{}]".format(self.name))
        columns = [c[0] for c in cursor.description]
        for row in cursor:
            yield dict(zip(columns, row))

    @property
    def pks(self):
        return [column.name for column in self.columns if column.is_pk]

    @property
    def foreign_keys(self):
        fks = []
        for row in self.db.conn.execute(
            "PRAGMA foreign_key_list([{}])".format(self.name)
        ).fetchall():
            if row is not None:
                id, seq, table_name, from_, to_, on_update, on_delete, match = row
                fks.append(
                    ForeignKey(
                        table=self.name,
                        column=from_,
                        other_table=table_name,
                        other_column=to_,
                    )
                )
        return fks

    @property
    def schema(self):
        return self.db.conn.execute(
            "select sql from sqlite_master where name = ?", (self.name,)
        ).fetchone()[0]

    @property
    def indexes(self):
        sql = 'PRAGMA index_list("{}")'.format(self.name)
        indexes = []
        for row in self.db.execute_returning_dicts(sql):
            index_name = row["name"]
            index_name_quoted = (
                '"{}"'.format(index_name)
                if not index_name.startswith('"')
                else index_name
            )
            column_sql = "PRAGMA index_info({})".format(index_name_quoted)
            columns = []
            for seqno, cid, name in self.db.conn.execute(column_sql).fetchall():
                columns.append(name)
            row["columns"] = columns
            # These columns may be missing on older SQLite versions:
            for key, default in {"origin": "c", "partial": 0}.items():
                if key not in row:
                    row[key] = default
            indexes.append(Index(**row))
        return indexes

    def create(
        self, columns, pk=None, foreign_keys=None, column_order=None, hash_id=None
    ):
        columns = {name: value for (name, value) in columns.items()}
        with self.db.conn:
            self.db.create_table(
                self.name,
                columns,
                pk=pk,
                foreign_keys=foreign_keys,
                column_order=column_order,
                hash_id=hash_id,
            )
        self.exists = True
        return self

    def create_index(self, columns, index_name=None, unique=False, if_not_exists=False):
        if index_name is None:
            index_name = "idx_{}_{}".format(
                self.name.replace(" ", "_"), "_".join(columns)
            )
        sql = """
            CREATE {unique}INDEX {if_not_exists}{index_name}
                ON {table_name} ({columns});
        """.format(
            index_name=index_name,
            table_name=self.name,
            columns=", ".join(columns),
            unique="UNIQUE " if unique else "",
            if_not_exists="IF NOT EXISTS " if if_not_exists else "",
        )
        self.db.conn.execute(sql)
        return self

    def add_column(self, col_name, col_type=None):
        if col_type is None:
            col_type = str
        sql = "ALTER TABLE [{table}] ADD COLUMN [{col_name}] {col_type};".format(
            table=self.name, col_name=col_name, col_type=COLUMN_TYPE_MAPPING[col_type]
        )
        self.db.conn.execute(sql)
        return self

    def drop(self):
        return self.db.conn.execute("DROP TABLE {}".format(self.name))

    def add_foreign_key(self, column, other_table, other_column):
        # Sanity check that the other column exists
        if not [c for c in self.db[other_table].columns if c.name == other_column]:
            raise AlterError("No such column: {}.{}".format(other_table, other_column))
        # Check we do not already have an existing foreign key
        if any(
            fk
            for fk in self.foreign_keys
            if fk.column == column
            and fk.other_table == other_table
            and fk.other_column == other_column
        ):
            raise AlterError(
                "Foreign key already exists for {} => {}.{}".format(
                    column, other_table, other_column
                )
            )
        with self.db.conn:
            cursor = self.db.conn.cursor()
            schema_version = cursor.execute("PRAGMA schema_version").fetchone()[0]
            cursor.execute("PRAGMA writable_schema = 1")
            old_sql = self.schema
            extra_sql = ",\n   FOREIGN KEY({column}) REFERENCES {other_table}({other_column})\n".format(
                column=column, other_table=other_table, other_column=other_column
            )
            # Stick that bit in at the very end just before the closing ')'
            last_paren = old_sql.rindex(")")
            new_sql = old_sql[:last_paren].strip() + extra_sql + old_sql[last_paren:]
            cursor.execute(
                "UPDATE sqlite_master SET sql = ? WHERE name = ?", (new_sql, self.name)
            )
            cursor.execute("PRAGMA schema_version = %d" % (schema_version + 1))
            cursor.execute("PRAGMA writable_schema = 0")
        # Have to VACUUM outside the transaction to ensure .foreign_keys property
        # can see the newly created foreign key.
        cursor.execute("VACUUM")

    def enable_fts(self, columns, fts_version="FTS5"):
        "Enables FTS on the specified columns"
        sql = """
            CREATE VIRTUAL TABLE "{table}_fts" USING {fts_version} (
                {columns},
                content="{table}"
            );
        """.format(
            table=self.name,
            columns=", ".join("[{}]".format(c) for c in columns),
            fts_version=fts_version,
        )
        self.db.conn.executescript(sql)
        self.populate_fts(columns)
        return self

    def populate_fts(self, columns):
        sql = """
            INSERT INTO "{table}_fts" (rowid, {columns})
                SELECT rowid, {columns} FROM {table};
        """.format(
            table=self.name, columns=", ".join(columns)
        )
        self.db.conn.executescript(sql)
        return self

    def detect_fts(self):
        "Detect if table has a corresponding FTS virtual table and return it"
        rows = self.db.conn.execute(
            """
            SELECT name FROM sqlite_master
                WHERE rootpage = 0
                AND (
                    sql LIKE '%VIRTUAL TABLE%USING FTS%content="{table}"%'
                    OR (
                        tbl_name = "{table}"
                        AND sql LIKE '%VIRTUAL TABLE%USING FTS%'
                    )
                )
        """.format(
                table=self.name
            )
        ).fetchall()
        if len(rows) == 0:
            return None
        else:
            return rows[0][0]

    def optimize(self):
        fts_table = self.detect_fts()
        if fts_table is not None:
            self.db.conn.execute(
                """
                INSERT INTO [{table}] ([{table}]) VALUES ("optimize");
            """.format(
                    table=fts_table
                )
            )
        return self

    def detect_column_types(self, records):
        all_column_types = {}
        for record in records:
            for key, value in record.items():
                all_column_types.setdefault(key, set()).add(type(value))
        column_types = {}
        for key, types in all_column_types.items():
            if len(types) == 1:
                t = list(types)[0]
                # But if it's list / tuple / dict, use str instead as we
                # will be storing it as JSON in the table
                if t in (list, tuple, dict):
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

    def search(self, q):
        sql = """
            select * from {table} where rowid in (
                select rowid from [{table}_fts]
                where [{table}_fts] match :search
            )
            order by rowid
        """.format(
            table=self.name
        )
        return self.db.conn.execute(sql, (q,)).fetchall()

    def insert(
        self,
        record,
        pk=None,
        foreign_keys=None,
        upsert=False,
        column_order=None,
        hash_id=None,
        alter=False,
    ):
        return self.insert_all(
            [record],
            pk=pk,
            foreign_keys=foreign_keys,
            upsert=upsert,
            column_order=column_order,
            hash_id=hash_id,
            alter=alter,
        )

    def insert_all(
        self,
        records,
        pk=None,
        foreign_keys=None,
        upsert=False,
        batch_size=100,
        column_order=None,
        hash_id=None,
        alter=False,
    ):
        """
        Like .insert() but takes a list of records and ensures that the table
        that it creates (if table does not exist) has columns for ALL of that
        data
        """
        assert not (hash_id and pk), "Use either pk= or hash_id="
        all_columns = None
        first = True
        for chunk in chunks(records, batch_size):
            chunk = list(chunk)
            if first:
                if not self.exists:
                    # Use the first batch to derive the table names
                    self.create(
                        self.detect_column_types(chunk),
                        pk,
                        foreign_keys,
                        column_order=column_order,
                        hash_id=hash_id,
                    )
                all_columns = set()
                for record in chunk:
                    all_columns.update(record.keys())
                all_columns = list(sorted(all_columns))
                if hash_id:
                    all_columns.insert(0, hash_id)
            first = False
            sql = """
                INSERT {upsert} INTO [{table}] ({columns}) VALUES {rows};
            """.format(
                upsert="OR REPLACE" if upsert else "",
                table=self.name,
                columns=", ".join("[{}]".format(c) for c in all_columns),
                rows=", ".join(
                    """
                    ({placeholders})
                """.format(
                        placeholders=", ".join(["?"] * len(all_columns))
                    )
                    for record in chunk
                ),
            )
            values = []
            for record in chunk:
                values.extend(
                    jsonify_if_needed(
                        record.get(key, None if key != hash_id else _hash(record))
                    )
                    for key in all_columns
                )
            with self.db.conn:
                try:
                    result = self.db.conn.execute(sql, values)
                except sqlite3.OperationalError as e:
                    if alter and (" has no column " in e.args[0]):
                        # Attempt to add any missing columns, then try again
                        self.add_missing_columns(chunk)
                        result = self.db.conn.execute(sql, values)
                    else:
                        raise
                self.last_rowid = result.lastrowid
                self.last_pk = None
                if hash_id or pk:
                    self.last_pk = self.db.conn.execute(
                        "select [{}] from [{}] where rowid = ?".format(
                            hash_id or pk, self.name
                        ),
                        (self.last_rowid,),
                    ).fetchone()[0]
        return self

    def upsert(
        self,
        record,
        pk=None,
        foreign_keys=None,
        column_order=None,
        hash_id=None,
        alter=False,
    ):
        return self.insert(
            record,
            pk=pk,
            foreign_keys=foreign_keys,
            upsert=True,
            column_order=column_order,
            hash_id=hash_id,
            alter=alter,
        )

    def upsert_all(
        self,
        records,
        pk=None,
        foreign_keys=None,
        column_order=None,
        batch_size=100,
        hash_id=None,
        alter=False,
    ):
        return self.insert_all(
            records,
            pk=pk,
            foreign_keys=foreign_keys,
            column_order=column_order,
            batch_size=100,
            upsert=True,
            hash_id=hash_id,
            alter=alter,
        )

    def add_missing_columns(self, records):
        needed_columns = self.detect_column_types(records)
        current_columns = self.columns_dict
        for col_name, col_type in needed_columns.items():
            if col_name not in current_columns:
                self.add_column(col_name, col_type)


def chunks(sequence, size):
    iterator = iter(sequence)
    for item in iterator:
        yield itertools.chain([item], itertools.islice(iterator, size - 1))


def jsonify_if_needed(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value)
    else:
        return value


def _hash(record):
    return hashlib.sha1(
        json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf8")
    ).hexdigest()
