import sqlite3
from collections import namedtuple
import json

Column = namedtuple(
    "Column", ("cid", "name", "type", "notnull", "default_value", "is_pk")
)
ForeignKey = namedtuple(
    "ForeignKey", ("table", "column", "other_table", "other_column")
)


class Database:
    def __init__(self, filename_or_conn):
        if isinstance(filename_or_conn, str):
            self.conn = sqlite3.connect(filename_or_conn)
        else:
            self.conn = filename_or_conn

    def __getitem__(self, table_name):
        return Table(self, table_name)

    @property
    def tables(self):
        return [
            r[0]
            for r in self.conn.execute(
                "select name from sqlite_master where type = 'table'"
            ).fetchall()
        ]

    def create_table(self, name, columns, pk=None, foreign_keys=None):
        foreign_keys = foreign_keys or []
        foreign_keys_by_name = {fk[0]: fk for fk in foreign_keys}
        extra = ""
        columns_sql = ",\n".join(
            "   {col_name} {col_type} {primary_key} {references}".format(
                col_name=col_name,
                col_type={
                    float: "FLOAT",
                    int: "INTEGER",
                    bool: "INTEGER",
                    str: "TEXT",
                    None.__class__: "TEXT",
                }[col_type],
                primary_key=" PRIMARY KEY" if (pk == col_name) else "",
                references=(
                    " REFERENCES [{other_table}]([{other_column}])".format(
                        other_table=foreign_keys_by_name[col_name][2],
                        other_column=foreign_keys_by_name[col_name][3],
                    )
                    if col_name in foreign_keys_by_name
                    else ""
                ),
            )
            for col_name, col_type in columns.items()
        )
        sql = """CREATE TABLE {table} (
            {columns_sql}
        ){extra};
        """.format(
            table=name, columns_sql=columns_sql, extra=extra
        )
        self.conn.execute(sql)
        return self[name]


class Table:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.exists = self.name in self.db.tables

    @property
    def columns(self):
        if not self.exists:
            return []
        rows = self.db.conn.execute(
            "PRAGMA table_info([{}])".format(self.name)
        ).fetchall()
        return [Column(*row) for row in rows]

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

    def create(self, columns, pk=None, foreign_keys=None):
        columns = {name: value for (name, value) in columns.items()}
        self.db.create_table(self.name, columns, pk=pk, foreign_keys=foreign_keys)
        self.exists = True

    def drop(self):
        return self.db.conn.execute("DROP TABLE {}".format(self.name))

    def add_foreign_key(self, column, column_type, other_table, other_column):
        sql = """
            ALTER TABLE {table} ADD COLUMN {column} {column_type}
            REFERENCES {other_table}({other_column});
        """.format(
            table=self.name,
            column=column,
            column_type=column_type,
            other_table=other_table,
            other_column=other_column,
        )
        result = self.db.conn.execute(sql)
        self.db.conn.commit()
        return result

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
            else:
                t = str
            column_types[key] = t
        return column_types

    def insert(self, record, pk=None, foreign_keys=None, upsert=False):
        return self.insert_all(
            [record], pk=pk, foreign_keys=foreign_keys, upsert=upsert
        )

    def insert_all(
        self, records, pk=None, foreign_keys=None, upsert=False, batch_size=100
    ):
        """
        Like .insert() but takes a list of records and ensures that the table
        that it creates (if table does not exist) has columns for ALL of that
        data
        """
        if not self.exists:
            self.create(self.detect_column_types(records), pk, foreign_keys)
        all_columns = set()
        for record in records:
            all_columns.update(record.keys())
        all_columns = list(sorted(all_columns))
        for chunk in chunks(records, batch_size):
            sql = """
                INSERT {upsert} INTO {table} ({columns}) VALUES {rows};
            """.format(
                upsert="OR REPLACE" if upsert else "",
                table=self.name,
                columns=", ".join(all_columns),
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
                    jsonify_if_needed(record.get(key, None)) for key in all_columns
                )
            result = self.db.conn.execute(sql, values)
            self.db.conn.commit()
        return result

    def upsert(self, record, pk=None, foreign_keys=None):
        return self.insert(record, pk=pk, foreign_keys=foreign_keys, upsert=True)

    def upsert_all(self, records, pk=None, foreign_keys=None):
        return self.insert_all(records, pk=pk, foreign_keys=foreign_keys, upsert=True)


def chunks(sequence, size):
    for i in range(0, len(sequence), size):
        yield sequence[i : i + size]


def jsonify_if_needed(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value)
    else:
        return value
