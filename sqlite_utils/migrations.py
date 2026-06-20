from dataclasses import dataclass
import datetime
from typing import Callable, cast, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlite_utils.db import Database, Table


class Migrations:
    migrations_table = "_sqlite_migrations"

    @dataclass
    class _Migration:
        name: str
        fn: Callable

    @dataclass
    class _AppliedMigration:
        name: str
        applied_at: datetime.datetime

    def __init__(self, name: str):
        """
        :param name: The name of the migration set. This should be unique.
        """
        self.name = name
        self._migrations: list[Migrations._Migration] = []

    def __call__(self, *, name: str | None = None) -> Callable:
        """
        :param name: The name to use for this migration - if not provided,
          the name of the function will be used.
        """

        def inner(func: Callable) -> Callable:
            self._migrations.append(
                self._Migration(name or getattr(func, "__name__"), func)
            )
            return func

        return inner

    def pending(self, db: "Database") -> list["Migrations._Migration"]:
        """
        Return a list of pending migrations.
        """
        self.ensure_migrations_table(db)
        already_applied = {
            r["name"]
            for r in db[self.migrations_table].rows_where(
                "migration_set = ?", [self.name]
            )
        }
        return [
            migration
            for migration in self._migrations
            if migration.name not in already_applied
        ]

    def applied(self, db: "Database") -> list["Migrations._AppliedMigration"]:
        """
        Return a list of applied migrations.
        """
        self.ensure_migrations_table(db)
        return [
            self._AppliedMigration(name=row["name"], applied_at=row["applied_at"])
            for row in db[self.migrations_table].rows_where(
                "migration_set = ?", [self.name]
            )
        ]

    def apply(self, db: "Database", *, stop_before: str | None = None):
        """
        Apply any pending migrations to the database.
        """
        self.ensure_migrations_table(db)
        for migration in self.pending(db):
            name = migration.name
            if name == stop_before:
                return
            migration.fn(db)
            _table(db, self.migrations_table).insert(
                {
                    "migration_set": self.name,
                    "name": name,
                    "applied_at": str(datetime.datetime.now(datetime.timezone.utc)),
                }
            )

    def ensure_migrations_table(self, db: "Database"):
        """
        Ensure the _sqlite_migrations table exists and has the correct schema.
        """
        table = _table(db, self.migrations_table)
        if not table.exists():
            table.create(
                {
                    "id": int,
                    "migration_set": str,
                    "name": str,
                    "applied_at": str,
                },
                pk="id",
            )
            table.create_index(["migration_set", "name"], unique=True)
        elif table.pks != ["id"]:
            table.transform(pk="id")
            unique_indexes = {tuple(index.columns) for index in table.indexes}
            if ("migration_set", "name") not in unique_indexes:
                table.create_index(["migration_set", "name"], unique=True)

    def __repr__(self):
        return "<Migrations '{}': [{}]>".format(
            self.name, ", ".join(m.name for m in self._migrations)
        )


def _table(db: "Database", name: str) -> "Table":
    return cast("Table", db[name])
