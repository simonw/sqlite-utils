from collections.abc import Iterable
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
        transactional: bool = True

    @dataclass
    class _AppliedMigration:
        name: str
        # A string timestamp such as "2026-07-04 12:00:00.000000+00:00" -
        # stored as TEXT in the _sqlite_migrations table
        applied_at: str

    def __init__(self, name: str):
        """
        :param name: The name of the migration set. This should be unique.
        """
        self.name = name
        self._migrations: list[Migrations._Migration] = []

    def __call__(
        self, *, name: str | None = None, transactional: bool = True
    ) -> Callable:
        """
        :param name: The name to use for this migration - if not provided,
          the name of the function will be used.
        :param transactional: If ``True`` (the default) the migration and the
          record of it having been applied are wrapped in a transaction, which
          will be rolled back if the migration raises an exception. Pass
          ``False`` for migrations that cannot run inside a transaction, for
          example those that execute ``VACUUM``.
        """

        def inner(func: Callable) -> Callable:
            migration_name = name or getattr(func, "__name__")
            if any(m.name == migration_name for m in self._migrations):
                raise ValueError(
                    "Migration '{}' is already registered in set '{}'".format(
                        migration_name, self.name
                    )
                )
            self._migrations.append(
                self._Migration(migration_name, func, transactional)
            )
            return func

        return inner

    def pending(self, db: "Database") -> list["Migrations._Migration"]:
        """
        Return a list of pending migrations.

        This is a read-only operation - it does not write to the database.
        """
        already_applied = {migration.name for migration in self.applied(db)}
        return [
            migration
            for migration in self._migrations
            if migration.name not in already_applied
        ]

    def applied(self, db: "Database") -> list["Migrations._AppliedMigration"]:
        """
        Return a list of applied migrations, in the order they were applied.

        This is a read-only operation - it does not write to the database.
        """
        table = _table(db, self.migrations_table)
        if not table.exists():
            return []
        return [
            self._AppliedMigration(name=row["name"], applied_at=row["applied_at"])
            for row in table.rows_where(
                "migration_set = ?", [self.name], order_by="rowid"
            )
        ]

    def apply(self, db: "Database", *, stop_before: str | Iterable[str] | None = None):
        """
        Apply any pending migrations to the database.

        Each migration runs inside a transaction, together with the record of
        it having been applied - if the migration raises an exception its
        changes are rolled back, no record is written and the migration stays
        pending. Migrations registered with ``transactional=False`` run
        outside of a transaction.

        :raises ValueError: if a ``stop_before`` name matches a migration in
          this set that has already been applied - stopping before it is
          impossible to honor, and no pending migrations are applied
        """
        if stop_before is None:
            stop_before_names = set()
        elif isinstance(stop_before, str):
            stop_before_names = {stop_before}
        else:
            stop_before_names = set(stop_before)
        # A stop_before naming an already-applied migration cannot be
        # honored - error rather than applying everything after it. Names
        # not in this set at all are ignored, because unqualified CLI
        # values are offered to every migration set
        already_applied = stop_before_names.intersection(
            migration.name for migration in self.applied(db)
        )
        if already_applied:
            raise ValueError(
                "Cannot stop before migration{} {} in set '{}' - already "
                "been applied".format(
                    "s" if len(already_applied) > 1 else "",
                    ", ".join(sorted(already_applied)),
                    self.name,
                )
            )
        self.ensure_migrations_table(db)
        for migration in self.pending(db):
            name = migration.name
            if name in stop_before_names:
                return
            if migration.transactional:
                with db.atomic():
                    migration.fn(db)
                    self._record_applied(db, name)
            else:
                migration.fn(db)
                self._record_applied(db, name)

    def _record_applied(self, db: "Database", name: str):
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
