import pathlib

from click.testing import CliRunner
import pytest
import sqlite_utils
import sqlite_utils.cli

TWO_MIGRATIONS = """
from sqlite_utils import Migrations

m = Migrations("hello")

@m()
def foo(db):
    db["foo"].insert({"hello": "world"})

@m()
def bar(db):
    db["bar"].insert({"hello": "world"})
"""


@pytest.fixture
def two_migrations(tmpdir):
    path = pathlib.Path(tmpdir)
    (path / "foo").mkdir()
    migrations_py = path / "foo" / "migrations.py"
    migrations_py.write_text(TWO_MIGRATIONS, "utf-8")
    return path, migrations_py


@pytest.fixture
def two_sets_same_migration_name(tmpdir):
    path = pathlib.Path(tmpdir)
    migrations_py = path / "migrations.py"
    migrations_py.write_text(
        """
from sqlite_utils import Migrations

creatures = Migrations("creatures")

@creatures()
def create_table(db):
    db["creatures"].insert({"name": "Cleo"})

@creatures()
def add_weight(db):
    db["creature_weights"].insert({"weight": 4.2})

sales = Migrations("sales")

@sales()
def create_table(db):
    db["sales"].insert({"id": 1})

@sales()
def add_weight(db):
    db["sales_weights"].insert({"weight": 10})
""",
        "utf-8",
    )
    return path, migrations_py


@pytest.mark.parametrize("arg", ("TMPDIR", "TMPDIR/foo/migrations.py", "TMPDIR/foo/"))
def test_basic(two_migrations, arg):
    path, _ = two_migrations
    db_path = str(path / "test.db")

    runner = CliRunner()

    def _list():
        list_result = runner.invoke(
            sqlite_utils.cli.cli,
            ["migrate", db_path, "--list", arg.replace("TMPDIR", str(path))],
        )
        assert list_result.exit_code == 0
        return list_result.output

    assert _list() == (
        "Migrations for: hello\n\n"
        "  Applied:\n\n"
        "  Pending:\n"
        "    foo\n"
        "    bar\n\n"
    )

    result = runner.invoke(
        sqlite_utils.cli.cli, ["migrate", db_path, arg.replace("TMPDIR", str(path))]
    )
    assert result.exit_code == 0, result.output

    list_output = _list()
    assert "Migrations for: hello\n\n  Applied:\n    " in list_output
    prior_to_pending = list_output.split(" Pending")[0]
    assert "  foo" in prior_to_pending
    assert "  bar" in prior_to_pending
    assert " Pending:\n    (none)" in list_output

    db = sqlite_utils.Database(db_path)
    assert db["foo"].exists()
    assert db["bar"].exists()
    assert db["_sqlite_migrations"].exists()
    rows = list(db["_sqlite_migrations"].rows)
    assert len(rows) == 2
    assert rows[0]["name"] == "foo"
    assert rows[1]["name"] == "bar"


def test_list_same_migration_names_in_different_sets(capsys):
    applied = sqlite_utils.Migrations("applied")

    @applied(name="foo")
    def applied_foo(db):
        db["applied"].insert({"hello": "world"})

    pending = sqlite_utils.Migrations("pending")

    @pending(name="foo")
    def pending_foo(db):
        db["pending"].insert({"hello": "world"})

    db = sqlite_utils.Database(memory=True)
    applied.apply(db)

    sqlite_utils.cli._display_migration_list(db, [applied, pending])

    output = capsys.readouterr().out
    assert (
        "Migrations for: pending\n\n" "  Applied:\n\n" "  Pending:\n" "    foo\n\n"
    ) in output


def test_verbose(tmpdir):
    path = pathlib.Path(tmpdir)
    (path / "foo").mkdir()
    migrations_py = path / "foo" / "migrations.py"
    migrations_py.write_text(
        """
from sqlite_utils import Migrations

m = Migrations("hello")

@m()
def foo(db):
    db["dogs"].insert({"id": 1, "name": "Cleo"})
    """,
        "utf-8",
    )
    db_path = str(path / "test.db")
    runner = CliRunner()
    result = runner.invoke(
        sqlite_utils.cli.cli, ["migrate", db_path, str(migrations_py)]
    )
    assert result.exit_code == 0

    result = runner.invoke(
        sqlite_utils.cli.cli, ["migrate", db_path, str(migrations_py), "--verbose"]
    )
    assert result.exit_code == 0
    expected = """
Schema before:

  CREATE TABLE "_sqlite_migrations" (
     "id" INTEGER PRIMARY KEY,
     "migration_set" TEXT,
     "name" TEXT,
     "applied_at" TEXT
  );
  CREATE UNIQUE INDEX "idx__sqlite_migrations_migration_set_name"
      ON "_sqlite_migrations" ("migration_set", "name");
  CREATE TABLE "dogs" (
     "id" INTEGER,
     "name" TEXT
  );

Schema after:

  (unchanged)
""".strip()
    assert expected in result.output

    new_migration = """
@m()
def bar(db):
    db["dogs"].add_column("age", int)
    db["dogs"].add_column("weight", float)
    db["dogs"].transform()
"""
    migrations_py.write_text(migrations_py.read_text("utf-8") + new_migration)

    result = runner.invoke(
        sqlite_utils.cli.cli, ["migrate", db_path, str(migrations_py), "--verbose"]
    )
    assert result.exit_code == 0
    expected_diff = """
Schema diff:

     ON "_sqlite_migrations" ("migration_set", "name");
 CREATE TABLE "dogs" (
    "id" INTEGER,
-   "name" TEXT
+   "name" TEXT,
+   "age" INTEGER,
+   "weight" REAL
 );
""".strip()
    assert expected_diff in result.output


def test_stop_before(two_migrations):
    path, _ = two_migrations
    db_path = str(path / "test.db")
    result = CliRunner().invoke(
        sqlite_utils.cli.cli,
        [
            "migrate",
            db_path,
            str(path / "foo" / "migrations.py"),
            "--stop-before",
            "bar",
        ],
    )
    assert result.exit_code == 0
    db = sqlite_utils.Database(db_path)
    assert db["foo"].exists()
    assert not db["bar"].exists()


def test_stop_before_multiple_sets_unqualified(two_migrations):
    path, _ = two_migrations
    db_path = str(path / "test.db")
    (path / "foo" / "migrations2.py").write_text(
        """
from sqlite_utils import Migrations

m = Migrations("hello2")

@m()
def foo(db):
    db["foo"].insert({"hello": "world"})
    """,
        "utf-8",
    )
    result = CliRunner().invoke(
        sqlite_utils.cli.cli,
        [
            "migrate",
            db_path,
            str(path / "foo" / "migrations.py"),
            str(path / "foo" / "migrations2.py"),
            "--stop-before",
            "foo",
        ],
    )
    assert result.exit_code == 0, result.output
    db = sqlite_utils.Database(db_path)
    assert db.table_names() == ["_sqlite_migrations"]
    assert list(db["_sqlite_migrations"].rows) == []


def test_stop_before_qualified_only_affects_named_set(two_sets_same_migration_name):
    path, migrations_py = two_sets_same_migration_name
    db_path = str(path / "test.db")
    result = CliRunner().invoke(
        sqlite_utils.cli.cli,
        [
            "migrate",
            db_path,
            str(migrations_py),
            "--stop-before",
            "creatures:add_weight",
        ],
    )
    assert result.exit_code == 0, result.output
    db = sqlite_utils.Database(db_path)
    assert db["creatures"].exists()
    assert not db["creature_weights"].exists()
    assert db["sales"].exists()
    assert db["sales_weights"].exists()


def test_stop_before_multiple_qualified(two_sets_same_migration_name):
    path, migrations_py = two_sets_same_migration_name
    db_path = str(path / "test.db")
    result = CliRunner().invoke(
        sqlite_utils.cli.cli,
        [
            "migrate",
            db_path,
            str(migrations_py),
            "--stop-before",
            "creatures:add_weight",
            "--stop-before",
            "sales:add_weight",
        ],
    )
    assert result.exit_code == 0, result.output
    db = sqlite_utils.Database(db_path)
    assert db["creatures"].exists()
    assert not db["creature_weights"].exists()
    assert db["sales"].exists()
    assert not db["sales_weights"].exists()


LEGACY_MIGRATIONS = """
import datetime

class _Migration:
    def __init__(self, name, fn):
        self.name = name
        self.fn = fn

class _Applied:
    def __init__(self, name, applied_at):
        self.name = name
        self.applied_at = applied_at

class LegacyMigrations:
    # Mimics the sqlite-migrate 0.x Migrations class, in particular
    # apply(db, stop_before=None) taking a single string
    migrations_table = "_sqlite_migrations"

    def __init__(self, name):
        self.name = name
        self._migrations = []

    def __call__(self, fn):
        self._migrations.append(_Migration(fn.__name__, fn))
        return fn

    def ensure_migrations_table(self, db):
        db[self.migrations_table].create(
            {"migration_set": str, "name": str, "applied_at": str},
            pk=("migration_set", "name"),
            if_not_exists=True,
        )

    def applied(self, db):
        self.ensure_migrations_table(db)
        return [
            _Applied(row["name"], row["applied_at"])
            for row in db[self.migrations_table].rows_where(
                "migration_set = ?", [self.name]
            )
        ]

    def pending(self, db):
        applied = {m.name for m in self.applied(db)}
        return [m for m in self._migrations if m.name not in applied]

    def apply(self, db, stop_before=None):
        for migration in self.pending(db):
            if migration.name == stop_before:
                return
            migration.fn(db)
            db[self.migrations_table].insert(
                {
                    "migration_set": self.name,
                    "name": migration.name,
                    "applied_at": str(
                        datetime.datetime.now(datetime.timezone.utc)
                    ),
                }
            )

legacy = LegacyMigrations("legacy_set")

@legacy
def first(db):
    db["first"].insert({"hello": "world"})

@legacy
def second(db):
    db["second"].insert({"hello": "world"})
"""


def test_stop_before_unknown_name_errors(two_migrations):
    path, _ = two_migrations
    db_path = str(path / "test.db")
    runner = CliRunner()
    result = runner.invoke(
        sqlite_utils.cli.cli,
        ["migrate", db_path, str(path), "--stop-before", "fooo"],
    )
    assert result.exit_code == 1
    assert "--stop-before did not match any migrations: fooo" in result.output
    # Nothing should have been applied
    db = sqlite_utils.Database(db_path)
    assert "foo" not in db.table_names()
    assert "bar" not in db.table_names()


def test_stop_before_with_legacy_migrations_class(tmpdir):
    path = pathlib.Path(tmpdir)
    (path / "migrations.py").write_text(LEGACY_MIGRATIONS, "utf-8")
    db_path = str(path / "test.db")
    runner = CliRunner()
    result = runner.invoke(
        sqlite_utils.cli.cli,
        ["migrate", db_path, str(path), "--stop-before", "second"],
    )
    assert result.exit_code == 0, result.output
    db = sqlite_utils.Database(db_path)
    assert "first" in db.table_names()
    assert "second" not in db.table_names()


def test_stop_before_multiple_values_for_legacy_set_errors(tmpdir):
    path = pathlib.Path(tmpdir)
    (path / "migrations.py").write_text(LEGACY_MIGRATIONS, "utf-8")
    db_path = str(path / "test.db")
    runner = CliRunner()
    result = runner.invoke(
        sqlite_utils.cli.cli,
        [
            "migrate",
            db_path,
            str(path),
            "--stop-before",
            "legacy_set:first",
            "--stop-before",
            "legacy_set:second",
        ],
    )
    assert result.exit_code == 1
    assert "single --stop-before" in result.output


def test_list_does_not_create_database_file(two_migrations):
    path, _ = two_migrations
    db_path = path / "test.db"
    runner = CliRunner()
    result = runner.invoke(
        sqlite_utils.cli.cli, ["migrate", str(db_path), str(path), "--list"]
    )
    assert result.exit_code == 0, result.output
    assert "Pending:\n    foo\n    bar" in result.output
    # Listing migrations must not create the database file
    assert not db_path.exists()


def test_list_does_not_upgrade_legacy_migrations_table(two_migrations):
    path, _ = two_migrations
    db_path = str(path / "test.db")
    db = sqlite_utils.Database(db_path)
    db["_sqlite_migrations"].create(
        {"migration_set": str, "name": str, "applied_at": str},
        pk=("migration_set", "name"),
    )
    db["_sqlite_migrations"].insert(
        {"migration_set": "hello", "name": "foo", "applied_at": "x"}
    )
    db.close()
    runner = CliRunner()
    result = runner.invoke(
        sqlite_utils.cli.cli, ["migrate", db_path, str(path), "--list"]
    )
    assert result.exit_code == 0, result.output
    assert "foo - x" in result.output
    # --list must not perform the one-way legacy schema upgrade
    db2 = sqlite_utils.Database(db_path)
    assert db2["_sqlite_migrations"].pks == ["migration_set", "name"]
    db2.close()
