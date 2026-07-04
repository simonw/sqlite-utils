# sqlite-utils 4.0rc1 pre-release review

Final review before the stable 4.0 release, focused on issues that would be
**breaking changes if fixed after 4.0 ships**. Reviewed at commit `79117b9`
(4.0rc1 plus four commits). Every finding below was verified against the
actual code; the two most serious were reproduced end-to-end.

Test suite at review time: **1080 passed, 16 skipped**.

**Verdict: do not tag stable yet.** There are five release blockers, all of
which are small fixes, plus a handful of semantic decisions that are cheap to
make now and breaking to change later. Recommended path: fix the blockers,
make the documented decisions, cut an **rc2**.

---

## Release blockers

Data loss or wrong-by-default behavior that 4.0 would lock in.

### 1. `delete_where()` never commits and poisons the connection (data loss)

`Table.delete_where()` (`sqlite_utils/db.py:2948`) runs its DELETE via a bare
`self.db.execute()` with no `atomic()` wrapper — compare `Table.delete()` at
`db.py:2944`, which wraps correctly. The connection is left
`in_transaction=True`, so every *subsequent* `atomic()` call takes the
savepoint branch (`db.py:430-440`) and never commits either.

Reproduced end-to-end:

```python
db = sqlite_utils.Database("dw.db")
db["t"].insert_all([{"id": i} for i in range(3)], pk="id")
db["t"].delete_where("id = ?", [0])   # conn.in_transaction is now True
db["t"].insert({"id": 50})
db["u"].insert({"a": 1})
db.close()
# Reopen: rows are [0, 1, 2] — the delete, row 50, AND table u are all gone.
```

In 3.x this leak was latent because later operations used `with db.conn:`
which committed the pending work (verified against 3.38, where the same
sequence persists everything). The 4.0 `atomic()` design — "don't commit an
existing transaction" — converts the old latent leak into permanent silent
data loss.

**`optimize()` (`db.py:2790`) and `rebuild_fts()` (`db.py:2752`) have the
identical leak** — both run `INSERT INTO fts(fts) VALUES(...)` via bare
`self.db.execute()`. The CLI escapes only because `cli.py:341` and
`cli.py:369` wrap the calls in `with db.conn:`.

Tellingly, the only `delete_where` example in the docs
(`docs/python-api.rst:983-990`) already wraps it in `with db.atomic():`,
papering over the bug, while every other single-op example needs no wrapper.

**Fix:** wrap all three in `with self.db.atomic():`; update the docs example.
These were the only leaky public write ops found — every other op
(insert/upsert/update/delete/lookup/transform/extract/create/add_column/
create_index/enable_fts/enable_counts/m2m/convert/duplicate) was verified to
leave `in_transaction=False`.

### 2. `drop-view` drops tables and `drop-table` drops views, silently

`cli.py:1728` (`drop-table`) and `cli.py:1800` (`drop-view`) both use
`db[name].drop()`. `Database.__getitem__` dispatches on the actual object
type, and `Table.drop()`/`View.drop()` each issue their matching `DROP`
statement.

Reproduced: `sqlite-utils drop-view t.db t` deleted **table `t`** with exit
code 0. The mirror case (`drop-table` on a view) also succeeds silently.

This is a data-loss footgun that contradicts the issue #657 table/view split
that is a headline 4.0 breaking change. Fixing it later converts a silent
"success" into an error — a semver problem, so it must land in 4.0.

**Fix:** one line each — use `db.table(name)` / `db.view(name)` and catch
`NoTable` / `NoView` for a clean `ClickException`.

### 3. Post-rc1 `insert({})` change lets `upsert` silently insert rows

The new `DEFAULT VALUES` branch (commit `b5d0080`,
`db.py:3184-3194`) checks `not list_mode and not all_columns` but never
checks the `upsert` flag:

- `db["t"].upsert({}, pk="id")` executes `INSERT INTO t DEFAULT VALUES` — a
  brand-new row is written — and *then* raises an unrelated `KeyError: 'id'`
  from the `last_pk` computation (`db.py:3742-3746`).
- `upsert_all([{}, {}], pk="id")` (2+ records skips the `last_pk` path)
  **silently inserts two new rows on every call, no error at all**.

At rc1 both failed fast with `ZeroDivisionError` — ugly, but zero rows
written. Going from crash-without-mutation to silent insertion under
"upsert" semantics is a regression 4.0 would lock in.

Relatedly, the compound-pk auto-detection (commit `bfd74a3`,
`db.py:3557-3560`) makes records that *omit* the pk value reachable where
they previously raised `PrimaryKeyRequired` before touching the database:
`upsert_all([{"v": "a"}, {"v": "b"}])` on an existing pk table now inserts
with `id=NULL` (never conflicts) and appends new rows on every call
(verified: two calls → 4 rows). Same for compound pks with a missing
component. Not covered by any test.

**Fix:** raise a clean error in the empty-record and missing-pk-value paths
when `upsert=True` (an empty record has no pk value, so it can never be an
upsert).

### 4. `Migrations.apply()` transaction semantics are accidental — decide now

`sqlite_utils/migrations.py:84-95` runs each migration function and its
tracking-row insert with no transaction wrapper. Verified: a migration that
fails halfway leaves its partial side effects committed, records nothing,
and re-running re-executes the *entire* function including already-applied
statements — the classic double-apply hazard.

This matches sqlite-migrate, so either behavior is defensible — but 4.0
freezes the contract: adding per-migration transactions in 4.1 would break
migrations that manage their own transactions or run statements illegal
inside one (`VACUUM`, some `PRAGMA`s).

**Decide now:** wrap each migration in `db.atomic()`, or explicitly document
"migrations are not transactional; write idempotent steps" so the current
behavior is the contract rather than an accident.

Also decide now: **`_AppliedMigration.applied_at` is annotated
`datetime.datetime` (`migrations.py:21`) but is a `str` at runtime**
(`migrations.py:67` passes the TEXT column value straight through), and the
type is published via autodoc (`docs/migrations.rst:168-171`). Changing the
annotation to `str` now is a free one-liner (and the sqlite-migrate-
compatible choice); "fixing" it to a real datetime later breaks every
consumer doing string operations.

### 5. `enable_wal()` / `disable_wal()` commit open transactions

`ensure_autocommit_off()` (`db.py:456-472`) assigns `conn.isolation_level`,
and CPython's setter **commits any pending transaction** as a side effect.
Verified consequences:

- `with db.atomic(): insert(2); db.enable_wal(); insert(3); raise` → **all
  rows persist despite the exception**, directly contradicting the
  documented rollback guarantee (`docs/python-api.rst:255`), which is *the*
  headline 4.0 semantic.
- A user's own `BEGIN` + insert + `enable_wal()` → their subsequent
  `rollback()` is a no-op; the insert persists. This is exactly the
  "unexpectedly committing an existing transaction" bug class `atomic()` was
  introduced to eliminate (changelog, issue #755).

Pre-existing in 3.x, but 4.0 is the moment to fix.

**Fix:** make `enable_wal`/`disable_wal` raise (or skip the isolation dance)
when `conn.in_transaction`.

---

## Decisions to make now — cheap today, breaking after 4.0

### `Database.__enter__` / `__exit__` contract is unpinned

`__exit__` only calls `self.close()` (`db.py:408-417`), which silently rolls
back uncommitted changes. Verified: a raw `db.execute("insert ...")` inside
`with Database(p) as db:` is discarded on exit. This diverges from
`sqlite3.Connection`'s own context manager (commits on success, does *not*
close). The docs (`docs/python-api.rst:139-145`) say only "automatically
close the connection" — no mention of rollback — and **no test anywhere uses
`with Database(...)`**, so the contract is completely unpinned. Whichever way
this might later be "improved" is a breaking behavior change. Decide now,
add one docs sentence ("any uncommitted changes are rolled back") and a
pinning test. Note blockers 1–2 make this worse: after a `delete_where`,
exiting the `with` block discards everything.

### `atomic()` is broken on Python 3.12+ `autocommit` connections

`db.py:441-453` uses `conn.commit()`/`conn.rollback()`, documented no-ops
when `Connection.autocommit=True`. Verified on 3.13:

- `autocommit=True` connection → a successful `atomic()` block leaves the
  transaction permanently open; rollback on exception is also a no-op.
- `autocommit=False` connection → the connection is always in a transaction,
  so `atomic()` always takes the savepoint branch and never commits
  anything.

`Database(filename_or_conn)` accepts arbitrary connections (`db.py:397`).
Either handle these modes (issue literal `COMMIT`/`ROLLBACK` statements, or
reject such connections) or document supported connection modes in
`python_api_atomic` now — changing commit timing later is breaking.
(Legacy `isolation_level=None` connections were verified to work correctly.)

### `atomic()` savepoint-branch semantics are intended but undocumented

If the connection is already in a transaction (user ran `BEGIN` or raw DML),
`atomic()` becomes a savepoint and its successful exit does **not** persist
anything until the user commits (verified: a user `rollback()` discards the
atomic block's writes). Reasonable semantics, but the `conn.in_transaction`
sniffing must be documented before it is locked — the docs currently
describe nesting only in terms of `atomic()`-inside-`atomic()`.

### `atomic()` issues plain deferred `BEGIN`

`db.py:442` — no way to request `BEGIN IMMEDIATE`. A future switch of the
default would change locking/`SQLITE_BUSY` behavior (breaking); adding an
`immediate=` parameter later is fine. Documenting "deferred" now costs one
sentence.

### No-op `-d/--detect-types` flag: keep or kill

`cli.py:942-947` still defines `-d/--detect-types` on `insert`/`upsert`
(help says "(default)"), deliberately tested as a no-op
(`tests/test_cli.py:2299,2322`). The `detect_types` parameter of
`insert_upsert_implementation` (`cli.py:1002`) is dead code — the body only
reads `no_detect_types`. If the flag is ever to be removed, 4.0 is the only
chance; keeping it as back-compat is defensible, but note
`--detect-types --no-detect-types` together silently favors the latter with
no conflict error.

### `--stop-before` is silently ignored for old `sqlite_migrate` objects

The duck-typing in `_compatible_migration_set` (`cli.py:3286-3289`) exists
precisely so migration files still doing `from sqlite_migrate import
Migrations` keep working — but `cli.py:3393` always passes `stop_before=` as
a **list**, and the old plugin's `apply()` does `if name == stop_before:`
against a string. Verified with the released 0.1b0: `--stop-before step2`
applied `step2` anyway. The CLI applies the exact migration the user asked
it not to, on the one upgrade path the duck typing exists to support.

Related: a **typo'd `--stop-before` name silently applies everything**,
including the migration you meant to stop before (unknown names simply never
match; no error from the CLI). Validation has to live in the CLI ("each
`--stop-before` value must match at least one set") because unqualified
names legitimately fan out across sets. Adding that error post-4.0 turns
currently-succeeding invocations into failures — decide now.

### Public-API validation via bare `assert`

User-facing errors raised via `assert` throughout `db.py` (370, 396, 1016,
1950, 2829, 3028, 3565, 3571, 3967, …). They vanish under `python -O`, and
`db.py:3028` carries `# TODO: Test this works (rolls back) - use better
exception:`. Converting `AssertionError` to `ValueError` later changes
exception types callers may catch — best done in a major, if ever.

---

## Should-fix polish (non-breaking later, but ugly for a stable release)

- **`insert`/`upsert` into a view name prints a raw traceback** —
  `NoTable("Table v is actually a view")` from `cli.py:1154` is not
  converted to `ClickException` (contrast `duplicate` at `cli.py:1676`).
- **Misleading `NoView` message** — `db.view("t")` where `t` is a table says
  "View t does not exist" (`db.py:661`); the mirror case helpfully says
  "Table v is actually a view" (`db.py:650`).
- **Stale `__getitem__` docstring** (`db.py:502`) says it "returns a Table
  object"; it returns a `View` for views. Feeds autodoc.
- **Wrong `hash_id` doc in the `Table` docstring** (`db.py:1588`) — says
  bool; it's a column-name string (`Optional[str]`).
- **`sqlite-utils migrate --list` writes to the database** —
  `pending()`/`applied()` call `ensure_migrations_table`
  (`migrations.py:48,65`), so `--list` creates the tracking table, performs
  the one-way legacy sqlite-migrate schema upgrade, and — since `db_path`
  has no `exists=True` (`cli.py:3338-3340`) — creates the database file
  itself. A read-looking operation should not do any of that.
- **`applied()` has no `ORDER BY`** (`migrations.py:66-71`) — relies on
  rowid order; `tests/test_cli_migrate.py:104-107` asserts that order.
  Add `order_by="id"` to make `--list` deterministic by contract.
- **Duplicate migration names within a set**: both functions execute (side
  effects committed) before an opaque `IntegrityError`; only the first is
  recorded (`migrations.py:36-42`). A `ValueError` at registration would be
  cheap and is effectively non-breaking to add now.
- **`pending()`/`applied()` return underscore-private dataclasses**
  (`_Migration`/`_AppliedMigration`) that are excluded from autodoc
  (`docs/migrations.rst:171`), so the `.name`/`.applied_at`/`.fn` fields
  users must access are undocumented API.
- **`set:name` colon syntax is unvalidated** (`cli.py:3328` uses
  `partition(":")`) — a set or migration name containing `:` can never be
  targeted. Document/validate "no colons in names" now.
- **Bare `@migrations` decorator** gives an opaque `TypeError`
  (`migrations.py:30`); a helpful message would be purely additive.
- **Tracer never sees transaction statements** — `atomic()` uses
  `self.conn.execute()`/`conn.commit()` directly (`db.py:432-450`),
  bypassing the tracer. Routing through `self.execute()` later would change
  tracer output that downstream tests may assert on — cheap to decide now.
- **Order-dependent empty-dict semantics** (from `b5d0080`):
  `insert_all([{}, {"v": "hi"}])` gives the empty record its column DEFAULTs,
  but `insert_all([{"v": "hi"}, {}])` inserts explicit `NULL` for it — a
  `NOT NULL ... DEFAULT` column succeeds in one ordering and raises
  `IntegrityError` in the other.
- **Batch-size cliff when the first record is `{}`** (`db.py:3625-3629`) —
  `num_columns` comes from the first record only, forcing `batch_size=1`
  for the entire stream. Performance-only, degenerate input.
- **`pyproject.toml` build-system underpinned** — `requires = ["setuptools"]`
  while the PEP 639 `license = "Apache-2.0"` string needs setuptools ≥
  77.0.3; non-isolated sdist builds on older setuptools will fail. Pin
  `setuptools>=77`.
- **Undocumented post-rc1 behavior changes** — neither upsert pk
  auto-detection (`bfd74a3`) nor `insert({})`-uses-DEFAULT-VALUES
  (`b5d0080`) is mentioned in `docs/` or the changelog. Note also that
  `db.table(name).insert({})` only works for *existing* tables — on a
  missing table it still raises `AssertionError: Tables must have at least
  one column`, so the issue #759 title scenario is only partially covered.

---

## Verified sound — no action needed

- **sqlite-migrate on-disk compatibility** (the scariest area): tested
  against the actual 0.1b0 sdist from PyPI. Same `_sqlite_migrations` table;
  the legacy compound-pk `(migration_set, name)` schema is detected via
  `table.pks != ["id"]` and upgraded in place (`migrations.py:97-117`); rows
  are preserved and previously-applied migrations are **not** re-applied.
  Both timestamp formats are identical. Tests cover both legacy layouts
  (`tests/test_migrations.py:83-110`). No data-corruption risk for upgrading
  users. The built-in `migrate` command is registered *after* plugin hooks
  (`cli.py:3415-3417`), so a still-installed old plugin's command is
  correctly overridden.
- Migration ordering is definition order (documented and tested); identity
  is `(set name, migration name)` with a unique index; same name across sets
  is fine. The new CLI is a strict superset of the old plugin's; file
  discovery is now `sorted()` where the plugin iterated an unordered set.
- Nested savepoint rollback/release logic including exception paths
  (`tests/test_atomic.py:44-120`); commit-time deferred-FK failure rolls
  back cleanly; `transform()` inside an open transaction correctly uses
  `PRAGMA defer_foreign_keys`; `_executescript` statement-splitting avoids
  sqlite3's implicit commit. Savepoint naming via `secrets.token_hex(16)` is
  collision-free.
- Compound-pk upsert detection happy path works for single and compound pks
  on both the `ON CONFLICT` and `use_old_upsert` implementations; explicit
  `pk=` still wins over detection; rowid/missing/hash_id tables still raise
  `PrimaryKeyRequired` (`tests/test_upsert.py:52-97`).
- The table/view split (#657) is otherwise complete: `db.table()` on a view
  raises `NoTable`, never returns a `View`; `db.view()` accepts no
  table-only kwargs. The tracer-test fix (`401fb69`) and click pin
  (`79117b9`, dev-group only) are correct.
- `__all__` exports are coherent (`Database`, `Migrations`,
  `suggest_column_types`, `hookimpl`, `hookspec`); every `sqlite_utils.X`
  docs reference resolves. The `pip` runtime dependency is genuinely
  required (`cli.py:2967-2983` uses `run_module("pip")` for
  `install`/`uninstall`). Classifiers match `requires-python`. No
  deprecation debt: zero `DeprecationWarning` markers; clean under
  `-W error::DeprecationWarning`.

---

## The transform() incoming-FK branch: hold for 4.1

The `update_incoming_fks` work on `claude/investigate-transform-fk-KMTZ7` is
right to hold:

- It is purely additive (`update_incoming_fks: bool = False` keyword +
  `--update-incoming-fks` flag; default behavior unchanged), so 4.1 is safe
  semver-wise.
- The branch predates the `atomic()` refactor — it still uses
  `with self.db.conn:` directly in `transform()`, the exact pattern 4.0
  eliminated (#755) — and has real issues: `_skip_fk_validation` is mutable
  state on the whole `Database` object, and the incoming-FK rebuilds execute
  `transform_sql()` output raw for referencing tables, silently dropping
  their indexes (the 3.38 index-recreation logic only runs for the primary
  table).
- One decision belongs to 4.0: whether `transform()` should eventually
  refuse/warn *by default* when a rename would leave dangling incoming FKs.
  Adding a warning later is fine; making it an error by default later is
  breaking. If strict-by-default is the desired end state, add just the
  guard in 4.0 and ship the auto-update machinery in 4.1.

---

## Suggested path to stable

1. Fix the five blockers — three `atomic()` wrappers (`delete_where`,
   `optimize`, `rebuild_fts`), two CLI lookups (`drop-table`/`drop-view`),
   the `upsert` guard in the DEFAULT VALUES / missing-pk paths, the
   `applied_at: str` annotation, and the `in_transaction` guard in
   `enable_wal`/`disable_wal`.
2. Make and document the decisions: `apply()` transactionality,
   `Database.__exit__` semantics (+ pinning test), `atomic()` supported
   connection modes and deferred-`BEGIN` note, `--detect-types` keep/kill,
   `--stop-before` validation and old-plugin compat.
3. Cut **rc2** — the transaction-semantics fixes deserve one more candidate
   round before the semantics freeze.
