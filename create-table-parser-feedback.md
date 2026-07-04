# Review: `create-table-parser` branch

Feedback on the `create-table-parser` branch (commits `ed1de15`, `95d6ff4`,
`407db59` — 819-line `create_table_parser.py` plus JSON test corpora),
reviewed as the proposed foundation for 4.1 `transform()` improvements that
make UNIQUE and CHECK constraints supported instead of silently dropped.

**Verdict: good fit for 4.1 — right design, right release — but it has two
silent-corruption bugs that must be fixed before `transform()` can trust it,
and a few design decisions to settle.**

## Why 4.1 is the right home

- The parser is purely additive: a new module and a new capability. Nothing
  about it needs to land in 4.0, so it does not constrain or delay the
  stable release.
- The `transform()` improvement it enables — preserving CHECK and UNIQUE
  constraints instead of silently dropping them — is a behavior change in
  the bug-fix direction, appropriate for a minor release with a prominent
  changelog note.
- SQLite exposes no introspection for CHECK constraints (and only partial
  detail elsewhere), so parsing the `sqlite_master` DDL is the correct — and
  only — approach. The module docstring says exactly this.

## What holds up well (verified empirically)

- **Corpus results:** all 272 statements in `tests/valid_create_table.json`
  parse without crashing; the 230 that are independently executable were
  cross-checked against SQLite's own `PRAGMA table_xinfo` — **0 column-name
  or primary-key mismatches**. The 29 invalid statements also do not crash
  the parser.
- Survived adversarial probing: identifier quoting in all four styles
  including embedded escaped quotes (`"col ""x"""`, `[square]`, backticks),
  strings containing `,)` and keywords inside CHECK expressions,
  `col IN (...)` option extraction with escaped quotes, generated columns
  (`GENERATED ALWAYS AS ... STORED` and bare `AS`), FK action clauses
  (`ON DELETE SET NULL ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED`),
  `WITHOUT ROWID, STRICT` trailers, keyword-named columns (`"unique"`),
  multi-word types with parenthesized arguments, and `CREATE TABLE AS
  SELECT`.
- The design is genuinely good: recursive-descent functions mirroring the
  SQLite grammar are readable; the constraint list as source of truth with
  derived accessor properties (`table.checks`, `col.not_null`,
  `table.primary_key`) is the right model — adding a constraint kind means
  adding a dataclass and a property. The group-capturing tokenizer
  (parenthesized groups taken whole so nested commas never reach the parser)
  is a clean way to sidestep expression parsing.

## Must-fix before `transform()` builds on it

### 1. Comments produce phantom columns (silent corruption)

The tokenizer does not handle `--` line comments or `/* */` block comments,
and `sqlite_master` stores DDL verbatim. Reproduced:

```
CREATE TABLE t (
  a TEXT, -- user's name, (important)
  b INTEGER
)
→ columns parsed as ['a', '-']        (real: ['a', 'b'])

CREATE TABLE t (a TEXT /* legacy, do not use */, b INTEGER)
→ columns parsed as ['a', 'do', 'b']  (real: ['a', 'b'])
```

Hand-written schemas — exactly the ones that have CHECK constraints — are
exactly the ones with comments. If `transform()` rebuilds a table from a
model with wrong columns, that is data loss. Comment handling belongs in
`_tokenize` / `_locate_body` / `_split_top_level` (all three scan raw text).

### 2. Numeric literal defaults are truncated (silent corruption)

`_tokenize` splits `1.5` into `1` / `.` / `5` and `_default_value` consumes
one token. Reproduced:

| DDL                  | parsed default | correct |
|----------------------|----------------|---------|
| `DEFAULT 1.5`        | `'1'`          | `1.5`   |
| `DEFAULT -1.5`       | `'-1'`         | `-1.5`  |
| `DEFAULT 1e-3`       | `'1e'`         | `1e-3`  |
| `DEFAULT x'0102'`    | `'x'`          | `x'0102'` |

Re-emitting these in a transform rewrites `DEFAULT 1.5` as `DEFAULT 1` —
silent schema corruption. Notably the corpus already contains
`DEFAULT -45.8e22`, but nothing catches this because the fixtures are
input-only (see next point). Fix by lexing numeric literals (including
sign, decimal point, exponent) and blob literals (`x'...'`) as single
tokens, or by making `_default_value` consume the full literal.

### 3. No tests are actually executed

The JSON corpora are not wired into pytest — there is no `test_*.py` on the
branch, so the parser currently has zero executed tests. Beyond wiring the
corpus in, add **expected-output snapshots** (parse each valid statement,
assert the full structured result), not just "doesn't crash": the
`DEFAULT -45.8e22` bug sat undetected in the corpus precisely because only
inputs are recorded. A cheap high-value addition: property test that for
every executable statement, parsed column names / pk / not-null match
`PRAGMA table_xinfo`.

## Design decisions to settle

### Discard-vs-error policy for grammar the model does not capture

The plan is presumably parse → modify model → serialize, which matches how
`transform()` already rebuilds tables. That makes "what the model discards"
the critical list, because anything discarded is silently stripped from the
user's schema on transform:

- `ON CONFLICT` clauses on PRIMARY KEY / UNIQUE / NOT NULL (parsed by
  `_conflict_clause`, thrown away)
- FK `MATCH` and `DEFERRABLE INITIALLY DEFERRED` (parsed, thrown away —
  deferred FKs are real in the wild)
- `ASC` / `DESC` in table-level `PRIMARY KEY (a DESC)` / `UNIQUE (...)`
  column lists (`_column_list_group` keeps only the leading identifier;
  inline single-column PK order *is* captured)
- Unknown column constraints (`_column_constraint` consumes one token and
  returns `None`)

Either capture these in the model, or have `transform()` raise
`TransformError` when the source DDL contains grammar it cannot round-trip —
the honest-failure pattern 3.38 established for un-recreatable indexes.
Silence is the one wrong answer.

Same question for column renames: a renamed column referenced inside a CHECK
expression (stored as a raw string) cannot be mechanically rewritten, so
`transform(rename=...)` intersecting a check should raise `TransformError`
rather than emit a stale constraint. Likewise dropping a column referenced
by a table-level CHECK or multi-column UNIQUE.

### Ship it private in 4.1

The module defines `Table`, `Column`, and `ForeignKey` — the package already
has `db.Table` and a `db.ForeignKey` namedtuple with different, incompatible
fields (`(table, column, other_table, other_column)` vs
`(columns, table, references, ...)`). Two public `ForeignKey` types in one
package is a confusion trap, and 4.0 is a fresh reminder that public surface
is forever. Recommendation: land it as a private module
(`sqlite_utils._parser` or similar) powering `transform()` in 4.1, promote
to documented public API in a later minor once battle-tested. Public-later
is additive; public-now-retract-later is breaking.

Related naming nit: the parser's `Table.schema` is the attached-database
name (`main`/`temp`), while `db.Table.schema` is the DDL string. Rename one
(e.g. `database` or `schema_name`) before anything goes public.

### Housekeeping

- `create_table_parser.py` lives at the repo root; it needs to move inside
  the `sqlite_utils/` package.
- `parse_checks` is documented as a "backwards-compatible helper" but
  nothing in sqlite-utils exists for it to be compatible with — stale
  comment from an earlier draft.
- The valid corpus is strong on grammar coverage but light on real-world
  mess: add fixtures with comments, tabs/newlines in odd places, numeric
  and blob defaults, and a couple of schemas generated by sqlite-utils
  itself (double-quoted everything) and by common tools.

## Bottom line

Hold it for 4.1 exactly as planned — it has no 4.0 dependency and should not
delay the stable release. Before `transform()` builds on it: fix comment
handling and numeric/blob literal lexing, wire the fixtures into pytest with
expected-output snapshots, decide the discard-vs-error policy, and keep the
module private for one release. The bones are solid; both bugs are
tokenizer-level and small.
