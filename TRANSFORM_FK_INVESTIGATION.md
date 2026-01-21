# Investigation: Foreign Key Constraints During transform()

This document summarizes the investigation into how the `transform()` method handles
foreign key constraints that **reference** the table being transformed (incoming FKs).

## Background

The `transform()` method in sqlite-utils performs the following steps:
1. Create a new temporary table with the desired schema
2. Copy data from the old table to the new table
3. Drop the old table
4. Rename the new table to the original table name
5. Recreate indexes

This raises the question: what happens to foreign key constraints in **other tables**
that reference the table being transformed?

## Key Finding: SQLite's ALTER TABLE RENAME Behavior

SQLite's `ALTER TABLE ... RENAME TO` command automatically updates all foreign key
references in the schema. This is documented in the SQLite documentation:

> "The RENAME command renames the table, and also updates all references to the
> table within the schema" - https://www.sqlite.org/lang_altertable.html

This means:
- When `authors_new_xxx` is renamed to `authors`, all FK constraints that reference
  `authors` continue to work correctly
- The FK references are stored by table **name**, and the rename updates them

## Test Scenarios and Results

### Scenario 1: Simple transform (no column rename)
```
Setup: books.author_id REFERENCES authors(id)
Action: db["authors"].transform(types={"name": str})
Result: FK constraints survive intact (both with FK ON and OFF)
```

### Scenario 2: Rename a non-referenced column
```
Setup: books.author_id REFERENCES authors(id)
Action: db["authors"].transform(rename={"name": "author_name"})
Result: FK constraints survive intact (both with FK ON and OFF)
```

### Scenario 3: Rename the referenced column (FK ON)
```
Setup: books.author_id REFERENCES authors(id)
Action: db["authors"].transform(rename={"id": "author_pk"})
Result: FAILS - "foreign key mismatch" error, transaction rolled back
```
The transform correctly detects the FK violation via `PRAGMA foreign_key_check`
and rolls back the transaction, preserving the original schema.

### Scenario 4: Rename the referenced column (FK OFF)
```
Setup: books.author_id REFERENCES authors(id)
Action: db["authors"].transform(rename={"id": "author_pk"})
Result: Transform succeeds, but FK constraint is now BROKEN
```
The FK in `books` still references `authors(id)` but that column no longer exists.
Running `PRAGMA foreign_key_check` produces a "foreign key mismatch" error.

### Scenario 5: Self-referential FK
```
Setup: employees.manager_id REFERENCES employees(id)
Action: db["employees"].transform(types={"name": str})
Result: FK constraint survives intact
```

### Scenario 6: Multiple tables referencing the transformed table
```
Setup: books.author_id, articles.writer_id, quotes.speaker_id all REFERENCE authors(id)
Action: db["authors"].transform(types={"name": str})
Result: All FK constraints survive intact
```

## How transform() Ensures FK Safety

The `transform()` method (db.py lines 1853-1917) implements the following safety measures:

1. **Saves FK enforcement state**: Checks `PRAGMA foreign_keys` before starting
2. **Disables FK enforcement**: Sets `PRAGMA foreign_keys=0` during the transform
3. **Executes transform SQL**: Within a transaction (`with self.db.conn:`)
4. **Validates FK integrity**: Runs `PRAGMA foreign_key_check` after the transform
5. **Rolls back on failure**: If FK check fails, the transaction is rolled back
6. **Restores FK state**: Re-enables FK enforcement if it was originally on

## Summary Table

| Scenario                            | FK ON              | FK OFF              |
|-------------------------------------|--------------------|--------------------|
| Simple transform                    | Works, FKs intact  | Works, FKs intact   |
| Rename non-referenced column        | Works, FKs intact  | Works, FKs intact   |
| Rename referenced column            | FAILS (rollback)   | Works, FKs BROKEN!  |
| Drop referenced column              | FAILS (rollback)   | Works, FKs BROKEN!  |
| Self-referential FK                 | Works, FKs intact  | Works, FKs intact   |
| Multiple tables with FKs            | Works, FKs intact  | Works, FKs intact   |

## Known Issue: Leftover Temp Table on Failure

When `transform()` fails (e.g., due to FK check failure), there may be a leftover
temporary table (e.g., `authors_new_xxx`). This appears to be because the error
occurs after some statements have executed but before the transaction fully commits.

The original table remains intact, so this is a minor cosmetic issue rather than
a data integrity problem.

## Recommendations

1. **Always use FK enforcement** (`PRAGMA foreign_keys=ON`) when working with
   relational data to ensure transform() catches FK violations early

2. **Be cautious when renaming columns**: If a column is referenced by FKs from
   other tables, you'll need to update those FKs as well. Consider:
   - First transforming the referencing tables to update their FK constraints
   - Then transforming the referenced table to rename the column

3. **Use `foreign_key_check`** after bulk operations with FK enforcement off to
   verify data integrity

## Code References

- `transform()` method: sqlite_utils/db.py:1853-1917
- `transform_sql()` method: sqlite_utils/db.py:1919-2127
- FK handling in transform_sql: sqlite_utils/db.py:1957-1993
- Related tests: tests/test_transform.py:301-500
