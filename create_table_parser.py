"""Parse a SQLite CREATE TABLE statement into a structured, queryable model.

SQLite exposes no introspection for CHECK constraints (and only partial detail
elsewhere), so the source of truth is the original DDL in sqlite_master. This
module parses that DDL with a recursive-descent parser whose functions mirror
the SQLite grammar, and returns a `Table` made of `Column`s and typed
`Constraint`s.

The constraint list on each Column/Table is the source of truth; the convenient
accessors (`table["age"]`, `table.primary_key`, `col.not_null`, `table.checks`)
are derived views over it, so adding a new constraint type means adding a
dataclass and a property -- nothing else has to change.
"""
from dataclasses import dataclass, field
from typing import List, Optional


# --------------------------------------------------------------------------- #
# Constraint model
# --------------------------------------------------------------------------- #

class Constraint:
    """Marker base so all constraint kinds share an isinstance() umbrella."""


@dataclass
class Check(Constraint):
    check: str                       # SQL expression inside CHECK (...)
    name: str = ""                   # from CONSTRAINT <name>, usually blank
    column: str = ""                 # owning column, or "" for table-level
    options: Optional[List] = None   # values iff the check is exactly `col IN (...)`

    def __repr__(self):
        return (f"Check(check={self.check!r}, name={self.name!r}, "
                f"column={self.column!r}, options={self.options!r})")


@dataclass
class PrimaryKey(Constraint):
    columns: List[str]
    name: str = ""
    autoincrement: bool = False
    order: str = ""                  # ASC/DESC for an inline single-column PK


@dataclass
class Unique(Constraint):
    columns: List[str]
    name: str = ""


@dataclass
class NotNull(Constraint):
    column: str
    name: str = ""


@dataclass
class Default(Constraint):
    column: str
    value: str                       # raw default text, e.g. "0", "'x'", "1 + 2"
    name: str = ""


@dataclass
class Collate(Constraint):
    column: str
    collation: str
    name: str = ""


@dataclass
class Generated(Constraint):
    column: str
    expression: str
    stored: bool = False
    name: str = ""


@dataclass
class ForeignKey(Constraint):
    columns: List[str]               # local column(s)
    table: str                       # referenced table
    references: List[str] = field(default_factory=list)  # referenced column(s)
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    name: str = ""


# --------------------------------------------------------------------------- #
# Column & Table
# --------------------------------------------------------------------------- #

@dataclass
class Column:
    name: str
    type: str = ""
    constraints: List[Constraint] = field(default_factory=list)

    def __post_init__(self):
        self._table = None  # set when attached to a Table

    def _first(self, cls):
        return next((c for c in self.constraints if isinstance(c, cls)), None)

    def _inline_primary_key(self) -> bool:
        return any(isinstance(c, PrimaryKey) for c in self.constraints)

    @property
    def checks(self) -> List[Check]:
        return [c for c in self.constraints if isinstance(c, Check)]

    @property
    def not_null(self) -> bool:
        return any(isinstance(c, NotNull) for c in self.constraints)

    @property
    def primary_key(self) -> bool:
        # Part of the table's PK, whether declared inline or as a composite
        # table constraint. Falls back to inline-only if not attached to a Table.
        if self._table is not None:
            return self.name in self._table.primary_key
        return self._inline_primary_key()

    @property
    def autoincrement(self) -> bool:
        pk = self._first(PrimaryKey)
        return bool(pk and pk.autoincrement)

    @property
    def unique(self) -> bool:
        return any(isinstance(c, Unique) for c in self.constraints)

    @property
    def default(self) -> Optional[str]:
        d = self._first(Default)
        return d.value if d else None

    @property
    def collation(self) -> Optional[str]:
        c = self._first(Collate)
        return c.collation if c else None

    @property
    def generated(self) -> Optional[str]:
        g = self._first(Generated)
        return g.expression if g else None

    @property
    def foreign_key(self) -> Optional[ForeignKey]:
        return self._first(ForeignKey)

    def __repr__(self):
        bits = [repr(self.name)]
        if self.type:
            bits.append(f"type={self.type!r}")
        if self.constraints:
            bits.append(f"constraints={len(self.constraints)}")
        return f"Column({', '.join(bits)})"


@dataclass
class Table:
    name: str
    columns: List[Column] = field(default_factory=list)
    constraints: List[Constraint] = field(default_factory=list)   # table-level
    schema: str = ""
    temporary: bool = False
    if_not_exists: bool = False
    without_rowid: bool = False
    strict: bool = False

    def __post_init__(self):
        self._by_name = {c.name: c for c in self.columns}
        for col in self.columns:
            col._table = self

    # --- mapping-style column access ---
    def __getitem__(self, name) -> Column:
        return self._by_name[name]

    def __contains__(self, name) -> bool:
        return name in self._by_name

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)

    def column(self, name, default=None) -> Optional[Column]:
        return self._by_name.get(name, default)

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    # --- checks ---
    @property
    def checks(self) -> List[Check]:
        out = []
        for col in self.columns:
            out.extend(col.checks)
        out.extend(c for c in self.constraints if isinstance(c, Check))
        return out

    @property
    def column_checks(self):
        return {col.name: col.checks for col in self.columns if col.checks}

    @property
    def table_checks(self) -> List[Check]:
        return [c for c in self.constraints if isinstance(c, Check)]

    # --- keys ---
    @property
    def primary_key(self) -> List[str]:
        table_pk = next((c for c in self.constraints if isinstance(c, PrimaryKey)), None)
        if table_pk:
            return list(table_pk.columns)
        return [col.name for col in self.columns if col._inline_primary_key()]

    @property
    def foreign_keys(self) -> List[ForeignKey]:
        fks = [col.foreign_key for col in self.columns if col.foreign_key]
        fks += [c for c in self.constraints if isinstance(c, ForeignKey)]
        return fks

    def __repr__(self):
        q = f"{self.schema}.{self.name}" if self.schema else self.name
        return f"Table({q!r}, columns=[{', '.join(self.column_names)}])"


# --------------------------------------------------------------------------- #
# Lexical helpers (string / identifier / paren aware)
# --------------------------------------------------------------------------- #

def _unquote(tok):
    tok = (tok or "").strip()
    if len(tok) >= 2 and tok[0] in ('"', '`', "'") and tok[-1] == tok[0]:
        return tok[1:-1].replace(tok[0] * 2, tok[0])
    if len(tok) >= 2 and tok[0] == '[' and tok[-1] == ']':
        return tok[1:-1]
    return tok


def _coerce(val):
    val = val.strip()
    if val and val[0] in ("'", '"') and val[-1] == val[0]:
        return val[1:-1].replace(val[0] * 2, val[0])
    for cast in (int, float):
        try:
            return cast(val)
        except ValueError:
            pass
    return val


def _split_top_level(body):
    items, depth, start, i, n = [], 0, 0, 0, len(body)
    while i < n:
        c = body[i]
        if c in ("'", '"', '`'):
            i += 1
            while i < n:
                if body[i] == c:
                    if i + 1 < n and body[i + 1] == c:
                        i += 2; continue
                    i += 1; break
                i += 1
            continue
        if c == '[':
            while i < n and body[i] != ']':
                i += 1
            i += 1; continue
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        elif c == ',' and depth == 0:
            items.append(body[start:i]); start = i + 1
        i += 1
    items.append(body[start:])
    return [it.strip() for it in items if it.strip()]


def _tokenize(s):
    """Top-level tokens: ('word'|'string'|'group'|'punct', text). Parenthesised
    groups are captured whole so nested commas/keywords never reach the parser."""
    tokens, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c.isspace():
            i += 1; continue
        if c in ("'", '"', '`'):
            start = i; i += 1
            while i < n:
                if s[i] == c:
                    if i + 1 < n and s[i + 1] == c:
                        i += 2; continue
                    i += 1; break
                i += 1
            tokens.append(('string', s[start:i])); continue
        if c == '[':
            start = i; i += 1
            while i < n and s[i] != ']':
                i += 1
            i += 1
            tokens.append(('string', s[start:i])); continue
        if c == '(':
            start = i; depth = 0
            while i < n:
                cc = s[i]
                if cc in ("'", '"', '`'):
                    i += 1
                    while i < n:
                        if s[i] == cc:
                            if i + 1 < n and s[i + 1] == cc:
                                i += 2; continue
                            i += 1; break
                        i += 1
                    continue
                if cc == '(':
                    depth += 1
                elif cc == ')':
                    depth -= 1
                    if depth == 0:
                        i += 1; break
                i += 1
            tokens.append(('group', s[start:i])); continue
        if c.isalnum() or c in '_$':
            start = i
            while i < n and (s[i].isalnum() or s[i] in '_$'):
                i += 1
            tokens.append(('word', s[start:i])); continue
        tokens.append(('punct', c)); i += 1
    return tokens


def _locate_body(sql):
    """Return (open_idx, close_idx) of the outermost ( ), or (-1, -1)."""
    i, n, depth, open_idx = 0, len(sql), 0, -1
    while i < n:
        c = sql[i]
        if c in ("'", '"', '`'):
            i += 1
            while i < n:
                if sql[i] == c:
                    if i + 1 < n and sql[i + 1] == c:
                        i += 2; continue
                    i += 1; break
                i += 1
            continue
        if c == '[':
            while i < n and sql[i] != ']':
                i += 1
            i += 1; continue
        if c == '(':
            if depth == 0:
                open_idx = i
            depth += 1
        elif c == ')':
            depth -= 1
            if depth == 0:
                return open_idx, i
        i += 1
    return open_idx, -1


def _leading_ident(part):
    toks = _tokenize(part)
    return _unquote(toks[0][1]) if toks else ""


def _parse_options(expr):
    """Value list iff the whole expression is exactly `col IN (...)`, else None."""
    toks = _tokenize(expr)
    if len(toks) != 3:
        return None
    (k0, v0), (k1, v1), (k2, v2) = toks
    is_ident = k0 == 'word' or (k0 == 'string' and v0[0] in '"[`')
    if not (is_ident and k1 == 'word' and v1.upper() == 'IN' and k2 == 'group'):
        return None
    return [_coerce(x) for x in _split_top_level(v2[1:-1])]


# --------------------------------------------------------------------------- #
# Parser
# --------------------------------------------------------------------------- #

_TABLE_CONSTRAINT = {"PRIMARY", "UNIQUE", "CHECK", "FOREIGN"}
_COLUMN_CONSTRAINT = {"PRIMARY", "UNIQUE", "CHECK", "COLLATE", "REFERENCES",
                      "GENERATED", "AS"}


class _TableParser:
    def __init__(self, sql):
        self.sql = sql
        self.toks = []
        self.pos = 0
        self.columns: List[Column] = []
        self.table_constraints: List[Constraint] = []

    # -- cursor --
    def _peek(self, offset=0):
        i = self.pos + offset
        return self.toks[i] if 0 <= i < len(self.toks) else (None, None)

    def _kind(self, offset=0):
        return self._peek(offset)[0]

    def _val(self, offset=0):
        return self._peek(offset)[1]

    def _advance(self):
        tok = self._peek()
        self.pos += 1
        return tok

    def _at_end(self):
        return self.pos >= len(self.toks)

    def _kw(self, *words, offset=0):
        k, v = self._peek(offset)
        return k == "word" and v is not None and v.upper() in words

    def _comma(self):
        return self._peek() == ("punct", ",")

    # -- entry --
    def parse(self) -> Table:
        open_idx, close_idx = _locate_body(self.sql)
        header = self.sql[:open_idx] if open_idx != -1 else self.sql
        info = self._parse_header(header)

        if open_idx != -1 and not info["as_select"]:
            body = self.sql[open_idx + 1:close_idx] if close_idx != -1 else self.sql[open_idx + 1:]
            self.toks = _tokenize(body)
            self._body()

        trailer = self.sql[close_idx + 1:] if close_idx != -1 else ""
        ups = [t[1].upper() for t in _tokenize(trailer) if t[0] == "word"]

        return Table(
            name=info["name"], columns=self.columns, constraints=self.table_constraints,
            schema=info["schema"], temporary=info["temporary"],
            if_not_exists=info["if_not_exists"],
            without_rowid=("WITHOUT" in ups and "ROWID" in ups),
            strict=("STRICT" in ups),
        )

    def _parse_header(self, header):
        toks = _tokenize(header)
        def up(k): return toks[k][1].upper() if 0 <= k < len(toks) and toks[k][0] == "word" else None
        j = 0
        temporary = if_not_exists = as_select = False
        schema = name = ""
        if up(j) == "CREATE": j += 1
        if up(j) in ("TEMP", "TEMPORARY"): temporary = True; j += 1
        if up(j) == "TABLE": j += 1
        if up(j) == "IF" and up(j + 1) == "NOT" and up(j + 2) == "EXISTS":
            if_not_exists = True; j += 3
        if j < len(toks):
            first = _unquote(toks[j][1]); j += 1
            if j < len(toks) and toks[j] == ("punct", "."):
                j += 1
                schema = first
                name = _unquote(toks[j][1]) if j < len(toks) else ""; j += 1
            else:
                name = first
        if up(j) == "AS":
            as_select = True
        return dict(name=name, schema=schema, temporary=temporary,
                    if_not_exists=if_not_exists, as_select=as_select)

    def _body(self):
        if self._at_end():
            return
        self._definition()
        while self._comma():
            self._advance()
            if self._at_end():
                break
            self._definition()

    def _definition(self):
        if self._kw("CONSTRAINT") or self._kw(*_TABLE_CONSTRAINT):
            self._table_constraint(self._constraint_name())
        else:
            self._column_def()

    def _constraint_name(self):
        if self._kw("CONSTRAINT"):
            self._advance()
            return _unquote(self._advance()[1])
        return ""

    # -- columns --
    def _column_def(self):
        name = _unquote(self._advance()[1])
        ctype = self._type_name()
        cons: List[Constraint] = []
        while not self._at_end() and not self._comma():
            c = self._column_constraint(name)
            if c is not None:
                cons.append(c)
        self.columns.append(Column(name=name, type=ctype, constraints=cons))

    def _type_name(self):
        words = []
        while self._kind() == "word" and not self._starts_column_constraint():
            words.append(self._advance()[1])
        ctype = " ".join(words)
        if words and self._kind() == "group":
            ctype += self._advance()[1]
        return ctype

    def _column_constraint(self, column):
        name = self._constraint_name()
        if self._kw("CHECK") and self._kind(1) == "group":
            self._advance()
            expr = self._advance()[1][1:-1].strip()
            return Check(expr, name=name, column=column, options=_parse_options(expr))
        if self._kw("PRIMARY"):
            self._advance()
            if self._kw("KEY"): self._advance()
            order = ""
            if self._kw("ASC", "DESC"): order = self._advance()[1].upper()
            self._conflict_clause()
            auto = False
            if self._kw("AUTOINCREMENT"): self._advance(); auto = True
            return PrimaryKey([column], name=name, autoincrement=auto, order=order)
        if self._kw("NOT"):
            self._advance()
            if self._kw("NULL"): self._advance()
            self._conflict_clause()
            return NotNull(column, name=name)
        if self._kw("NULL"):
            self._advance(); self._conflict_clause(); return None
        if self._kw("UNIQUE"):
            self._advance(); self._conflict_clause()
            return Unique([column], name=name)
        if self._kw("DEFAULT"):
            self._advance()
            return Default(column, self._default_value(), name=name)
        if self._kw("COLLATE"):
            self._advance()
            coll = _unquote(self._advance()[1])
            return Collate(column, coll, name=name)
        if self._kw("REFERENCES"):
            return self._foreign_key_clause([column], name)
        if self._kw("GENERATED", "AS"):
            return self._generated(column, name)
        # unknown: consume one token to guarantee progress
        self._advance()
        return None

    def _default_value(self):
        k, v = self._peek()
        if k == "group":
            self._advance(); return v[1:-1].strip()
        if k == "punct" and v in "+-":
            self._advance(); return v + (self._advance()[1] or "")
        self._advance(); return v

    def _generated(self, column, name):
        if self._kw("GENERATED"):
            self._advance()
            if self._kw("ALWAYS"): self._advance()
        if self._kw("AS"): self._advance()
        expr = ""
        if self._kind() == "group":
            expr = self._advance()[1][1:-1].strip()
        stored = False
        if self._kw("STORED"): self._advance(); stored = True
        elif self._kw("VIRTUAL"): self._advance()
        return Generated(column, expr, stored=stored, name=name)

    def _conflict_clause(self):
        if self._kw("ON") and self._kw("CONFLICT", offset=1):
            self._advance(); self._advance()
            if self._kind() == "word": self._advance()

    def _foreign_key_clause(self, columns, name):
        self._advance()  # REFERENCES
        table = _unquote(self._advance()[1]) if self._kind() in ("word", "string") else ""
        refs = []
        if self._kind() == "group":
            refs = [_leading_ident(p) for p in _split_top_level(self._advance()[1][1:-1])]
        on_delete = on_update = "NO ACTION"
        while True:
            if self._kw("ON"):
                self._advance()
                which = (self._advance()[1] or "").upper()   # DELETE | UPDATE
                action = self._fk_action()
                if which == "DELETE": on_delete = action
                elif which == "UPDATE": on_update = action
            elif self._kw("MATCH"):
                self._advance()
                if self._kind() == "word": self._advance()
            elif self._kw("DEFERRABLE") or (self._kw("NOT") and self._kw("DEFERRABLE", offset=1)):
                if self._kw("NOT"): self._advance()
                self._advance()  # DEFERRABLE
                if self._kw("INITIALLY"):
                    self._advance()
                    if self._kind() == "word": self._advance()
            else:
                break
        return ForeignKey(columns, table, references=refs,
                          on_delete=on_delete, on_update=on_update, name=name)

    def _fk_action(self):
        if self._kw("SET"):
            self._advance()
            return "SET " + (self._advance()[1] or "").upper()
        if self._kw("NO"):
            self._advance()
            if self._kind() == "word": self._advance()
            return "NO ACTION"
        if self._kind() == "word":
            return (self._advance()[1] or "").upper()
        return ""

    def _starts_column_constraint(self, offset=0):
        k, v = self._peek(offset)
        if k != "word":
            return False
        up = v.upper()
        if up == "NOT":
            return self._kw("NULL", offset=offset + 1)
        if up == "DEFAULT":
            return not self._kw("SET", offset=offset - 1)
        if up == "CONSTRAINT":
            return True
        return up in _COLUMN_CONSTRAINT

    # -- table constraints --
    def _table_constraint(self, name):
        if self._kw("CHECK") and self._kind(1) == "group":
            self._advance()
            expr = self._advance()[1][1:-1].strip()
            self.table_constraints.append(
                Check(expr, name=name, column="", options=_parse_options(expr)))
        elif self._kw("PRIMARY"):
            self._advance()
            if self._kw("KEY"): self._advance()
            cols = self._column_list_group()
            self._conflict_clause()
            self.table_constraints.append(PrimaryKey(cols, name=name))
        elif self._kw("UNIQUE"):
            self._advance()
            cols = self._column_list_group()
            self._conflict_clause()
            self.table_constraints.append(Unique(cols, name=name))
        elif self._kw("FOREIGN"):
            self._advance()
            if self._kw("KEY"): self._advance()
            cols = self._column_list_group()
            self.table_constraints.append(self._foreign_key_clause(cols, name))
        # consume any remainder up to the next top-level comma
        while not self._at_end() and not self._comma():
            self._advance()

    def _column_list_group(self):
        if self._kind() == "group":
            return [_leading_ident(p) for p in _split_top_level(self._advance()[1][1:-1])]
        return []


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def parse_create_table(create_sql) -> Table:
    """Parse a CREATE TABLE statement into a Table."""
    return _TableParser(create_sql).parse()


def parse_checks(create_sql) -> List[Check]:
    """Backwards-compatible helper: just the CHECK constraints, in declaration order."""
    return parse_create_table(create_sql).checks
