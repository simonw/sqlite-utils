"""Helpers for parsing CHECK constraints from SQLite CREATE TABLE SQL.

SQLite does not expose CHECK constraints through a pragma, so preserving them
across a table rebuild requires reading ``sqlite_schema.sql``.  This module is
deliberately small, but it uses a real lexer: strings, quoted identifiers and
comments are opaque, every token retains its source span and malformed input is
reported instead of being silently under-parsed.
"""

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Check:
    check: str
    name: str = ""
    column: str = ""
    options: list[Any] | None = None
    # Source details are excluded from equality and repr so callers can compare
    # semantic constraints while still having the original SQL available for
    # diagnostics or future lossless edits.
    sql: str = field(default="", compare=False, repr=False)
    start: int = field(default=-1, compare=False, repr=False)
    end: int = field(default=-1, compare=False, repr=False)


@dataclass(frozen=True)
class ColumnComments:
    before: str = ""
    after: str = ""


class ParseError(ValueError):
    pass


@dataclass(frozen=True)
class _Token:
    kind: str
    text: str
    start: int
    end: int

    def is_keyword(self, keyword: str) -> bool:
        return self.kind == "word" and self.text.upper() == keyword


_PUNCTUATION = frozenset("(),.;+-*/%<>=!~|&?:")
_TRIVIA = frozenset(("whitespace", "comment"))
_TABLE_CONSTRAINT_KEYWORDS = frozenset(("PRIMARY", "UNIQUE", "CHECK", "FOREIGN"))
_OTHER_COLUMN_CONSTRAINT_KEYWORDS = frozenset(
    ("PRIMARY", "UNIQUE", "REFERENCES", "DEFAULT", "NOT", "COLLATE", "GENERATED")
)
_SQLITE_KEYWORDS = frozenset(
    (
        "ABORT",
        "ACTION",
        "ADD",
        "AFTER",
        "ALL",
        "ALTER",
        "ANALYZE",
        "AND",
        "AS",
        "ASC",
        "ATTACH",
        "AUTOINCREMENT",
        "BEFORE",
        "BEGIN",
        "BETWEEN",
        "BY",
        "CASCADE",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "COMMIT",
        "CONFLICT",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "DATABASE",
        "DEFAULT",
        "DEFERRABLE",
        "DEFERRED",
        "DELETE",
        "DESC",
        "DETACH",
        "DISTINCT",
        "DO",
        "DROP",
        "EACH",
        "ELSE",
        "END",
        "ESCAPE",
        "EXCEPT",
        "EXCLUDE",
        "EXCLUSIVE",
        "EXISTS",
        "EXPLAIN",
        "FAIL",
        "FALSE",
        "FILTER",
        "FIRST",
        "FOLLOWING",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GENERATED",
        "GLOB",
        "GROUP",
        "GROUPS",
        "HAVING",
        "IF",
        "IGNORE",
        "IMMEDIATE",
        "IN",
        "INDEX",
        "INDEXED",
        "INITIALLY",
        "INNER",
        "INSERT",
        "INSTEAD",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "KEY",
        "LAST",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MATCH",
        "MATERIALIZED",
        "NATURAL",
        "NO",
        "NOT",
        "NOTHING",
        "NOTNULL",
        "NULL",
        "NULLS",
        "OF",
        "OFFSET",
        "ON",
        "OR",
        "ORDER",
        "OTHERS",
        "OUTER",
        "OVER",
        "PARTITION",
        "PLAN",
        "PRAGMA",
        "PRECEDING",
        "PRIMARY",
        "QUERY",
        "RAISE",
        "RANGE",
        "RECURSIVE",
        "REFERENCES",
        "REGEXP",
        "REINDEX",
        "RELEASE",
        "RENAME",
        "REPLACE",
        "RESTRICT",
        "RETURNING",
        "RIGHT",
        "ROLLBACK",
        "ROW",
        "ROWS",
        "SAVEPOINT",
        "SELECT",
        "SET",
        "STRICT",
        "TABLE",
        "TEMP",
        "TEMPORARY",
        "THEN",
        "TIES",
        "TO",
        "TRANSACTION",
        "TRIGGER",
        "TRUE",
        "UNBOUNDED",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VACUUM",
        "VALUES",
        "VIEW",
        "VIRTUAL",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
        "WITHOUT",
    )
)
_INTEGER_RE = re.compile(r"[+-]?(?:0[xX][0-9a-fA-F]+|[0-9]+)\Z")
_FLOAT_RE = re.compile(
    r"[+-]?(?:(?:[0-9]+\.[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|"
    r"[0-9]+[eE][+-]?[0-9]+)\Z"
)


def _lex(sql: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    while i < len(sql):
        start = i
        char = sql[i]
        if char.isspace():
            i += 1
            while i < len(sql) and sql[i].isspace():
                i += 1
            tokens.append(_Token("whitespace", sql[start:i], start, i))
            continue
        if sql.startswith("--", i):
            newline = sql.find("\n", i + 2)
            i = len(sql) if newline == -1 else newline + 1
            tokens.append(_Token("comment", sql[start:i], start, i))
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            if end == -1:
                raise ParseError("Unterminated SQL comment")
            i = end + 2
            tokens.append(_Token("comment", sql[start:i], start, i))
            continue
        if char in ("'", '"', "`"):
            quote = char
            i += 1
            while i < len(sql):
                if sql[i] == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise ParseError(f"Unterminated {quote} quoted token")
            kind = "string" if quote == "'" else "identifier"
            tokens.append(_Token(kind, sql[start:i], start, i))
            continue
        if char == "[":
            end = sql.find("]", i + 1)
            if end == -1:
                raise ParseError("Unterminated [ quoted identifier")
            i = end + 1
            tokens.append(_Token("identifier", sql[start:i], start, i))
            continue
        if char in _PUNCTUATION:
            i += 1
            tokens.append(_Token("punct", char, start, i))
            continue
        # SQLite accepts any character >= U+0080 in a bare identifier.  More
        # generally, consume until a lexical delimiter rather than relying on
        # Python's narrower definition of an alphanumeric character.
        i += 1
        while i < len(sql):
            if sql[i].isspace() or sql[i] in _PUNCTUATION or sql[i] in "'\"`[":
                break
            i += 1
        tokens.append(_Token("word", sql[start:i], start, i))
    return tokens


def _meaningful(tokens: list[_Token]) -> list[_Token]:
    return [token for token in tokens if token.kind not in _TRIVIA]


def _unquote(token: str) -> str:
    if len(token) >= 2 and token[0] in ("'", '"', "`") and token[-1] == token[0]:
        return token[1:-1].replace(token[0] * 2, token[0])
    if len(token) >= 2 and token[0] == "[" and token[-1] == "]":
        return token[1:-1]
    return token


def _matching_paren(tokens: list[_Token], open_index: int) -> int:
    if tokens[open_index].text != "(":
        raise ParseError("Expected an opening parenthesis")
    depth = 0
    for index in range(open_index, len(tokens)):
        if tokens[index].text == "(":
            depth += 1
        elif tokens[index].text == ")":
            depth -= 1
            if depth == 0:
                return index
    raise ParseError("Unbalanced parentheses")


def _split_spans(sql: str, tokens: list[_Token]) -> list[tuple[str, int, int]]:
    if not tokens:
        return []
    items: list[tuple[str, int, int]] = []
    depth = 0
    start = tokens[0].start
    for token in tokens:
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
            if depth < 0:
                raise ParseError("Unbalanced parentheses")
        elif token.text == "," and depth == 0:
            raw = sql[start : token.start]
            item = raw.strip()
            if item:
                item_start = start + len(raw) - len(raw.lstrip())
                items.append((item, item_start, item_start + len(item)))
            start = token.end
    if depth:
        raise ParseError("Unbalanced parentheses")
    raw = sql[start : tokens[-1].end]
    item = raw.strip()
    if item:
        item_start = start + len(raw) - len(raw.lstrip())
        items.append((item, item_start, item_start + len(item)))
    return items


def _split_ranges(sql: str, tokens: list[_Token]) -> list[str]:
    return [item for item, _, _ in _split_spans(sql, tokens)]


def _strip_outer_parens(tokens: list[_Token]) -> list[_Token]:
    while tokens and tokens[0].text == "(":
        close = _matching_paren(tokens, 0)
        if close != len(tokens) - 1:
            break
        tokens = tokens[1:-1]
    return tokens


_NO_LITERAL = object()


def _literal_value(text: str) -> Any:
    tokens = _meaningful(_lex(text))
    if len(tokens) == 1 and tokens[0].kind == "string":
        return _unquote(tokens[0].text)
    raw = "".join(token.text for token in tokens)
    if raw.upper() == "NULL":
        return None
    if raw.upper() == "TRUE":
        return True
    if raw.upper() == "FALSE":
        return False
    if _INTEGER_RE.fullmatch(raw):
        try:
            return (
                int(raw, 16) if raw.lower().lstrip("+-").startswith("0x") else int(raw)
            )
        except ValueError:
            return _NO_LITERAL
    if _FLOAT_RE.fullmatch(raw):
        try:
            return float(raw)
        except ValueError:
            return _NO_LITERAL
    return _NO_LITERAL


def _ascii_fold(identifier: str) -> str:
    return identifier.translate(
        str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
    )


def _parse_options(expression: str, column: str) -> list[Any] | None:
    tokens = _strip_outer_parens(_meaningful(_lex(expression)))
    if len(tokens) < 4:
        return None
    lhs = tokens[0]
    if lhs.kind not in ("word", "identifier"):
        return None
    if column and _ascii_fold(_unquote(lhs.text)) != _ascii_fold(column):
        return None
    if not tokens[1].is_keyword("IN") or tokens[2].text != "(":
        return None
    close = _matching_paren(tokens, 2)
    if close != len(tokens) - 1:
        return None
    inner = expression[tokens[2].end : tokens[close].start]
    inner_tokens = _lex(inner)
    if not _meaningful(inner_tokens):
        return []
    values = []
    for item in _split_ranges(inner, inner_tokens):
        value = _literal_value(item)
        if value is _NO_LITERAL:
            return None
        values.append(value)
    return values


def _check_after(
    item: str,
    tokens: list[_Token],
    check_index: int,
    name: str,
    column: str,
    constraint_start: int,
    base_offset: int,
) -> tuple[Check, int]:
    if check_index + 1 >= len(tokens) or tokens[check_index + 1].text != "(":
        raise ParseError("CHECK must be followed by a parenthesized expression")
    close = _matching_paren(tokens, check_index + 1)
    expression = item[tokens[check_index + 1].end : tokens[close].start].strip()
    source_start = tokens[constraint_start].start
    source_end = tokens[close].end
    return (
        Check(
            expression,
            name=name,
            column=column,
            options=_parse_options(expression, column),
            sql=item[source_start:source_end],
            start=base_offset + source_start,
            end=base_offset + source_end,
        ),
        close + 1,
    )


def _column_checks(
    item: str, tokens: list[_Token], column: str, base_offset: int
) -> list[Check]:
    checks: list[Check] = []
    pending_name = ""
    pending_start: int | None = None
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token.text == "(":
            index = _matching_paren(tokens, index) + 1
            continue
        if token.is_keyword("CONSTRAINT"):
            if index + 1 >= len(tokens):
                raise ParseError("CONSTRAINT is missing its name")
            pending_name = _unquote(tokens[index + 1].text)
            pending_start = index
            index += 2
            continue
        if token.is_keyword("CHECK"):
            check, index = _check_after(
                item,
                tokens,
                index,
                pending_name,
                column,
                pending_start if pending_start is not None else index,
                base_offset,
            )
            checks.append(check)
            pending_name = ""
            pending_start = None
            continue
        if (
            token.kind == "word"
            and token.text.upper() in _OTHER_COLUMN_CONSTRAINT_KEYWORDS
        ):
            pending_name = ""
            pending_start = None
        index += 1
    return checks


def _table_body(create_sql: str) -> tuple[str, int] | None:
    all_tokens = _lex(create_sql)
    tokens = _meaningful(all_tokens)
    if not tokens or not tokens[0].is_keyword("CREATE"):
        raise ParseError("Expected CREATE TABLE")
    index = 1
    if index < len(tokens) and (
        tokens[index].is_keyword("TEMP") or tokens[index].is_keyword("TEMPORARY")
    ):
        index += 1
    if index < len(tokens) and tokens[index].is_keyword("VIRTUAL"):
        return None
    if index >= len(tokens) or not tokens[index].is_keyword("TABLE"):
        raise ParseError("Expected CREATE TABLE")
    index += 1
    if (
        index + 2 < len(tokens)
        and tokens[index].is_keyword("IF")
        and tokens[index + 1].is_keyword("NOT")
        and tokens[index + 2].is_keyword("EXISTS")
    ):
        index += 3
    if index >= len(tokens):
        raise ParseError("CREATE TABLE is missing its table name")
    index += 1
    if index + 1 < len(tokens) and tokens[index].text == ".":
        index += 2
    if index < len(tokens) and tokens[index].is_keyword("AS"):
        return None
    if index >= len(tokens) or tokens[index].text != "(":
        raise ParseError("CREATE TABLE is missing its column list")
    close = _matching_paren(tokens, index)
    trailing = tokens[close + 1 :]
    allowed_trailing = {"STRICT", "WITHOUT", "ROWID", ",", ";"}
    if any(token.text.upper() not in allowed_trailing for token in trailing):
        raise ParseError("Unexpected SQL after CREATE TABLE column list")

    body_start = tokens[index].end
    body_end = tokens[close].start
    return create_sql[body_start:body_end], body_start


def parse_checks(create_sql: str) -> list[Check]:
    """Return CHECK constraints from a valid SQLite CREATE TABLE statement."""
    body_info = _table_body(create_sql)
    if body_info is None:
        return []
    body, body_start = body_info
    body_tokens = _lex(body)
    checks: list[Check] = []
    for item, item_start, _ in _split_spans(body, body_tokens):
        item_tokens = _meaningful(_lex(item))
        if not item_tokens:
            continue
        item_index = 0
        constraint_name = ""
        if item_tokens[item_index].is_keyword("CONSTRAINT"):
            if len(item_tokens) < 2:
                raise ParseError("CONSTRAINT is missing its name")
            constraint_name = _unquote(item_tokens[1].text)
            item_index = 2
        head = item_tokens[item_index] if item_index < len(item_tokens) else None
        if (
            head
            and head.kind == "word"
            and head.text.upper() in _TABLE_CONSTRAINT_KEYWORDS
        ):
            if head.is_keyword("CHECK"):
                check, _ = _check_after(
                    item,
                    item_tokens,
                    item_index,
                    constraint_name,
                    "",
                    0,
                    body_start + item_start,
                )
                checks.append(check)
            continue
        column = _unquote(item_tokens[0].text)
        checks.extend(
            _column_checks(item, item_tokens, column, body_start + item_start)
        )
    return checks


def parse_column_comments(create_sql: str) -> dict[str, ColumnComments]:
    """Return comments immediately before and after each column definition."""
    body_info = _table_body(create_sql)
    if body_info is None:
        return {}
    body, _ = body_info
    comments: dict[str, ColumnComments] = {}
    for item, _, _ in _split_spans(body, _lex(body)):
        item_tokens = _meaningful(_lex(item))
        if not item_tokens:
            continue
        item_index = 0
        if item_tokens[item_index].is_keyword("CONSTRAINT"):
            item_index = 2
        head = item_tokens[item_index] if item_index < len(item_tokens) else None
        if (
            head
            and head.kind == "word"
            and head.text.upper() in _TABLE_CONSTRAINT_KEYWORDS
        ):
            continue
        column = _unquote(item_tokens[0].text)
        before = item[: item_tokens[0].start].strip()
        after = item[item_tokens[-1].end :].strip()
        if before or after:
            comments[column] = ColumnComments(before=before, after=after)
    return comments


def _is_identifier_token(tokens: list[_Token], index: int) -> bool:
    token = tokens[index]
    if index + 1 < len(tokens) and tokens[index + 1].text in ("(", "."):
        return False
    if index and (
        tokens[index - 1].is_keyword("COLLATE") or tokens[index - 1].is_keyword("AS")
    ):
        return False
    if token.kind == "identifier":
        return True
    if token.kind != "word" or token.text.upper() in _SQLITE_KEYWORDS:
        return False
    return True


def check_references_identifier(expression: str, identifier: str) -> bool:
    tokens = _meaningful(_lex(expression))
    folded = _ascii_fold(identifier)
    return any(
        _is_identifier_token(tokens, index)
        and _ascii_fold(_unquote(token.text)) == folded
        for index, token in enumerate(tokens)
    )


def sql_ends_in_line_comment(sql: str) -> bool:
    """Return True if appended SQL would be swallowed by a ``--`` comment."""
    tokens = _lex(sql)
    if not tokens:
        return False
    final = tokens[-1]
    return (
        final.kind == "comment"
        and final.text.startswith("--")
        and not final.text.endswith(("\n", "\r"))
    )


def _valid_bare_identifier(identifier: str) -> bool:
    if not identifier or identifier.upper() in _SQLITE_KEYWORDS:
        return False
    first = identifier[0]
    if not (first == "_" or first.isalpha() or ord(first) >= 0x80):
        return False
    return all(
        char == "_" or char == "$" or char.isalnum() or ord(char) >= 0x80
        for char in identifier[1:]
    )


def _quote_replacement(original: str, replacement: str) -> str:
    if original.startswith('"'):
        return '"{}"'.format(replacement.replace('"', '""'))
    if original.startswith("`"):
        return "`{}`".format(replacement.replace("`", "``"))
    if original.startswith("[") and "]" not in replacement:
        return f"[{replacement}]"
    if _valid_bare_identifier(replacement):
        return replacement
    return '"{}"'.format(replacement.replace('"', '""'))


def rewrite_check_expression(expression: str, rename: dict[str, str]) -> str:
    """Rewrite column identifiers in a CHECK expression, preserving trivia."""
    if not rename:
        return expression
    tokens = _lex(expression)
    meaningful = _meaningful(tokens)
    replacements = {_ascii_fold(key): value for key, value in rename.items()}
    edits: list[tuple[int, int, str]] = []
    for index, token in enumerate(meaningful):
        if not _is_identifier_token(meaningful, index):
            continue
        replacement = replacements.get(_ascii_fold(_unquote(token.text)))
        if replacement is not None:
            edits.append(
                (token.start, token.end, _quote_replacement(token.text, replacement))
            )
    for start, end, replacement in reversed(edits):
        expression = expression[:start] + replacement + expression[end:]
    return expression
