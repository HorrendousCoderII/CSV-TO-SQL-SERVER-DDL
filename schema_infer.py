"""
Core CSV schema inference and CREATE TABLE generation.
Designed for reuse by a future CLI.
"""
from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import BinaryIO, Literal, Union

import polars as pl

# --- Constants ---

PG_VARCHAR_MAX = 10485760

SQLITE_RESERVED = {
    "abort", "action", "add", "after", "all", "alter", "analyze", "and", "as", "asc",
    "attach", "autoincrement", "before", "begin", "between", "by", "cascade", "case",
    "cast", "check", "collate", "column", "commit", "conflict", "constraint", "create",
    "cross", "current", "current_date", "current_time", "current_timestamp", "database",
    "default", "deferrable", "deferred", "delete", "desc", "detach", "distinct", "drop",
    "each", "else", "end", "escape", "except", "exclusive", "exists", "explain", "fail",
    "filter", "first", "following", "for", "foreign", "from", "full", "glob", "group",
    "having", "if", "ignore", "immediate", "in", "index", "indexed", "initially", "inner",
    "insert", "instead", "intersect", "into", "is", "isnull", "join", "key", "last",
    "left", "like", "limit", "match", "natural", "no", "not", "nothing", "notnull", "null",
    "of", "offset", "on", "or", "order", "others", "outer", "over", "partition", "plan",
    "pragma", "preceding", "primary", "query", "raise", "range", "recursive", "references",
    "regexp", "reindex", "release", "rename", "replace", "restrict", "returning", "right",
    "rollback", "row", "rows", "savepoint", "select", "set", "table", "temp", "temporary",
    "then", "ties", "to", "transaction", "trigger", "unbounded", "union", "unique", "update",
    "using", "vacuum", "values", "view", "virtual", "when", "where", "window", "with",
    "without",
}

# Common T-SQL reserved keywords (lowercase); bracket-quote when matched.
MSSQL_RESERVED = {
    "add", "all", "alter", "and", "any", "as", "asc", "authorization", "backup",
    "begin", "between", "break", "browse", "bulk", "by", "cascade", "case", "check",
    "checkpoint", "close", "clustered", "coalesce", "collate", "column", "commit",
    "constraint", "contains", "continue", "convert", "create", "cross", "current",
    "cursor", "database", "dbcc", "deallocate", "declare", "default", "delete",
    "deny", "desc", "disk", "distinct", "distributed", "drop", "else", "end", "errlvl",
    "escape", "except", "exec", "execute", "exists", "exit", "fetch", "file",
    "for", "foreign", "from", "full", "function", "goto", "grant", "group", "having",
    "holdlock", "identity", "if", "in", "index", "inner", "insert", "intersect", "into",
    "is", "join", "key", "kill", "left", "like", "lineno", "load", "merge", "national",
    "nocheck", "nonclustered", "not", "null", "nullif", "of", "off", "offsets", "on",
    "open", "option", "or", "order", "outer", "over", "percent", "pivot", "plan",
    "precision", "primary", "print", "proc", "procedure", "public", "raiserror", "read",
    "readtext", "references", "replication", "restore", "restrict", "return", "revert",
    "revoke", "right", "rollback", "row", "rowguidcol", "rule", "save", "schema", "select",
    "session_user", "set", "setuser", "shutdown", "some", "statistics", "system_user",
    "table", "textsize", "then", "to", "top", "tran", "transaction", "trigger", "truncate",
    "tsequal", "union", "unique", "unpivot", "update", "updatetext", "use", "user",
    "values", "varying", "view", "waitfor", "when", "where", "while", "with", "writetext",
}

PG_RESERVED = {
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
    "authorization", "binary", "both", "case", "cast", "check", "collate", "column",
    "concurrently", "constraint", "create", "cross", "current_catalog", "current_date",
    "current_role", "current_schema", "current_time", "current_timestamp", "current_user",
    "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false",
    "fetch", "for", "foreign", "freeze", "from", "full", "grant", "group", "having",
    "ilike", "in", "initially", "inner", "intersect", "into", "is", "isnull", "join",
    "lateral", "leading", "left", "like", "limit", "localtime", "localtimestamp", "natural",
    "not", "notnull", "null", "offset", "on", "only", "or", "order", "outer", "overlaps",
    "placing", "primary", "references", "returning", "right", "select", "session_user",
    "similar", "some", "symmetric", "table", "tablesample", "then", "to", "trailing", "true",
    "union", "unique", "user", "using", "variadic", "verbose", "when", "where", "window",
    "with",
}

BOOL_TOKENS = {
    "true", "false", "1", "0", "yes", "no", "y", "n", "t", "f",
}


class InferredKind(str, Enum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    BIGINT = "bigint"
    NUMERIC = "numeric"
    DATE = "date"
    TIMESTAMP = "timestamp"
    VARCHAR = "varchar"


@dataclass(frozen=True)
class ColumnInference:
    """Result of inferring one column from string values."""

    original_name: str
    sql_name: str
    kind: InferredKind
    nullable: bool
    max_varchar_len: int | None
    numeric_precision: int | None
    numeric_scale: int | None


Dialect = Literal["postgresql", "sqlite", "sqlserver"]

# NVARCHAR(n) max n before SQL Server requires MAX
MSSQL_NVARCHAR_INLINE_MAX = 4000

# DECIMAL precision cap in SQL Server
MSSQL_DECIMAL_MAX_PRECISION = 38


def load_csv(
    source: Union[str, bytes, BinaryIO],
    *,
    separator: str = ",",
) -> pl.DataFrame:
    """
    Load CSV with a full scan, then cast every column to Utf8 so mixed-type columns
    are preserved as their textual representation for inference.

    ``separator`` must be exactly one character (e.g. ``,``, ``;``, ``\\t``, ``|``).
    """
    if len(separator) != 1:
        raise ValueError("separator must be exactly one character")
    if isinstance(source, bytes):
        source = io.BytesIO(source)
    df = pl.read_csv(
        source,
        separator=separator,
        try_parse_dates=False,
        infer_schema_length=None,
    )
    return df.select(pl.all().cast(pl.Utf8))


def _non_empty_strings(series: pl.Series) -> list[str]:
    out: list[str] = []
    for v in series.to_list():
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    return out


def _all_bool(values: list[str]) -> bool:
    if not values:
        return False
    for v in values:
        if v.lower() not in BOOL_TOKENS:
            return False
    return True


def _all_int(values: list[str]) -> bool:
    for v in values:
        try:
            int(v, 10)
        except ValueError:
            return False
    return True


def _int_fits_32(s: str) -> bool:
    n = int(s, 10)
    return -(2**31) <= n <= (2**31 - 1)


def _all_int32(values: list[str]) -> bool:
    for v in values:
        if not _int_fits_32(v):
            return False
    return True


def _all_decimal(values: list[str]) -> bool:
    for v in values:
        try:
            Decimal(v)
        except (InvalidOperation, ValueError):
            return False
    return True


def _numeric_precision_scale(values: list[str]) -> tuple[int, int]:
    max_prec = 0
    max_scale = 0
    for v in values:
        d = Decimal(v)
        sign, digits, exp = d.as_tuple()
        if exp == "F":
            continue
        int_digits = len(digits) + (exp if exp < 0 else 0)
        frac_digits = -exp if exp < 0 else 0
        prec = int_digits + frac_digits
        scale = frac_digits
        max_prec = max(max_prec, prec)
        max_scale = max(max_scale, scale)
    # NUMERIC(p,s): precision includes scale
    p = max(max_prec, 1)
    s = min(max_scale, p)
    return p, s


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
)


def _try_parse_datetime(s: str) -> datetime | None:
    s = s.strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        d = date.fromisoformat(s)
        return datetime(d.year, d.month, d.day)
    except ValueError:
        return None


def _infer_date_vs_timestamp(values: list[str]) -> tuple[bool, bool]:
    """Returns (all_parseable, any_has_time)."""
    any_time = False
    for v in values:
        dt = _try_parse_datetime(v)
        if dt is None:
            return False, False
        if dt.hour or dt.minute or dt.second or dt.microsecond:
            any_time = True
    return True, any_time


def infer_column(
    series: pl.Series,
    original_name: str,
    sql_name: str,
) -> ColumnInference:
    """
    Infer SQL-oriented column metadata from a Utf8 Polars series.
    Order: integer -> numeric -> boolean -> date/time -> varchar.

    Integers are checked before booleans so values like ``1`` / ``2`` are not
    classified as boolean via ``1``/``0`` tokens.

    ``nullable`` is True iff some row is NULL/empty (used when rendering strict NOT NULL).
    """
    values = _non_empty_strings(series)
    nullable = len(values) < len(series)

    if not values:
        max_len = _max_string_len(series)
        return ColumnInference(
            original_name=original_name,
            sql_name=sql_name,
            kind=InferredKind.VARCHAR,
            nullable=nullable,
            max_varchar_len=max(1, max_len),
            numeric_precision=None,
            numeric_scale=None,
        )

    if _all_int(values):
        kind = InferredKind.INTEGER if _all_int32(values) else InferredKind.BIGINT
        return ColumnInference(
            original_name=original_name,
            sql_name=sql_name,
            kind=kind,
            nullable=nullable,
            max_varchar_len=None,
            numeric_precision=None,
            numeric_scale=None,
        )

    if _all_bool(values):
        return ColumnInference(
            original_name=original_name,
            sql_name=sql_name,
            kind=InferredKind.BOOLEAN,
            nullable=nullable,
            max_varchar_len=None,
            numeric_precision=None,
            numeric_scale=None,
        )

    if _all_decimal(values):
        p, s = _numeric_precision_scale(values)
        return ColumnInference(
            original_name=original_name,
            sql_name=sql_name,
            kind=InferredKind.NUMERIC,
            nullable=nullable,
            max_varchar_len=None,
            numeric_precision=p,
            numeric_scale=s,
        )

    ok_date, any_time = _infer_date_vs_timestamp(values)
    if ok_date:
        kind = InferredKind.TIMESTAMP if any_time else InferredKind.DATE
        return ColumnInference(
            original_name=original_name,
            sql_name=sql_name,
            kind=kind,
            nullable=nullable,
            max_varchar_len=None,
            numeric_precision=None,
            numeric_scale=None,
        )

    max_len = _max_string_len(series)
    return ColumnInference(
        original_name=original_name,
        sql_name=sql_name,
        kind=InferredKind.VARCHAR,
        nullable=nullable,
        max_varchar_len=max(1, max_len),
        numeric_precision=None,
        numeric_scale=None,
    )


def _max_string_len(series: pl.Series) -> int:
    stripped = series.cast(pl.Utf8).str.strip_chars()
    lengths = stripped.str.len_chars()
    m = lengths.max()
    if m is None:
        return 1
    return int(m)


def sanitize_base_name(name: str) -> str:
    s = name.strip()
    if not s:
        s = "column"
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "column"
    if s[0].isdigit():
        s = "col_" + s
    return s.lower()


def unique_sql_names(base_names: list[str]) -> list[str]:
    used: set[str] = set()
    out: list[str] = []
    for b in base_names:
        name = b
        n = 2
        while name in used:
            name = f"{b}_{n}"
            n += 1
        used.add(name)
        out.append(name)
    return out


def sanitize_column_names(headers: list[str]) -> list[str]:
    bases = [sanitize_base_name(h) for h in headers]
    return unique_sql_names(bases)


def quote_identifier(name: str, dialect: Dialect) -> str:
    """Quote if not valid unquoted identifier or reserved."""
    if dialect == "postgresql":
        reserved = PG_RESERVED
    elif dialect == "sqlite":
        reserved = SQLITE_RESERVED
    else:
        reserved = MSSQL_RESERVED

    safe = re.match(r"^[a-z_][a-z0-9_]*$", name) is not None
    key = name.lower()

    if dialect == "sqlserver":
        if safe and key not in reserved:
            return name
        escaped = name.replace("]", "]]")
        return f"[{escaped}]"

    if safe and key not in reserved:
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def infer_dataframe_schema(df: pl.DataFrame) -> list[ColumnInference]:
    headers = df.columns
    sql_names = sanitize_column_names(headers)
    cols: list[ColumnInference] = []
    for h, sql_n in zip(headers, sql_names):
        s = df[h]
        cols.append(infer_column(s, h, sql_n))
    return cols


def _sql_type_postgresql(col: ColumnInference) -> str:
    k = col.kind
    if k == InferredKind.BOOLEAN:
        return "BOOLEAN"
    if k == InferredKind.INTEGER:
        return "INTEGER"
    if k == InferredKind.BIGINT:
        return "BIGINT"
    if k == InferredKind.NUMERIC:
        p = col.numeric_precision or 18
        s = col.numeric_scale or 0
        return f"NUMERIC({p},{s})"
    if k == InferredKind.DATE:
        return "DATE"
    if k == InferredKind.TIMESTAMP:
        return "TIMESTAMP"
    if k == InferredKind.VARCHAR:
        n = col.max_varchar_len or 1
        if n > PG_VARCHAR_MAX:
            return "TEXT"
        return f"VARCHAR({n})"
    raise ValueError(k)


def _sql_type_sqlserver(col: ColumnInference) -> str:
    k = col.kind
    if k == InferredKind.BOOLEAN:
        return "BIT"
    if k == InferredKind.INTEGER:
        return "INT"
    if k == InferredKind.BIGINT:
        return "BIGINT"
    if k == InferredKind.NUMERIC:
        p = min(col.numeric_precision or 18, MSSQL_DECIMAL_MAX_PRECISION)
        s = min(col.numeric_scale or 0, p)
        return f"DECIMAL({p},{s})"
    if k == InferredKind.DATE:
        return "DATE"
    if k == InferredKind.TIMESTAMP:
        return "DATETIME2(7)"
    if k == InferredKind.VARCHAR:
        n = col.max_varchar_len or 1
        if n > MSSQL_NVARCHAR_INLINE_MAX:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({n})"
    raise ValueError(k)


def _sql_type_sqlite(col: ColumnInference) -> str:
    k = col.kind
    if k == InferredKind.BOOLEAN:
        return "INTEGER"
    if k in (InferredKind.INTEGER, InferredKind.BIGINT):
        return "INTEGER"
    if k == InferredKind.NUMERIC:
        p = col.numeric_precision or 18
        s = col.numeric_scale or 0
        return f"NUMERIC({p},{s})"
    if k == InferredKind.DATE:
        return "TEXT"
    if k == InferredKind.TIMESTAMP:
        return "TEXT"
    if k == InferredKind.VARCHAR:
        return "TEXT"
    raise ValueError(k)


TRUE_BOOL_TOKENS = frozenset({"true", "1", "yes", "y", "t"})
FALSE_BOOL_TOKENS = frozenset({"false", "0", "no", "n", "f"})


def resolve_table_base_name(table_name: str) -> str:
    """Sanitized table name used in DDL/INSERT and for suggested download filenames."""
    t = sanitize_base_name(table_name)
    if not t or t == "column":
        t = "imported_table"
    return t


def _quoted_table_name(table_name: str, dialect: Dialect) -> str:
    return quote_identifier(resolve_table_base_name(table_name), dialect)


def _sql_char_literal(text: str, dialect: Dialect) -> str:
    esc = text.replace("'", "''")
    if dialect == "sqlserver":
        return f"N'{esc}'"
    return f"'{esc}'"


def _is_null_cell(raw: object) -> bool:
    if raw is None:
        return True
    if isinstance(raw, str) and not raw.strip():
        return True
    return False


def _format_insert_value(raw: object, col: ColumnInference, dialect: Dialect) -> str:
    if _is_null_cell(raw):
        return "NULL"
    s = str(raw).strip()

    k = col.kind
    if k == InferredKind.BOOLEAN:
        v = s.lower()
        if v in TRUE_BOOL_TOKENS:
            if dialect == "postgresql":
                return "TRUE"
            return "1"
        if v in FALSE_BOOL_TOKENS:
            if dialect == "postgresql":
                return "FALSE"
            return "0"
        return "NULL"

    if k in (InferredKind.INTEGER, InferredKind.BIGINT):
        try:
            return str(int(s, 10))
        except ValueError:
            return "NULL"

    if k == InferredKind.NUMERIC:
        try:
            d = Decimal(s)
            return format(d, "f")
        except (InvalidOperation, ValueError):
            return "NULL"

    if k == InferredKind.DATE:
        dt = _try_parse_datetime(s)
        if dt is None:
            return "NULL"
        return _sql_char_literal(dt.date().isoformat(), dialect)

    if k == InferredKind.TIMESTAMP:
        dt = _try_parse_datetime(s)
        if dt is None:
            return "NULL"
        if dt.microsecond:
            inner = dt.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        else:
            inner = dt.strftime("%Y-%m-%d %H:%M:%S")
        return _sql_char_literal(inner, dialect)

    if k == InferredKind.VARCHAR:
        return _sql_char_literal(s, dialect)

    raise ValueError(k)


def render_insert_statements(
    df: pl.DataFrame,
    columns: list[ColumnInference],
    dialect: Dialect,
    *,
    table_name: str,
    max_rows: int | None = None,
) -> str:
    """
    Emit one INSERT per row. ``columns`` must match ``infer_dataframe_schema(df)`` pairing
    (``original_name`` ↔ dataframe column).
    """
    table_sql = _quoted_table_name(table_name, dialect)
    work = df if max_rows is None else df.head(max_rows)

    col_idents = [quote_identifier(c.sql_name, dialect) for c in columns]
    cols_sql = ", ".join(col_idents)

    lines: list[str] = []
    for row_idx in range(work.height):
        parts: list[str] = []
        for col in columns:
            series = work[col.original_name]
            raw = series[row_idx]
            parts.append(_format_insert_value(raw, col, dialect))
        vals_sql = ", ".join(parts)
        lines.append(f"INSERT INTO {table_sql} ({cols_sql}) VALUES ({vals_sql});")

    return "\n".join(lines)


def render_create_table(
    table_name: str,
    columns: list[ColumnInference],
    dialect: Dialect,
    *,
    if_not_exists: bool = False,
    nullable_mode: Literal["safe", "strict"] = "safe",
    primary_key: list[str] | None = None,
) -> str:
    """
    nullable_mode:
      - safe: all columns allow NULL (safest for ad-hoc imports).
      - strict: NOT NULL on columns with no empty values; NULL allowed otherwise.

    primary_key: optional list of ``sql_name`` values. Those columns are always ``NOT NULL``
    (required for a valid PRIMARY KEY). Table-level ``PRIMARY KEY (...)`` is appended.
    """
    sql_names = {c.sql_name for c in columns}
    pk_tuple: tuple[str, ...] = ()
    if primary_key is not None:
        if not primary_key:
            raise ValueError("primary_key must not be empty when provided")
        if len(primary_key) != len(set(primary_key)):
            raise ValueError("duplicate column in primary_key")
        for name in primary_key:
            if name not in sql_names:
                raise ValueError(f"unknown primary key column: {name!r}")
        pk_tuple = tuple(primary_key)

    pk_set = set(pk_tuple)
    table_sql = _quoted_table_name(table_name, dialect)

    lines: list[str] = []
    if if_not_exists:
        lines.append(f"CREATE TABLE IF NOT EXISTS {table_sql} (")
    else:
        lines.append(f"CREATE TABLE {table_sql} (")

    parts: list[str] = []
    for col in columns:
        ident = quote_identifier(col.sql_name, dialect)
        if dialect == "postgresql":
            typ = _sql_type_postgresql(col)
        elif dialect == "sqlserver":
            typ = _sql_type_sqlserver(col)
        else:
            typ = _sql_type_sqlite(col)

        if col.sql_name in pk_set:
            null_sql = "NOT NULL"
        elif nullable_mode == "safe":
            null_sql = "NULL"
        else:
            null_sql = "NULL" if col.nullable else "NOT NULL"
        parts.append(f"    {ident} {typ} {null_sql}")

    if pk_tuple:
        pk_list = ", ".join(quote_identifier(n, dialect) for n in pk_tuple)
        parts.append(f"    PRIMARY KEY ({pk_list})")

    lines.append(",\n".join(parts))
    lines.append(");")
    return "\n".join(lines)


def render_create_indexes(
    table_name: str,
    columns: list[ColumnInference],
    dialect: Dialect,
    index_specs: list[list[str]],
    *,
    if_not_exists: bool = True,
    primary_key: list[str] | None = None,
) -> str:
    """
    Emit ``CREATE INDEX`` statements after ``CREATE TABLE``. Each inner list of
    ``index_specs`` is a composite index over ``sql_name`` column names (order matters).

    For PostgreSQL and SQLite, ``IF NOT EXISTS`` is included when ``if_not_exists`` is True.
    SQL Server does not use ``IF NOT EXISTS`` on ``CREATE INDEX`` in this generator.

    If ``primary_key`` is set, an index whose column list matches the primary key
    (same order) is omitted as redundant.
    """
    if not index_specs:
        return ""

    sql_names = {c.sql_name for c in columns}
    pk_order = tuple(primary_key) if primary_key else None

    statements: list[str] = []
    used_names: set[str] = set()

    for spec in index_specs:
        if not spec:
            raise ValueError("each index must include at least one column")
        if len(spec) != len(set(spec)):
            raise ValueError("duplicate column in index specification")
        for name in spec:
            if name not in sql_names:
                raise ValueError(f"unknown index column: {name!r}")
        if pk_order is not None and tuple(spec) == pk_order:
            continue

        base_raw = "idx_" + resolve_table_base_name(table_name) + "_" + "_".join(spec)
        base_name = sanitize_base_name(base_raw)
        if len(base_name) > 110:
            base_name = sanitize_base_name(base_name[:110])
        idx_name = base_name
        n = 2
        while idx_name in used_names:
            idx_name = f"{base_name}_{n}"
            n += 1
        used_names.add(idx_name)

        idx_ident = quote_identifier(idx_name, dialect)
        table_sql = _quoted_table_name(table_name, dialect)
        col_list = ", ".join(quote_identifier(cn, dialect) for cn in spec)

        if dialect == "sqlserver":
            statements.append(f"CREATE INDEX {idx_ident} ON {table_sql} ({col_list});")
        elif if_not_exists:
            statements.append(
                f"CREATE INDEX IF NOT EXISTS {idx_ident} ON {table_sql} ({col_list});"
            )
        else:
            statements.append(f"CREATE INDEX {idx_ident} ON {table_sql} ({col_list});")

    return "\n".join(statements)
