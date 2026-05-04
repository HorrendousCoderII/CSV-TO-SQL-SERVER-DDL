"""Tests for schema_infer core (load, inference, DDL)."""
from __future__ import annotations

import io

import polars as pl
import pytest

from schema_infer import (
    InferredKind,
    infer_column,
    infer_dataframe_schema,
    load_csv,
    render_create_indexes,
    render_create_table,
    render_insert_statements,
    sanitize_column_names,
    unique_sql_names,
)


def test_load_csv_casts_all_utf8():
    csv = b"a,b\n1,hello\n3.5,x\n"
    df = load_csv(io.BytesIO(csv))
    assert df.dtypes == [pl.Utf8, pl.Utf8]
    assert df.height == 2


def test_load_csv_tab_separator():
    csv = b"a\tb\n1\tx\n"
    df = load_csv(io.BytesIO(csv), separator="\t")
    assert list(df.columns) == ["a", "b"]
    assert df.height == 1


def test_load_csv_semicolon_separator():
    csv = b"a;b\n1;2\n"
    df = load_csv(io.BytesIO(csv), separator=";")
    assert list(df.columns) == ["a", "b"]


def test_load_csv_invalid_separator_length():
    with pytest.raises(ValueError, match="exactly one character"):
        load_csv(io.BytesIO(b"a\n1\n"), separator=",,")


def test_infer_integer_column():
    s = pl.Series("a", ["1", "2", "3"])
    col = infer_column(s, "a", "a")
    assert col.kind == InferredKind.INTEGER
    assert col.nullable is False


def test_infer_bigint():
    s = pl.Series("a", [str(2**31), str(2**31 + 1)])
    col = infer_column(s, "a", "a")
    assert col.kind == InferredKind.BIGINT


def test_infer_numeric():
    s = pl.Series("a", ["1.5", "2.25", "0"])
    col = infer_column(s, "a", "a")
    assert col.kind == InferredKind.NUMERIC
    assert col.numeric_precision is not None


def test_mixed_int_and_string_becomes_varchar():
    s = pl.Series("a", ["1", "2", "oops"])
    col = infer_column(s, "a", "a")
    assert col.kind == InferredKind.VARCHAR
    assert col.max_varchar_len >= 4


def test_max_varchar_length():
    s = pl.Series("a", ["a", "bb", "ccc"])
    col = infer_column(s, "a", "a")
    assert col.kind == InferredKind.VARCHAR
    assert col.max_varchar_len == 3


def test_sanitize_duplicate_headers():
    names = sanitize_column_names(["Name", "name", "NAME"])
    assert names[0] == "name"
    assert names[1] == "name_2"
    assert names[2] == "name_3"


def test_unique_sql_names_collision():
    assert unique_sql_names(["a", "a", "a"]) == ["a", "a_2", "a_3"]


def test_render_postgresql_create_table():
    csv = b"id,name\n1,Ann\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("people", cols, "postgresql", nullable_mode="safe")
    assert "CREATE TABLE" in sql
    assert "people" in sql.lower() or '"people"' in sql
    assert "INTEGER" in sql or "int" in sql.lower()


def test_render_strict_not_null():
    csv = b"x\n1\n2\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("t", cols, "postgresql", nullable_mode="strict")
    assert "NOT NULL" in sql


def test_if_not_exists():
    csv = b"x\n1\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("t", cols, "postgresql", if_not_exists=True)
    assert "IF NOT EXISTS" in sql


def test_primary_key_constraint_and_not_null_in_safe_mode():
    csv = b"id,val\n1,a\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table(
        "t",
        cols,
        "postgresql",
        nullable_mode="safe",
        primary_key=["id"],
    )
    assert "PRIMARY KEY" in sql
    assert "NOT NULL" in sql


def test_primary_key_unknown_column_raises():
    csv = b"a\n1\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    with pytest.raises(ValueError, match="unknown primary key"):
        render_create_table("t", cols, "postgresql", primary_key=["nope"])


def test_render_create_indexes_postgresql_if_not_exists():
    csv = b"id,name\n1,Ann\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_indexes(
        "people",
        cols,
        "postgresql",
        [["name"]],
        if_not_exists=True,
    )
    assert "CREATE INDEX IF NOT EXISTS" in sql
    assert "ON" in sql


def test_render_create_indexes_sqlite_if_not_exists():
    csv = b"id,name\n1,Ann\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_indexes(
        "people",
        cols,
        "sqlite",
        [["name"]],
        if_not_exists=True,
    )
    assert "CREATE INDEX IF NOT EXISTS" in sql


def test_render_create_indexes_sqlserver_no_if_not_exists():
    csv = b"id,name\n1,Ann\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_indexes(
        "people",
        cols,
        "sqlserver",
        [["name"]],
        if_not_exists=True,
    )
    assert "IF NOT EXISTS" not in sql
    assert "CREATE INDEX" in sql
    assert "[name]" in sql or "name" in sql


def test_render_create_indexes_skips_redundant_primary_key():
    csv = b"id\n1\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_indexes(
        "t",
        cols,
        "postgresql",
        [["id"]],
        primary_key=["id"],
    )
    assert sql == ""


def test_sqlite_uses_text_for_strings():
    csv = b"desc\nhello\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("t", cols, "sqlite", nullable_mode="safe")
    assert "TEXT" in sql


def test_sqlserver_uses_nvarchar_and_int():
    csv = b"id,name\n1,Ann\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("people", cols, "sqlserver", nullable_mode="safe")
    assert "CREATE TABLE" in sql
    assert "NVARCHAR" in sql
    assert "INT" in sql


def test_sqlserver_boolean_bit():
    csv = b"flag\ntrue\nfalse\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("b", cols, "sqlserver", nullable_mode="safe")
    assert "BIT" in sql


def test_sqlserver_brackets_reserved_column():
    csv = b"order\n1\n2\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("t", cols, "sqlserver", nullable_mode="safe")
    assert "[order]" in sql.lower() or "[order]" in sql


def test_sqlserver_nvarchar_max_over_4000():
    long_s = "x" * 5000
    csv = "txt\n" + long_s + "\n"
    df = load_csv(io.BytesIO(csv.encode("utf-8")))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("wide", cols, "sqlserver", nullable_mode="safe")
    assert "NVARCHAR(MAX)" in sql


def test_sqlserver_if_not_exists():
    csv = b"x\n1\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    sql = render_create_table("t", cols, "sqlserver", if_not_exists=True)
    assert "IF NOT EXISTS" in sql


def test_insert_postgresql_basic():
    csv = b"id,name\n1,Ann\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    ins = render_insert_statements(df, cols, "postgresql", table_name="people")
    assert "INSERT INTO" in ins
    assert "VALUES (1, 'Ann')" in ins


def test_insert_sqlite_basic():
    csv = b"a,b\n3,hello\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    ins = render_insert_statements(df, cols, "sqlite", table_name="t")
    assert "INSERT INTO" in ins
    assert "VALUES (3, 'hello')" in ins


def test_insert_sqlserver_reserved_column_quoted():
    csv = b"order\n1\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    ins = render_insert_statements(df, cols, "sqlserver", table_name="t")
    assert "[order]" in ins
    assert "VALUES (1)" in ins


def test_insert_apostrophe_escaped_sqlserver():
    csv = b"nm\nO'Brien\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    ins = render_insert_statements(df, cols, "sqlserver", table_name="t")
    assert "N'O''Brien'" in ins


def test_insert_max_rows():
    csv = b"x\n1\n2\n3\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    ins = render_insert_statements(df, cols, "postgresql", table_name="t", max_rows=2)
    assert ins.count("INSERT INTO") == 2


def test_insert_null_empty_cell():
    csv = b"a,b\n1,\n"
    df = load_csv(io.BytesIO(csv))
    cols = infer_dataframe_schema(df)
    ins = render_insert_statements(df, cols, "postgresql", table_name="t")
    assert "VALUES (1, NULL)" in ins
