"""Tests for schema_infer core (load, inference, DDL)."""
from __future__ import annotations

import io

import polars as pl

from schema_infer import (
    InferredKind,
    infer_column,
    infer_dataframe_schema,
    load_csv,
    render_create_table,
    sanitize_column_names,
    unique_sql_names,
)


def test_load_csv_casts_all_utf8():
    csv = b"a,b\n1,hello\n3.5,x\n"
    df = load_csv(io.BytesIO(csv))
    assert df.dtypes == [pl.Utf8, pl.Utf8]
    assert df.height == 2


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
