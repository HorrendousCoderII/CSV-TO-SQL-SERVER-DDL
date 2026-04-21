"""Streamlit UI for CSV → CREATE TABLE (calls schema_infer core only)."""

from __future__ import annotations

import streamlit as st

from schema_infer import infer_dataframe_schema, load_csv, render_create_table

st.set_page_config(page_title="CSV → CREATE TABLE", layout="wide")

st.title("CSV → CREATE TABLE")
st.caption("Infer column types from a CSV and generate DDL (Polars + core inference).")

uploaded = st.file_uploader("Upload a CSV file please", type=["csv"])

dialect_label = st.selectbox(
    "SQL dialect",
    options=("postgresql", "sqlite", "sqlserver"),
    format_func=lambda x: (
        "PostgreSQL"
        if x == "postgresql"
        else "SQLite" if x == "sqlite" else "SQL Server"
    ),
)
dialect = dialect_label

default_table = "imported_table"
if uploaded is not None:
    name = uploaded.name.rsplit(".", 1)[0]
    default_table = "".join(c if c.isalnum() or c in "._-" else "_" for c in name) or "imported_table"

table_name = st.text_input("Table name", value=default_table)

preview_n = st.number_input("Preview rows", min_value=1, max_value=500, value=20, step=1)

nullability = st.radio(
    "Nullability",
    options=("safe", "strict"),
    format_func=lambda x: (
        "Safe — every column allows NULL (recommended for messy CSVs)"
        if x == "safe"
        else "Strict — NOT NULL only when every row has a value in that column"
    ),
    horizontal=True,
)

if_not_exists = st.checkbox("CREATE TABLE IF NOT EXISTS", value=False)

if uploaded is None:
    st.info("Upload a CSV to generate DDL.")
else:
    try:
        df = load_csv(uploaded.getvalue())
        cols = infer_dataframe_schema(df)
        sql = render_create_table(
            table_name,
            cols,
            dialect,
            if_not_exists=if_not_exists,
            nullable_mode="safe" if nullability == "safe" else "strict",
        )

        st.subheader("Preview")
        st.dataframe(df.head(int(preview_n)), use_container_width=True)

        st.subheader("Generated SQL")
        st.code(sql, language="sql")
        cap = "Copy the SQL above and run it in your client (adjust types if needed)."
        if dialect == "sqlserver":
            cap += " SQL Server: `CREATE TABLE IF NOT EXISTS` requires SQL Server 2016+."
        st.caption(cap)
    except Exception as e:
        st.error(f"Failed to process CSV: {e}")
