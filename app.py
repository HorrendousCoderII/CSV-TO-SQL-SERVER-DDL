"""Streamlit UI for CSV → CREATE TABLE (calls schema_infer core only)."""

from __future__ import annotations

import html
import json

import streamlit as st
import streamlit.components.v1 as components

from schema_infer import (
    infer_dataframe_schema,
    load_csv,
    render_create_table,
    render_insert_statements,
    resolve_table_base_name,
)


def _clipboard_copy_button(label: str, text: str, *, height: int = 52) -> None:
    payload = json.dumps(text)
    safe_label = html.escape(label)
    components.html(
        f"""
        <div>
            <button type="button" onclick="navigator.clipboard.writeText({payload})"
                style="padding:0.35rem 0.75rem;border-radius:0.35rem;cursor:pointer;">
                {safe_label}
            </button>
        </div>
        """,
        height=height,
    )


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

gen_inserts = st.checkbox("Generate INSERT statements", value=False)
insert_max_rows = st.number_input(
    "Max rows for INSERT",
    min_value=1,
    max_value=10000,
    value=500,
    step=1,
    disabled=not gen_inserts,
)

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

        safe_base = resolve_table_base_name(table_name)

        st.subheader("Preview")
        st.dataframe(df.head(int(preview_n)), use_container_width=True)

        st.subheader("Generated SQL")
        ddl_actions, ddl_dl = st.columns([1, 1])
        with ddl_actions:
            _clipboard_copy_button("Copy DDL", sql)
        with ddl_dl:
            st.download_button(
                label="Download DDL",
                data=sql.encode("utf-8"),
                file_name=f"{safe_base}.sql",
                mime="text/plain",
                key="download_ddl",
                use_container_width=True,
            )

        st.code(sql, language="sql")

        insert_sql = ""
        if gen_inserts:
            insert_sql = render_insert_statements(
                df,
                cols,
                dialect,
                table_name=table_name,
                max_rows=int(insert_max_rows),
            )
            st.subheader("INSERT statements")
            ins_copy, ins_dl, ins_full = st.columns([1, 1, 1])
            with ins_copy:
                _clipboard_copy_button("Copy INSERTs", insert_sql)
            with ins_dl:
                st.download_button(
                    label="Download INSERTs",
                    data=insert_sql.encode("utf-8"),
                    file_name=f"{safe_base}_inserts.sql",
                    mime="text/plain",
                    key="download_inserts",
                    use_container_width=True,
                )
            with ins_full:
                combined = f"{sql}\n\n{insert_sql}"
                st.download_button(
                    label="Download DDL + INSERTs",
                    data=combined.encode("utf-8"),
                    file_name=f"{safe_base}_full.sql",
                    mime="text/plain",
                    key="download_full",
                    use_container_width=True,
                )
            st.code(insert_sql, language="sql")

        cap = "Copy or download the SQL above. Clipboard works in supported browsers (HTTPS or localhost)."
        if dialect == "sqlserver":
            cap += " SQL Server: `CREATE TABLE IF NOT EXISTS` requires SQL Server 2016+."
        st.caption(cap)
    except Exception as e:
        st.error(f"Failed to process CSV: {e}")
