"""Streamlit UI for CSV → CREATE TABLE (calls schema_infer core only)."""

from __future__ import annotations

import html
import json

import streamlit as st
import streamlit.components.v1 as components

from schema_infer import (
    infer_dataframe_schema,
    load_csv,
    render_create_indexes,
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
    _DELIMS = {
        "Comma (,)": ",",
        "Semicolon (;)": ";",
        "Tab": "\t",
        "Pipe (|)": "|",
        "Custom": "__custom__",
    }
    delim_label = st.selectbox("Delimiter", options=list(_DELIMS.keys()), index=0)
    if _DELIMS[delim_label] == "__custom__":
        custom_d = st.text_input(
            "Custom delimiter (exactly one character)",
            max_chars=4,
            help="Paste or type a single character (e.g. tab).",
        )
        custom_d = (custom_d or "").strip()
        if len(custom_d) == 1:
            sep = custom_d
        elif custom_d == "":
            sep = ","
            st.caption("Empty custom delimiter defaults to comma.")
        else:
            sep = ","
            st.warning("Custom delimiter must be one character; using comma.")
    else:
        sep = _DELIMS[delim_label]

    try:
        df = load_csv(uploaded.getvalue(), separator=sep)
        cols = infer_dataframe_schema(df)

        parse_key = (uploaded.name, uploaded.size, sep)
        if st.session_state.get("_parse_key") != parse_key:
            st.session_state._parse_key = parse_key
            st.session_state.n_indexes = 0
            for k in ("pk_headers",):
                st.session_state.pop(k, None)
            for k in list(st.session_state.keys()):
                if isinstance(k, str) and k.startswith("ix_"):
                    st.session_state.pop(k, None)

        by_original = {c.original_name: c.sql_name for c in cols}

        st.subheader("Primary key")
        pk_enabled = st.checkbox("Define PRIMARY KEY", value=False, key="pk_enabled")
        pk_headers = st.multiselect(
            "PRIMARY KEY columns (order is preserved for composite keys)",
            options=list(df.columns),
            default=[],
            disabled=not pk_enabled,
            key="pk_headers",
        )

        st.subheader("Indexes")
        idx_row1, idx_row2 = st.columns(2)
        with idx_row1:
            if st.button("Add index", key="add_idx"):
                st.session_state.n_indexes = int(st.session_state.get("n_indexes", 0)) + 1
        with idx_row2:
            if st.button("Remove last index", key="rem_idx"):
                n = int(st.session_state.get("n_indexes", 0))
                if n > 0:
                    st.session_state.pop(f"ix_{n - 1}", None)
                    st.session_state.n_indexes = n - 1

        n_ix = int(st.session_state.get("n_indexes", 0))
        for i in range(n_ix):
            st.multiselect(
                f"Index {i + 1} columns (order preserved)",
                options=list(df.columns),
                key=f"ix_{i}",
            )

        pk_valid = not (pk_enabled and not pk_headers)
        if not pk_valid:
            st.error("Define PRIMARY KEY is on — select at least one column, or turn it off.")

        pk_sql = [by_original[h] for h in pk_headers] if pk_enabled and pk_headers else None

        index_specs: list[list[str]] = []
        for i in range(n_ix):
            chosen = st.session_state.get(f"ix_{i}") or []
            if chosen:
                index_specs.append([by_original[h] for h in chosen])

        sql_table = render_create_table(
            table_name,
            cols,
            dialect,
            if_not_exists=if_not_exists,
            nullable_mode="safe" if nullability == "safe" else "strict",
            primary_key=pk_sql,
        )
        index_sql = render_create_indexes(
            table_name,
            cols,
            dialect,
            index_specs,
            if_not_exists=True,
            primary_key=pk_sql,
        )
        schema_sql = sql_table + (("\n\n" + index_sql) if index_sql else "")

        safe_base = resolve_table_base_name(table_name)

        st.subheader("Preview")
        st.dataframe(df.head(int(preview_n)), use_container_width=True)

        if pk_valid:
            st.subheader("Generated SQL")
            ddl_actions, ddl_dl = st.columns([1, 1])
            with ddl_actions:
                _clipboard_copy_button("Copy DDL", schema_sql)
            with ddl_dl:
                st.download_button(
                    label="Download DDL",
                    data=schema_sql.encode("utf-8"),
                    file_name=f"{safe_base}.sql",
                    mime="text/plain",
                    key="download_ddl",
                    use_container_width=True,
                )

            st.code(schema_sql, language="sql")

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
                    combined = f"{schema_sql}\n\n{insert_sql}"
                    st.download_button(
                        label="Download schema + INSERTs",
                        data=combined.encode("utf-8"),
                        file_name=f"{safe_base}_full.sql",
                        mime="text/plain",
                        key="download_full",
                    )
                st.code(insert_sql, language="sql")

            cap = (
                "Copy or download the SQL above. “DDL” includes CREATE TABLE and any CREATE INDEX lines. "
                "Clipboard works in supported browsers (HTTPS or localhost)."
            )
            if dialect == "sqlserver":
                cap += " SQL Server: `CREATE TABLE IF NOT EXISTS` requires SQL Server 2016+. `CREATE INDEX` is emitted without `IF NOT EXISTS`."
            else:
                cap += " `CREATE INDEX IF NOT EXISTS` is used for PostgreSQL and SQLite."
            st.caption(cap)
    except Exception as e:
        st.error(f"Failed to process CSV: {e}")
