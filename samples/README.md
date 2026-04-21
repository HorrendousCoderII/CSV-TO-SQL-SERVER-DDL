# Sample CSVs for manual testing

Use **Streamlit → Upload** and pick a file from this folder (or drag it in).

| File | What to check |
|------|----------------|
| `01_customers_simple.csv` | Integers, short strings, emails (longer `VARCHAR`). |
| `02_orders_decimals.csv` | `NUMERIC` / decimals, integer qty. |
| `03_rows_with_empty_cells.csv` | **Strict** nullability: some columns should stay `NULL` vs `NOT NULL`. |
| `04_dirty_and_duplicate_headers.csv` | Sanitized names, reserved-like chars, spaces in headers. |
| `05_dates_iso.csv` | ISO dates / timestamps (may infer `DATE` / `TIMESTAMP`). |
| `06_boolean_like.csv` | `true`/`false` and `yes`/`no` → **BOOLEAN** (after integer check). |
| `07_mixed_type_column.csv` | `code` mixes numbers and `N/A` → **VARCHAR**. |
| `08_bigint_ids.csv` | Values above 32-bit int → **BIGINT**. |
| `09_duplicate_identical_headers.csv` | Polars renames duplicate headers (`col_duplicated_0`); still valid smoke test. |

**Note:** Streamlit’s uploader does not open this folder automatically; select these files from disk when testing.

## Large / stress-test CSVs

Generate files locally (they are **gitignored** under `samples/large_*.csv`):

```bash
python scripts/generate_large_csv.py -o samples/large_50k.csv --rows 50000
```

**Rough sizing**

| Rows      | Approx. file size | What it exercises                          |
|-----------|-------------------|---------------------------------------------|
| 10k       | ~1–3 MiB          | Quick check that UI still feels snappy    |
| 50k–100k| ~5–25 MiB         | Noticeable parse + inference time         |
| 200k–500k | ~20–80 MiB      | Memory + Streamlit responsiveness         |
| 1M+       | 100+ MiB          | Risk of OOM or long freezes on 8 GiB RAM  |

The app loads the **entire CSV into memory** (Polars + Streamlit upload buffer), so treat **200k rows** as a solid “big but usually fine” default on a typical dev PC; go higher only if you have plenty of RAM and patience.
