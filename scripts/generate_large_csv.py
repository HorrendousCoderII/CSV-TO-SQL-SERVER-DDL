#!/usr/bin/env python3
"""
Generate a wide synthetic CSV for stress-testing the Streamlit CSV→DDL app.

Writes in batches (low memory). Does not use Polars here so generation stays fast.

Suggested sizes (rough disk size; RAM use in the app is higher because Polars loads
the whole file):

  --rows 10_000     ~1–3 MB   quick sanity check
  --rows 50_000     ~5–15 MB  light stress
  --rows 200_000    ~20–60 MB medium; good default “big CSV” on 16 GB RAM
  --rows 1_000_000  ~100–300 MB+ heavy; may freeze UI or OOM on smaller machines

Examples:

  python scripts/generate_large_csv.py -o samples/large_100k.csv --rows 100000
  python scripts/generate_large_csv.py -o D:/temp/huge.csv --rows 500000 --text-len 80
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import date, timedelta


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-o", "--output", required=True, help="Output .csv path")
    p.add_argument("--rows", type=int, default=100_000, help="Number of data rows (default 100000)")
    p.add_argument("--text-len", type=int, default=48, help="Max length of each synthetic text column")
    p.add_argument("--batch-size", type=int, default=10_000, help="Rows buffered before each write")
    args = p.parse_args()

    if args.rows < 1:
        raise SystemExit("--rows must be >= 1")
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")

    text_len = max(1, min(args.text_len, 500))

    # Mix of types the inferrer cares about: int, decimal-ish, dates, long-ish text
    headers = [
        "id",
        "region_id",
        "units_sold",
        "unit_price",
        "discount",
        "ship_date",
        "customer_ref",
        "sku",
        "notes",
    ]

    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    base = date(2020, 1, 1)
    pad = "ABCDEFGH" * ((text_len // 8) + 1)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)

        batch: list[list] = []
        for i in range(1, args.rows + 1):
            ship = base + timedelta(days=i % 4000)
            note = (f"row_{i}_" + pad)[:text_len]
            sku = f"SKU-{(i * 7919) % 99999:05d}"
            ref = f"CUST-{(i * 11003) % 999999:06d}"
            batch.append(
                [
                    i,
                    (i % 50) + 1,
                    (i % 200) + 1,
                    f"{((i % 1000) + 1) * 1.237:.4f}",
                    f"{(i % 20) / 100:.2f}",
                    ship.isoformat(),
                    ref,
                    sku,
                    note,
                ]
            )
            if len(batch) >= args.batch_size:
                w.writerows(batch)
                batch.clear()
        if batch:
            w.writerows(batch)

    size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"Wrote {args.rows:,} rows to {args.output} ({size_mb:.2f} MiB)")


if __name__ == "__main__":
    main()
