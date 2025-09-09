#!/usr/bin/env python3
"""
load_to_sqlite.py
-----------------
Load cleaned Powerball CSVs into a SQLite database.

Defaults assume the "long" file produced by clean_powerball.py, but "wide" works too.

Examples:
  # long → table draws (default)
  python scripts/load_to_sqlite.py \
    --csv data/processed/powerball_sample/powerball_clean_long.csv \
    --db powerball.db

  # wide → table draws_wide
  python scripts/load_to_sqlite.py \
    --csv data/processed/powerball_sample/powerball_clean_wide.csv \
    --db powerball.db \
    --schema wide \
    --table draws_wide \
    --replace
"""

import argparse
import csv
import sqlite3
from pathlib import Path
import pandas as pd

LONG_COLS = ["Date","BallType","BallNumber","Position","PowerPlay","Year","YearMonth"]
WIDE_COLS = ["Date","Num1","Num2","Num3","Num4","Num5","Powerball","PowerPlay","Year","YearMonth"]


def infer_schema(schema: str):
    if schema == "long":
        # Position can be 1..5 or 'PB' → store as TEXT
        return {
            "Date": "TEXT",
            "BallType": "TEXT",
            "BallNumber": "INTEGER",
            "Position": "TEXT",
            "PowerPlay": "INTEGER",
            "Year": "INTEGER",
            "YearMonth": "TEXT",
        }, LONG_COLS
    elif schema == "wide":
        return {
            "Date": "TEXT",
            "Num1": "INTEGER",
            "Num2": "INTEGER",
            "Num3": "INTEGER",
            "Num4": "INTEGER",
            "Num5": "INTEGER",
            "Powerball": "INTEGER",
            "PowerPlay": "INTEGER",
            "Year": "INTEGER",
            "YearMonth": "TEXT",
        }, WIDE_COLS
    else:
        raise SystemExit(f"Unknown schema: {schema}")


def coerce_types(df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    for col, typ in schema.items():
        if col not in out.columns:
            raise SystemExit(f"Column '{col}' was not found in CSV.")
        if typ == "INTEGER":
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        else:
            out[col] = out[col].astype(str)
    return out


def create_table(conn: sqlite3.Connection, table: str, schema: dict[str, str], replace: bool):
    cur = conn.cursor()
    if replace:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    # Build CREATE TABLE
    cols_sql = ", ".join([f'"{c}" {t}' for c, t in schema.items()])
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
    conn.commit()


def insert_rows(conn: sqlite3.Connection, table: str, df: pd.DataFrame, ordered_cols: list[str]):
    # Convert pandas NA to None for sqlite
    rows = [
        [None if pd.isna(v) else (int(v) if isinstance(v, (pd.Int64Dtype().type,)) else v) for v in row]
        for row in df[ordered_cols].itertuples(index=False, name=None)
    ]
    placeholders = ", ".join(["?"] * len(ordered_cols))
    sql = f'INSERT INTO "{table}" ({", ".join([f"""\"{c}\"""" for c in ordered_cols])}) VALUES ({placeholders})'
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()


def main():
    ap = argparse.ArgumentParser(description="Load cleaned Powerball CSV into SQLite.")
    ap.add_argument("--csv", required=True, help="Path to cleaned CSV (long or wide).")
    ap.add_argument("--db", default="powerball.db", help="SQLite DB path (will be created if missing).")
    ap.add_argument("--table", default="draws", help='Destination table name (default: "draws").')
    ap.add_argument("--schema", choices=["long","wide"], default="long",
                    help='Which CSV schema to expect (default: "long").')
    ap.add_argument("--replace", action="store_true", help="Drop table if it exists, then recreate.")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    schema, ordered_cols = infer_schema(args.schema)

    # Read CSV as strings first (no dtype surprises), then coerce
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=True, na_values=["", "NA", "NaN"])
    # Trim to expected columns only (and order them)
    missing = [c for c in ordered_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV missing expected columns: {missing}")
    df = df[ordered_cols]
    df = coerce_types(df, schema)

    conn = sqlite3.connect(args.db)
    try:
        create_table(conn, args.table, schema, replace=args.replace)
        insert_rows(conn, args.table, df, ordered_cols)
    finally:
        conn.close()

    print(f"Loaded {len(df):,} rows into {args.db}:{args.table} ({args.schema}).")


if __name__ == "__main__":
    main()
