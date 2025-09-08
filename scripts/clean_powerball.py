#!/usr/bin/env python3
"""
clean_powerball.py
---------------
Minimal cleaner tailored to the Texas Powerball CSVs.

Assumptions:
- CSVs have no header row.
- Columns are in this exact order, with either 10 or 11 columns total depending on the era:
    10 cols: Game, Month, Day, Year, Num1, Num2, Num3, Num4, Num5, Powerball
    11 cols: Game, Month, Day, Year, Num1, Num2, Num3, Num4, Num5, Powerball, PowerPlay
- If PowerPlay is not present (10 cols), we add it as blank.

Outputs (written to --outdir):
- powerball_clean_wide.csv  (one row per draw)
- powerball_clean_long.csv  (one row per ball; ideal for Tableau frequency)
- data_dictionary.csv       (quick column descriptions)

Usage:
    python clean_powerball.py --in data/raw/powerball_full.csv --outdir data/out
    python clean_powerball.py --in data/raw/powerball_sample.csv --outdir data/out
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

EXPECTED_WITH_POWERPLAY    = ["Game","Month","Day","Year","Num1","Num2","Num3","Num4","Num5","Powerball","PowerPlay"]
EXPECTED_WITHOUT_POWERPLAY = ["Game","Month","Day","Year","Num1","Num2","Num3","Num4","Num5","Powerball"]

def read_powerball_csv(path: Path) -> pd.DataFrame:
    """Read a no-header CSV and assign expected columns based on column count."""
    df = pd.read_csv(path, header=None, low_memory=False)
    if df.shape[1] == 11:
        df.columns = EXPECTED_WITH_POWERPLAY
    elif df.shape[1] == 10:
        df.columns = EXPECTED_WITHOUT_POWERPLAY
        df["PowerPlay"] = np.nan
    else:
        raise ValueError(f"Unexpected number of columns: {df.shape[1]} (expected 10 or 11).")
    return df[["Game","Month","Day","Year","Num1","Num2","Num3","Num4","Num5","Powerball","PowerPlay"]]

def to_date(df: pd.DataFrame) -> pd.Series:
    """Build a proper Date from numeric Month/Day/Year columns."""
    month = pd.to_numeric(df["Month"], errors="coerce")
    day   = pd.to_numeric(df["Day"], errors="coerce")
    year  = pd.to_numeric(df["Year"], errors="coerce")
    return pd.to_datetime(dict(year=year, month=month, day=day), errors="coerce")

def clean_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric fields to nullable integers and tidy Game."""
    num_cols = ["Num1","Num2","Num3","Num4","Num5","Powerball","PowerPlay"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["Game"] = df["Game"].astype(str).str.strip()
    return df

def dedupe_by_draw(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["Date","Num1","Num2","Num3","Num4","Num5","Powerball","PowerPlay"]
    return df.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)

def build_wide(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Date"] = to_date(out)
    out = out[pd.notna(out["Date"])]
    out = clean_types(out)

    # Drop rows missing any number
    for c in ["Num1","Num2","Num3","Num4","Num5","Powerball"]:
        out = out[pd.notna(out[c])]

    # Helpers
    out["Year"] = out["Date"].dt.year
    out["YearMonth"] = out["Date"].dt.to_period("M").astype(str)

    # Select only the lean columns (no Game, no Month)
    out = out[["Date","Num1","Num2","Num3","Num4","Num5","Powerball","PowerPlay","Year","YearMonth"]]
    out = out.sort_values("Date").reset_index(drop=True)
    out = dedupe_by_draw(out)
    return out

def build_long(wide: pd.DataFrame) -> pd.DataFrame:
    main = wide.melt(
        id_vars=["Date","PowerPlay","Year","YearMonth"],
        value_vars=["Num1","Num2","Num3","Num4","Num5"],
        var_name="Position",
        value_name="BallNumber"
    )
    main["Position"] = main["Position"].str.extract(r"(\d+)").astype(int)
    main["BallType"] = "Main"

    pb = wide[["Date","PowerPlay","Year","YearMonth","Powerball"]].copy()
    pb = pb.rename(columns={"Powerball":"BallNumber"})
    pb["Position"] = "PB"
    pb["BallType"] = "Powerball"

    cols = ["Date","BallType","BallNumber","Position","PowerPlay","Year","YearMonth"]
    long_df = pd.concat([main[cols], pb[cols]], ignore_index=True)
    long_df["BallNumber"] = pd.to_numeric(long_df["BallNumber"], errors="coerce").astype("Int64")
    return long_df

def main():
    ap = argparse.ArgumentParser(description="Minimal Powerball CSV cleaner (no headers; fixed column order).")
    ap.add_argument("--in", dest="inp", required=True, help="Path to input CSV")
    ap.add_argument("--outdir", default="out", help="Output directory")
    args = ap.parse_args()

    inp = Path(args.inp)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    raw  = read_powerball_csv(inp)
    wide = build_wide(raw)
    long = build_long(wide)

    data_dict = pd.DataFrame({
        "column": ["Date", "Num1", "Num2", "Num3", "Num4", "Num5", "Powerball", "PowerPlay",
                   "Year", "YearMonth", "BallType", "BallNumber", "Position"],
        "description": [
            "Draw date (YYYY-MM-DD)",
            "First main ball", "Second main ball", "Third main ball", "Fourth main ball", "Fifth main ball",
            "Powerball number",
            "Power Play multiplier (may be blank)",
            "Calendar year",
            "YYYY-MM period string",
            "Main or Powerball (long format only)",
            "Ball number value",
            "Position: 1..5 for main, PB for Powerball (long only)"
        ]
    })

    wide.to_csv(outdir / "powerball_clean_wide.csv", index=False)
    long.to_csv(outdir / "powerball_clean_long.csv", index=False)
    data_dict.to_csv(outdir / "data_dictionary.csv", index=False)

    print(f"Wrote {outdir / 'powerball_clean_wide.csv'}")
    print(f"Wrote {outdir / 'powerball_clean_long.csv'}")
    print(f"Wrote {outdir / 'data_dictionary.csv'}")

if __name__ == "__main__":
    main()
