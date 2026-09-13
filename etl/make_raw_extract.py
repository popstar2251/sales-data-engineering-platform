"""
Generates a "raw" source extract with realistic data-quality problems, seeded
from the public Superstore dataset (data/raw/superstore.csv), which is already
too clean to demonstrate any real cleaning logic.

This script injects, with a fixed random seed (reproducible):
  - exact duplicate rows
  - missing values in key fields (customer name, postal code, sales, order date)
  - invalid values (negative sales, zero/negative quantity, malformed dates,
    ship date before order date)

Run once to produce data/raw/superstore_raw.csv, which is what the ETL
pipeline (etl/etl.py) treats as the incoming raw extract. This is disclosed
here and in the README so the cleaning counts reported by the pipeline are
honest: they reflect issues deliberately introduced for demo purposes, not
issues found in the original clean dataset.
"""

import numpy as np
import pandas as pd

SEED = 42
SRC = "data/raw/superstore.csv"
OUT = "data/raw/superstore_raw.csv"

rng = np.random.default_rng(SEED)


def main():
    df = pd.read_csv(SRC, encoding="cp1252")
    n = len(df)
    print(f"Loaded clean source: {n} rows")

    # 1. Duplicate ~2% of rows (exact duplicates, appended)
    dup_n = int(n * 0.02)
    dup_rows = df.sample(n=dup_n, random_state=SEED)
    df = pd.concat([df, dup_rows], ignore_index=True)
    print(f"Injected {dup_n} duplicate rows")

    # 2. Null out key fields in a random ~1.5% slice each
    for col, frac in [
        ("Customer Name", 0.015),
        ("Postal Code", 0.02),
        ("Sales", 0.01),
        ("Order Date", 0.008),
    ]:
        idx = df.sample(frac=frac, random_state=SEED).index
        df.loc[idx, col] = np.nan
        print(f"Nulled {len(idx)} values in '{col}'")

    # 3. Invalid Sales (negative) in ~0.5% of rows
    idx = df.sample(frac=0.005, random_state=SEED + 1).index
    df.loc[idx, "Sales"] = -df.loc[idx, "Sales"].abs()
    print(f"Set {len(idx)} rows to negative Sales")

    # 4. Invalid Quantity (zero or negative) in ~0.5% of rows
    idx = df.sample(frac=0.005, random_state=SEED + 2).index
    df.loc[idx, "Quantity"] = 0
    print(f"Set {len(idx)} rows to zero Quantity")

    # 5. Malformed Order Date strings in ~0.3% of rows
    idx = df.sample(frac=0.003, random_state=SEED + 3).index
    df.loc[idx, "Order Date"] = "not-a-date"
    print(f"Corrupted {len(idx)} Order Date values")

    # 6. Ship Date before Order Date in ~0.3% of rows (swap them)
    idx = df.sample(frac=0.003, random_state=SEED + 4).index
    tmp = df.loc[idx, "Order Date"].copy()
    df.loc[idx, "Order Date"] = df.loc[idx, "Ship Date"]
    df.loc[idx, "Ship Date"] = tmp
    print(f"Swapped Order/Ship Date on {len(idx)} rows (ship before order)")

    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)  # shuffle
    df.to_csv(OUT, index=False, encoding="utf-8")
    print(f"\nWrote raw extract: {OUT} ({len(df)} rows)")


if __name__ == "__main__":
    main()
