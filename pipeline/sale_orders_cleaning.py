"""Clean order confirmation numbers before running the reconciliation."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
INPUT_FILE = DATA_DIR / "sales_orders.xlsx"
OUTPUT_FILE = DATA_DIR / "sales_orders_clean.xlsx"
CONFIRMATION_COL = "OrderLineOrConfirmation"


def clean_confirmation(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    text = text.replace(",", " ")
    text = re.sub(r"[-]+$", "", text)
    text = re.sub(r"[^A-Za-z0-9\-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


orders_df = pd.read_excel(INPUT_FILE, sheet_name=0)
if CONFIRMATION_COL not in orders_df.columns:
    raise ValueError(f"Column '{CONFIRMATION_COL}' not found. Available: {list(orders_df.columns)}")

orders_df[CONFIRMATION_COL] = orders_df[CONFIRMATION_COL].apply(clean_confirmation)
orders_df = orders_df[orders_df[CONFIRMATION_COL].notna()].reset_index(drop=True)

allowed_pattern = re.compile(r"^[A-Za-z0-9\-]+$")
invalid_rows = orders_df[~orders_df[CONFIRMATION_COL].apply(lambda x: bool(allowed_pattern.match(str(x))))]

if not invalid_rows.empty:
    print("⚠️ Confirmations with unexpected characters detected:")
    print(invalid_rows[[CONFIRMATION_COL]].head(20))
    print(f"Total rows with issues: {len(invalid_rows)}")
else:
    print("✅ All confirmation numbers contain only letters, digits, or hyphens.")

DATA_DIR.mkdir(parents=True, exist_ok=True)
orders_df.to_excel(OUTPUT_FILE, index=False)
print(f"\nClean file saved to: {OUTPUT_FILE}")
