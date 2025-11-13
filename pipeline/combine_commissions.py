"""Consolidate duplicate commission rows for the portfolio reconciliation demo."""

from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
INPUT_FILE = DATA_DIR / "commission_exports.xlsx"
OUTPUT_FILE = DATA_DIR / "commission_exports_grouped.xlsx"

PROVIDER_COL = "Provider"
LOCATOR_COL = "BookingLocator"
VALUE_COL = "BilledCommissionUSD"


def require_columns(df: pd.DataFrame, sheet_label: str) -> None:
    missing = [col for col in (PROVIDER_COL, LOCATOR_COL, VALUE_COL) if col not in df.columns]
    if missing:
        raise ValueError(f"{sheet_label} is missing required columns: {missing}")


df = pd.read_excel(INPUT_FILE)
df.columns = df.columns.str.strip()
require_columns(df, "Commission export")

df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors="coerce").fillna(0)

grouped = (
    df.groupby([PROVIDER_COL, LOCATOR_COL], as_index=False)[VALUE_COL]
    .sum()
    .sort_values([PROVIDER_COL, LOCATOR_COL])
)

grouped.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Grouped commission export saved to {OUTPUT_FILE}")

# --- validation snapshot ----------------------------------------------------
orig_totals = (
    df.groupby(LOCATOR_COL, as_index=False)[VALUE_COL]
    .sum()
    .rename(columns={VALUE_COL: "OriginalValue"})
)

grouped_totals = (
    grouped.groupby(LOCATOR_COL, as_index=False)[VALUE_COL]
    .sum()
    .rename(columns={VALUE_COL: "GroupedValue"})
)

comparison = orig_totals.merge(grouped_totals, on=LOCATOR_COL, how="outer", indicator=True)
comparison["Delta"] = (comparison["OriginalValue"] - comparison["GroupedValue"]).round(2)

mismatches = comparison[(comparison["_merge"] == "both") & (comparison["Delta"].abs() > 0.01)]
missing_after_group = comparison[comparison["_merge"] == "left_only"]
new_after_group = comparison[comparison["_merge"] == "right_only"]

print("\nValidation summary")
print(f"- Locators in raw file: {len(orig_totals):,}")
print(f"- Locators after grouping: {len(grouped_totals):,}")
print(f"- Perfect matches: {len(comparison) - len(mismatches) - len(missing_after_group) - len(new_after_group):,}")
print(f"- Variances > $0.01: {len(mismatches):,}")
print(f"- Missing post-group: {len(missing_after_group):,}")
print(f"- Unexpected new locators: {len(new_after_group):,}")

if mismatches.empty and missing_after_group.empty and new_after_group.empty:
    print("This confirms that the grouped file preserves booked commission values.")
else:
    print("Review the rows above before publishing the data to Tableau.")
