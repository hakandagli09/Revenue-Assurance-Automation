"""Clean and aggregate commission CSV exports."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from importlib.util import find_spec


DATA_DIR = Path("data")
INPUT_CSV = DATA_DIR / "commission_snapshot.csv"
OUTPUT_XLSX = DATA_DIR / "commission_snapshot_agg.xlsx"

LOCATOR_COL = "BookingLocator"
AMOUNT_COL = "GrossAmountUSD"
COMMISSION_COL = "CommissionWithoutTaxUSD"
CURRENCY_COL = "Currency"
SALE_DATE_COL = "SaleDate"
SERVICE_DATE_COL = "ServiceDate"


def read_csv_safely(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=encoding)
        except Exception:
            continue
    return pd.read_csv(path, sep=None, engine="python")


def to_number(value: object) -> float:
    if pd.isna(value):
        return np.nan
    text = str(value).strip()
    if not text:
        return np.nan
    is_negative = text.startswith("(") and text.endswith(")")
    if is_negative:
        text = text[1:-1]
    sanitized = text.replace("$", "").replace(",", "").replace(" ", "")
    try:
        result = float(sanitized)
    except ValueError:
        result = pd.to_numeric(sanitized, errors="coerce")
    if pd.isna(result):
        return np.nan
    return -result if is_negative else result


def excelish_datetime(value: object) -> pd.Timestamp | pd.NaT:
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.to_datetime(value, errors="coerce")
    text = str(value).strip()
    if not text:
        return pd.NaT
    if re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
        return pd.to_datetime(float(text), unit="d", origin="1899-12-30", errors="coerce")
    return pd.to_datetime(text, errors="coerce")


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns {missing}")


df = read_csv_safely(INPUT_CSV)
df = df.loc[:, ~df.columns.str.contains(r"^Unnamed:", case=False)]
ensure_columns(df, [LOCATOR_COL, AMOUNT_COL, COMMISSION_COL, CURRENCY_COL, SALE_DATE_COL, SERVICE_DATE_COL])

df["loc_norm"] = df[LOCATOR_COL].astype(str).str.strip()
df.loc[df["loc_norm"].isin(["", "nan"]), "loc_norm"] = np.nan

df["GrossAmountValue"] = df[AMOUNT_COL].apply(to_number)
df["CommissionValue"] = df[COMMISSION_COL].apply(to_number)
df["SaleDate"] = df[SALE_DATE_COL].apply(excelish_datetime)
df["ServiceDate"] = df[SERVICE_DATE_COL].apply(excelish_datetime)

aggregated = (
    df.dropna(subset=["loc_norm"])
    .groupby("loc_norm", as_index=False)
    .agg(
        CommissionWithoutTaxUSD=("CommissionValue", "sum"),
        GrossAmountUSD=("GrossAmountValue", "sum"),
        Rows=("loc_norm", "size"),
        Currency=(CURRENCY_COL, lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan),
        SaleDateMin=("SaleDate", "min"),
        SaleDateMax=("SaleDate", "max"),
    )
)

engine = "openpyxl" if find_spec("openpyxl") else ("xlsxwriter" if find_spec("xlsxwriter") else None)
if not engine:
    raise RuntimeError("Install 'openpyxl' or 'xlsxwriter' to export Excel files.")

DATA_DIR.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUTPUT_XLSX, engine=engine, datetime_format="yyyy-mm-dd" if engine == "xlsxwriter" else None) as writer:
    aggregated.to_excel(writer, sheet_name="booking_locator_agg", index=False)

print(f"✅ Aggregated commission snapshot written to {OUTPUT_XLSX}")
print(f"   Unique locators: {len(aggregated):,}")
