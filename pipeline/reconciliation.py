"""Full-featured reconciliation script that powers the Tableau dashboard."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def cfg(config: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in config:
            return config[key]
    raise KeyError(f"Missing configuration key. Expected one of: {keys}")


def normalize_key(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.replace(r"[^0-9A-Za-z]", "", regex=True)
        .str.upper()
    )


def load_provider_map() -> dict[str, str]:
    for candidate in ("unique_providers.json", "unique_proveedores.json"):
        path = Path(candidate)
        if path.exists():
            with path.open("r", encoding="utf-8") as fp:
                return json.load(fp)
    return {}


def safe_sum(series: pd.Series | list | None) -> float:
    if series is None:
        return 0.0
    series = pd.to_numeric(pd.Series(series), errors="coerce")
    return float(series.sum(skipna=True))


def ensure_provider_column(df: pd.DataFrame, candidates: list[str], suffix: str) -> None:
    for col in candidates:
        candidate = col.lower()
        if candidate in df.columns:
            df[f"provider_{suffix}"] = df[candidate]
            return
    df[f"provider_{suffix}"] = pd.NA


def breakdown_by_provider(df: pd.DataFrame, category: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    grouped = (
        df.groupby("Provider", dropna=False)
        .agg(
            Records=("key", "count"),
            ExpectedCommissionUSD=("ExpectedCommissionUSD", "sum"),
            BilledCommissionUSD=("BilledCommissionUSD", "sum"),
            CommissionGapUSD=("CommissionGapUSD", "sum"),
        )
        .reset_index()
        .fillna({"Provider": "Unassigned"})
    )
    grouped.insert(0, "Category", category)
    return grouped


def main(config_path: str) -> None:
    with open(config_path, "r", encoding="utf-8") as fp:
        config = json.load(fp)

    workbook = pd.ExcelFile(cfg(config, "input_file"))
    sheets = {name.lower(): name for name in workbook.sheet_names}

    orders_sheet = cfg(config, "orders_sheet", "odv_sheet").lower()
    commissions_sheet = cfg(config, "commissions_sheet", "com_sheet").lower()
    if orders_sheet not in sheets or commissions_sheet not in sheets:
        raise ValueError(f"Available sheets: {workbook.sheet_names}")

    orders_df = workbook.parse(sheets[orders_sheet])
    commissions_df = workbook.parse(sheets[commissions_sheet])

    orders_df.columns = orders_df.columns.str.strip().str.lower()
    commissions_df.columns = commissions_df.columns.str.strip().str.lower()

    orders_key = cfg(config, "orders_key", "odv_key").strip().lower()
    commissions_key = cfg(config, "commissions_key", "com_key").strip().lower()
    orders_commission = cfg(config, "orders_commission_col", "odv_commission_col").strip().lower()
    commissions_commission = cfg(config, "commissions_commission_col", "com_commission_col").strip().lower()

    for name, df, required in (
        ("orders", orders_df, [orders_key, orders_commission]),
        ("commissions", commissions_df, [commissions_key, commissions_commission]),
    ):
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"{name} sheet is missing columns {missing}")

    provider_cols = [col.lower() for col in config.get("provider_cols", ["provider", "supplier", "operator"])]
    provider_map = load_provider_map()

    ensure_provider_column(orders_df, provider_cols, "orders")
    ensure_provider_column(commissions_df, provider_cols, "commissions")

    orders_df["provider_orders"] = orders_df["provider_orders"].replace(provider_map)
    commissions_df["provider_commissions"] = commissions_df["provider_commissions"].replace(provider_map)

    orders_df["key"] = normalize_key(orders_df[orders_key])
    commissions_df["key"] = normalize_key(commissions_df[commissions_key])

    orders_df["ExpectedCommissionUSD"] = pd.to_numeric(orders_df[orders_commission], errors="coerce").fillna(0)
    commissions_df["BilledCommissionUSD"] = pd.to_numeric(
        commissions_df[commissions_commission],
        errors="coerce",
    ).fillna(0)

    merged = orders_df.merge(
        commissions_df,
        on="key",
        how="outer",
        suffixes=("_order", "_commission"),
        indicator=True,
    )

    merged["ExpectedCommissionUSD"] = merged["ExpectedCommissionUSD"].fillna(0)
    merged["BilledCommissionUSD"] = merged["BilledCommissionUSD"].fillna(0)
    merged["CommissionGapUSD"] = merged["ExpectedCommissionUSD"] - merged["BilledCommissionUSD"]
    merged["Provider"] = (
        merged.get("provider_orders")
        .combine_first(merged.get("provider_commissions"))
        .fillna("Unassigned")
    )

    tolerance = float(config.get("tolerance", 0.25))
    matches = merged[merged["_merge"] == "both"].copy()
    orders_only = merged[merged["_merge"] == "left_only"].copy()
    commissions_only = merged[merged["_merge"] == "right_only"].copy()
    gaps = matches[matches["CommissionGapUSD"].abs() > tolerance].copy()

    summary = pd.DataFrame(
        {
            "Category": [
                "Perfect Match",
                "Commission Gap",
                "Orders Missing Commission",
                "Commission Missing Order",
            ],
            "Records": [len(matches), len(gaps), len(orders_only), len(commissions_only)],
            "ExpectedCommissionUSD": [
                safe_sum(matches["ExpectedCommissionUSD"]),
                safe_sum(gaps["ExpectedCommissionUSD"]),
                safe_sum(orders_only["ExpectedCommissionUSD"]),
                0,
            ],
            "BilledCommissionUSD": [
                safe_sum(matches["BilledCommissionUSD"]),
                safe_sum(gaps["BilledCommissionUSD"]),
                0,
                safe_sum(commissions_only["BilledCommissionUSD"]),
            ],
            "CommissionGapUSD": [
                safe_sum(matches["CommissionGapUSD"]),
                safe_sum(gaps["CommissionGapUSD"]),
                safe_sum(orders_only["CommissionGapUSD"]),
                safe_sum(commissions_only["CommissionGapUSD"]),
            ],
        }
    )

    provider_summary = pd.concat(
        [
            breakdown_by_provider(matches, "Perfect Match"),
            breakdown_by_provider(gaps, "Commission Gap"),
            breakdown_by_provider(orders_only, "Orders Missing Commission"),
            breakdown_by_provider(commissions_only, "Commission Missing Order"),
        ],
        ignore_index=True,
    )

    output_path = Path(cfg(config, "output_file"))
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        matches.to_excel(writer, sheet_name="matches", index=False)
        gaps.to_excel(writer, sheet_name="commission_gap", index=False)
        orders_only.to_excel(writer, sheet_name="orders_missing_commission", index=False)
        commissions_only.to_excel(writer, sheet_name="commissions_missing_orders", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        provider_summary.to_excel(writer, sheet_name="provider_summary", index=False)

    print("=== Portfolio Reconciliation (v3) ===")
    print(summary.to_string(index=False))
    print(f"\nWorkbook saved to: {output_path}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: reconciliation.py path/to/config.json")
    else:
        main(sys.argv[1])
