# =============================================================================
# quality_checks.py — Milestone 2: Data Quality Checks → staging_1
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================
"""
Checks performed on every DataFrame:
  1. Null / missing-value audit  → fill or drop based on column criticality
  2. Duplicate detection & removal
  3. Data-type coercion
  4. Domain / range validation   (prices, discounts, quantities, dates)
  5. String standardisation      (strip whitespace, title-case names)

Results are written to the staging_1 folder as cleaned CSVs.
A quality-report CSV summarising issues per dataset is also saved there.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

from config import (
    STAGING_1_DIR,
    PRICE_MIN, PRICE_MAX,
    DISCOUNT_MIN, DISCOUNT_MAX,
    QTY_MIN, QTY_MAX,
    ORDER_DATE_MIN, ORDER_DATE_MAX,
)

# ── Per-dataset rules ─────────────────────────────────────────────────────────
# critical_cols  → rows with nulls in these columns are DROPPED
# fill_rules     → {col: fill_value} for non-critical nulls
# date_cols      → columns to coerce to datetime
# string_cols    → columns to strip & title-case

DATASET_RULES = {
    "orders": {
        "critical_cols": ["order_id", "customer_id", "order_date", "store_id", "staff_id"],
        "fill_rules":    {"shipped_date": "unknown", "order_status": 1},
        "date_cols":     ["order_date", "required_date", "shipped_date"],
        "string_cols":   [],
    },
    "order_items": {
        "critical_cols": ["order_id", "item_id", "product_id", "quantity", "list_price"],
        "fill_rules":    {"discount": 0.0},
        "date_cols":     [],
        "string_cols":   [],
    },
    "customers": {
        "critical_cols": ["customer_id", "email"],
        "fill_rules":    {"phone": "unknown", "street": "unknown",
                          "city": "unknown", "state": "unknown", "zip_code": "unknown"},
        "date_cols":     [],
        "string_cols":   ["first_name", "last_name", "city", "state"],
    },
    "products": {
        "critical_cols": ["product_id", "product_name", "list_price"],
        "fill_rules":    {},
        "date_cols":     [],
        "string_cols":   ["product_name"],
    },
    "stores": {
        "critical_cols": ["store_id", "store_name"],
        "fill_rules":    {"phone": "unknown", "email": "unknown",
                          "zip_code": "unknown"},
        "date_cols":     [],
        "string_cols":   ["store_name", "city", "state"],
    },
    "staffs": {
        "critical_cols": ["staff_id", "email"],
        "fill_rules":    {"last_name": "unknown", "phone": "unknown"},
        "date_cols":     [],
        "string_cols":   ["first_name", "last_name"],
    },
    "brands": {
        "critical_cols": ["brand_id", "brand_name"],
        "fill_rules":    {},
        "date_cols":     [],
        "string_cols":   ["brand_name"],
    },
    "categories": {
        "critical_cols": ["category_id", "category_name"],
        "fill_rules":    {},
        "date_cols":     [],
        "string_cols":   ["category_name"],
    },
    "stocks": {
        "critical_cols": ["store_id", "product_id", "quantity"],
        "fill_rules":    {},
        "date_cols":     [],
        "string_cols":   [],
    },
    "exchange_rates": {
        "critical_cols": ["currency_code", "rate_to_usd"],
        "fill_rules":    {},
        "date_cols":     [],
        "string_cols":   [],
    },
}


# ── Helper utilities ──────────────────────────────────────────────────────────

def _replace_null_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Replace literal 'NULL' / 'null' / 'None' strings with np.nan."""
    return df.replace(["NULL", "null", "None", "none", "N/A", "n/a", ""], np.nan)


def _coerce_dates(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    """Parse date columns; invalid values become NaT."""
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _standardise_strings(df: pd.DataFrame, string_cols: list) -> pd.DataFrame:
    """Strip whitespace and apply title-case to specified columns."""
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
    return df


def _validate_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Domain checks specific to orders."""
    before = len(df)
    date_min = pd.Timestamp(ORDER_DATE_MIN)
    date_max = pd.Timestamp(ORDER_DATE_MAX)

    # Drop rows where order_date is outside the valid window
    if "order_date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["order_date"]):
        df = df[df["order_date"].between(date_min, date_max)]

    # Clamp order_status to known values (1-4)
    if "order_status" in df.columns:
        df["order_status"] = pd.to_numeric(df["order_status"], errors="coerce")
        df = df[df["order_status"].isin([1, 2, 3, 4])]

    return df, before - len(df)


def _validate_order_items(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Domain checks specific to order_items."""
    before = len(df)

    if "list_price" in df.columns:
        df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")
        df = df[df["list_price"].between(PRICE_MIN, PRICE_MAX)]

    if "discount" in df.columns:
        df["discount"] = pd.to_numeric(df["discount"], errors="coerce")
        df = df[df["discount"].between(DISCOUNT_MIN, DISCOUNT_MAX)]

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df = df[df["quantity"].between(QTY_MIN, QTY_MAX)]

    return df, before - len(df)


def _validate_products(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Domain checks specific to products."""
    before = len(df)

    if "list_price" in df.columns:
        df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")
        df = df[df["list_price"].between(PRICE_MIN, PRICE_MAX)]

    return df, before - len(df)


VALIDATORS = {
    "orders":      _validate_orders,
    "order_items": _validate_order_items,
    "products":    _validate_products,
}


# ── Core quality-check routine ────────────────────────────────────────────────

def check_dataset(name: str, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Run the full quality-check suite on a single DataFrame.

    Returns
    -------
    (cleaned_df, report_row)  where report_row is a dict summarising findings.
    """
    report = {
        "dataset":          name,
        "rows_before":      len(df),
        "null_cells_found": 0,
        "rows_dropped_null":0,
        "duplicates_found": 0,
        "rows_dropped_domain": 0,
        "rows_after":       0,
    }

    rules = DATASET_RULES.get(name, {
        "critical_cols": [], "fill_rules": {},
        "date_cols": [], "string_cols": [],
    })

    # Step 1 – replace literal NULL strings
    df = _replace_null_strings(df)

    # Step 2 – report null counts
    report["null_cells_found"] = int(df.isnull().sum().sum())

    # Step 3 – fill non-critical nulls
    for col, val in rules.get("fill_rules", {}).items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    # Step 4 – drop rows with nulls in critical columns
    before_drop = len(df)
    critical = [c for c in rules.get("critical_cols", []) if c in df.columns]
    df = df.dropna(subset=critical)
    report["rows_dropped_null"] = before_drop - len(df)

    # Step 5 – drop duplicate rows
    before_dup = len(df)
    df = df.drop_duplicates()
    report["duplicates_found"] = before_dup - len(df)

    # Step 6 – date coercion
    df = _coerce_dates(df, rules.get("date_cols", []))

    # Step 7 – string standardisation
    df = _standardise_strings(df, rules.get("string_cols", []))

    # Step 8 – domain-specific validation
    if name in VALIDATORS:
        df, dropped = VALIDATORS[name](df)
        report["rows_dropped_domain"] = dropped

    report["rows_after"] = len(df)
    return df, report


# ── Master quality-check runner ───────────────────────────────────────────────

def run_quality_checks(datasets: dict) -> dict:
    """
    Apply quality checks to all datasets and persist cleaned versions to
    staging_1.  A quality-report CSV is also written there.

    Parameters
    ----------
    datasets : dict[str, DataFrame]  — raw extracted data

    Returns
    -------
    dict[str, DataFrame]  — cleaned DataFrames
    """
    print("\n" + "="*60)
    print("  MILESTONE 2 — DATA QUALITY CHECKS")
    print("="*60)

    cleaned   = {}
    all_reports = []

    for name, df in datasets.items():
        print(f"\n[QC] Checking '{name}' ({len(df)} rows) …")
        clean_df, report = check_dataset(name, df)
        cleaned[name] = clean_df
        all_reports.append(report)

        print(f"  • Null cells found    : {report['null_cells_found']}")
        print(f"  • Rows dropped (null) : {report['rows_dropped_null']}")
        print(f"  • Duplicates removed  : {report['duplicates_found']}")
        print(f"  • Rows dropped (domain): {report['rows_dropped_domain']}")
        print(f"  ✓ Rows remaining       : {report['rows_after']}")

        # Write cleaned CSV to staging_1
        out_path = os.path.join(STAGING_1_DIR, f"{name}_cleaned.csv")
        clean_df.to_csv(out_path, index=False)
        print(f"  → Saved: {out_path}")

    # Save quality report
    report_df   = pd.DataFrame(all_reports)
    report_path = os.path.join(STAGING_1_DIR, "_quality_report.csv")
    report_df.to_csv(report_path, index=False)
    print(f"\n[QC] Quality report saved → {report_path}")
    print(f"[QC] Done — {len(cleaned)} datasets passed quality checks.\n")

    return cleaned
