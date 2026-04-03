# =============================================================================
# transform.py — Milestone 3: Transformations → staging_2
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================
"""
Key transformations applied:
  1. Currency Conversion    – add local-price columns using exchange rates
  2. Delivery Metrics       – latency_days, is_late_delivery flag
  3. Locality Flag          – is_local_customer (customer state == store state)
  4. Order-Status Lookup    – resolve numeric status to human-readable label
  5. Final-Price Calculation– list_price × quantity × (1 – discount)
  6. Timestamp Cleanup      – convert Unix epoch Extraction_Date to datetime
"""

import os
import pandas as pd
import numpy as np

from config import (
    STAGING_2_DIR,
    ORDER_STATUS_MAP,
    TARGET_CURRENCY,
)


# ── 1 – Timestamp cleanup ─────────────────────────────────────────────────────

def _fix_extraction_date(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric Unix-epoch Extraction_Date column to readable datetime."""
    if "Extraction_Date" in df.columns:
        col = pd.to_numeric(df["Extraction_Date"], errors="coerce")
        df["Extraction_Date"] = pd.to_datetime(col, unit="s", errors="coerce")
    return df


# ── 2 – Currency conversion ───────────────────────────────────────────────────

def transform_currency(order_items: pd.DataFrame,
                        exchange_rates: pd.DataFrame) -> pd.DataFrame:
    """
    Add `list_price_<TARGET>` and `final_price_usd` columns to order_items.

    list_price is assumed to be in USD.  We convert to TARGET_CURRENCY using:
        local_price = list_price * rate_to_usd[TARGET_CURRENCY]
    """
    print(f"  [T1] Currency conversion → {TARGET_CURRENCY} …")

    # Build a simple dict  { currency_code: rate_from_usd }
    rate_lookup = exchange_rates.set_index("currency_code")["rate_to_usd"].to_dict()
    target_rate  = rate_lookup.get(TARGET_CURRENCY, 1.0)

    df = order_items.copy()
    df["list_price_usd"]           = pd.to_numeric(df["list_price"], errors="coerce")
    df[f"list_price_{TARGET_CURRENCY}"] = (df["list_price_usd"] * target_rate).round(2)

    # Final price calculation: price × qty × (1 – discount)
    qty      = pd.to_numeric(df["quantity"],  errors="coerce").fillna(1)
    discount = pd.to_numeric(df["discount"],  errors="coerce").fillna(0)
    df["final_price_usd"] = (df["list_price_usd"] * qty * (1 - discount)).round(2)
    df[f"final_price_{TARGET_CURRENCY}"] = (df["list_price_usd"] * target_rate * qty * (1 - discount)).round(2)

    print(f"    ✓ Added list_price_{TARGET_CURRENCY}, final_price_usd, final_price_{TARGET_CURRENCY}")
    return df


# ── 3 – Delivery metrics ──────────────────────────────────────────────────────

def transform_delivery_metrics(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Add:
      latency_days       – calendar days from order_date to shipped_date
      is_late_delivery   – True when shipped_date > required_date
      delivery_status    – 'On-Time', 'Late', or 'Not Shipped'
    """
    print("  [T2] Computing delivery metrics …")
    df = orders.copy()

    # Ensure datetime types
    for col in ["order_date", "required_date", "shipped_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # latency_days: days between order placed and shipped
    shipped_known = df["shipped_date"].notna() & (df["shipped_date"] != "unknown")
    df["latency_days"] = np.where(
        shipped_known,
        (df["shipped_date"] - df["order_date"]).dt.days,
        np.nan,
    )

    # Late delivery flag
    df["is_late_delivery"] = np.where(
        shipped_known,
        df["shipped_date"] > df["required_date"],
        False,
    )

    # Human-readable delivery status
    def _status(row):
        if pd.isna(row["shipped_date"]) or row["shipped_date"] == "unknown":
            return "Not Shipped"
        return "Late" if row["is_late_delivery"] else "On-Time"

    df["delivery_status"] = df.apply(_status, axis=1)

    late_pct = df["is_late_delivery"].mean() * 100
    print(f"    ✓ latency_days, is_late_delivery, delivery_status added")
    print(f"    ℹ Late-delivery rate: {late_pct:.1f}%")
    return df


# ── 4 – Locality flag ─────────────────────────────────────────────────────────

def transform_locality_flag(orders: pd.DataFrame,
                             customers: pd.DataFrame,
                             stores: pd.DataFrame) -> pd.DataFrame:
    """
    Join customer state and store state to orders, then flag rows where
    the customer's state matches the store's state as 'local'.
    """
    print("  [T3] Adding locality flag …")
    df = orders.copy()

    # Pull only the columns we need
    cust_geo  = customers[["customer_id", "state"]].rename(columns={"state": "customer_state"})
    store_geo = stores[["store_id",    "state"]].rename(columns={"state": "store_state"})

    df = df.merge(cust_geo,  on="customer_id", how="left")
    df = df.merge(store_geo, on="store_id",    how="left")

    df["is_local_customer"] = (
        df["customer_state"].str.upper() == df["store_state"].str.upper()
    )

    local_pct = df["is_local_customer"].mean() * 100
    print(f"    ✓ is_local_customer added  (local rate: {local_pct:.1f}%)")
    return df


# ── 5 – Order-status lookup ───────────────────────────────────────────────────

def transform_order_status(orders: pd.DataFrame) -> pd.DataFrame:
    """Replace numeric order_status with its descriptive label."""
    print("  [T4] Resolving order_status labels …")
    df = orders.copy()
    df["order_status"]       = pd.to_numeric(df["order_status"], errors="coerce")
    df["order_status_label"] = df["order_status"].map(ORDER_STATUS_MAP).fillna("Unknown")
    print("    ✓ order_status_label added")
    return df


# ── 6 – Product enrichment ────────────────────────────────────────────────────

def transform_products(products: pd.DataFrame,
                        brands: pd.DataFrame,
                        categories: pd.DataFrame) -> pd.DataFrame:
    """Enrich products with brand_name and category_name."""
    print("  [T5] Enriching products with brand/category names …")
    df = products.merge(brands,     on="brand_id",    how="left")
    df = df.merge(categories, on="category_id", how="left")

    # Drop metadata columns from DeltaLake if present (avoid duplication)
    drop_cols = [c for c in ["extraction_timestamp", "data_source"] if c in df.columns]
    df = df.drop(columns=drop_cols)
    print("    ✓ brand_name, category_name joined")
    return df


# ── Master transform runner ───────────────────────────────────────────────────

def run_transformations(datasets: dict) -> dict:
    """
    Apply all transformation steps to the cleaned staging_1 datasets and
    persist results to staging_2.

    Parameters
    ----------
    datasets : dict[str, DataFrame]  — cleaned DataFrames from quality_checks

    Returns
    -------
    dict[str, DataFrame]  — transformed DataFrames
    """
    print("\n" + "="*60)
    print("  MILESTONE 3 — TRANSFORMATIONS")
    print("="*60)

    # Convenience references
    orders        = datasets["orders"].copy()
    order_items   = datasets["order_items"].copy()
    customers     = datasets["customers"].copy()
    stores        = datasets["stores"].copy()
    products      = datasets["products"].copy()
    brands        = datasets["brands"].copy()
    categories    = datasets["categories"].copy()
    exchange_rates= datasets["exchange_rates"].copy()

    # Fix Unix timestamps on DB tables
    orders      = _fix_extraction_date(orders)
    order_items = _fix_extraction_date(order_items)

    # Apply transformations
    order_items = transform_currency(order_items, exchange_rates)
    orders      = transform_delivery_metrics(orders)
    orders      = transform_locality_flag(orders, customers, stores)
    orders      = transform_order_status(orders)
    products    = transform_products(products, brands, categories)

    transformed = {
        **datasets,                   # keep untouched datasets (brands, etc.)
        "orders":      orders,
        "order_items": order_items,
        "products":    products,
    }

    # Write all transformed datasets to staging_2
    print("\n  Persisting to staging_2 …")
    for name, df in transformed.items():
        out_path = os.path.join(STAGING_2_DIR, f"{name}_transformed.csv")
        df.to_csv(out_path, index=False)
        print(f"  → {name}_transformed.csv  ({df.shape[0]} rows × {df.shape[1]} cols)")

    print(f"\n[TRANSFORM] Done.\n")
    return transformed
