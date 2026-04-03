# =============================================================================
# data_model.py — Milestone 4: Star Schema Data Model → Information_Mart
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================
"""
Star Schema Design
==================

Fact Table
----------
  fact_sales         — one row per order line-item

Dimension Tables
----------------
  dim_date           — calendar attributes (from order_date)
  dim_customer       — customer master data
  dim_product        — product with brand & category
  dim_store          — store master data
  dim_staff          — staff member data
  dim_order          — order-level attributes (status, delivery info)

All tables are saved as CSVs in the Information_Mart folder.
"""

import os
import pandas as pd
import numpy as np

from config import INFO_MART_DIR, TARGET_CURRENCY


# ── dim_date ──────────────────────────────────────────────────────────────────

def build_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Build a date dimension from all unique dates found in orders.
    Generates year, quarter, month, month_name, week, day, day_name.
    """
    dates = pd.to_datetime(orders["order_date"], errors="coerce").dropna().unique()
    df = pd.DataFrame({"date": pd.DatetimeIndex(sorted(dates))})

    df["date_id"]    = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]       = df["date"].dt.year
    df["quarter"]    = df["date"].dt.quarter
    df["quarter_label"] = "Q" + df["quarter"].astype(str)
    df["month"]      = df["date"].dt.month
    df["month_name"] = df["date"].dt.strftime("%B")
    df["week"]       = df["date"].dt.isocalendar().week.astype(int)
    df["day"]        = df["date"].dt.day
    df["day_of_week"]= df["date"].dt.dayofweek          # Mon=0, Sun=6
    df["day_name"]   = df["date"].dt.strftime("%A")
    df["is_weekend"] = df["day_of_week"] >= 5

    return df[["date_id", "date", "year", "quarter", "quarter_label",
               "month", "month_name", "week", "day", "day_of_week",
               "day_name", "is_weekend"]]


# ── dim_customer ──────────────────────────────────────────────────────────────

def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    cols = ["customer_id", "first_name", "last_name",
            "email", "phone", "city", "state", "zip_code"]
    cols = [c for c in cols if c in customers.columns]
    df = customers[cols].drop_duplicates(subset=["customer_id"]).copy()
    df["full_name"] = (
        df["first_name"].fillna("") + " " + df["last_name"].fillna("")
    ).str.strip()
    return df


# ── dim_product ───────────────────────────────────────────────────────────────

def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    cols = ["product_id", "product_name", "brand_name",
            "category_name", "model_year", "list_price"]
    cols = [c for c in cols if c in products.columns]
    return products[cols].drop_duplicates(subset=["product_id"])


# ── dim_store ─────────────────────────────────────────────────────────────────

def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    cols = ["store_id", "store_name", "phone", "email",
            "street", "city", "state", "zip_code"]
    cols = [c for c in cols if c in stores.columns]
    return stores[cols].drop_duplicates(subset=["store_id"])


# ── dim_staff ─────────────────────────────────────────────────────────────────

def build_dim_staff(staffs: pd.DataFrame) -> pd.DataFrame:
    cols = ["staff_id", "first_name", "last_name", "email", "phone",
            "active", "store_id", "manager_id"]
    cols = [c for c in cols if c in staffs.columns]
    df = staffs[cols].drop_duplicates(subset=["staff_id"]).copy()
    df["full_name"] = (
        df["first_name"].fillna("") + " " + df["last_name"].fillna("")
    ).str.strip()
    return df


# ── dim_order ─────────────────────────────────────────────────────────────────

def build_dim_order(orders: pd.DataFrame) -> pd.DataFrame:
    """Captures order-level descriptive attributes."""
    cols = ["order_id", "order_status", "order_status_label",
            "order_date", "required_date", "shipped_date",
            "latency_days", "is_late_delivery", "delivery_status",
            "is_local_customer", "customer_state", "store_state"]
    cols = [c for c in cols if c in orders.columns]
    return orders[cols].drop_duplicates(subset=["order_id"])


# ── fact_sales ────────────────────────────────────────────────────────────────

def build_fact_sales(order_items: pd.DataFrame,
                     orders: pd.DataFrame,
                     dim_date: pd.DataFrame) -> pd.DataFrame:
    """
    Grain: one row per (order_id, item_id).

    Measures: quantity, list_price_usd, discount,
              final_price_usd, final_price_<TARGET_CURRENCY>

    FKs: date_id, customer_id, store_id, staff_id, product_id
    """
    # Join order-level FKs onto items
    order_keys = orders[["order_id", "customer_id", "store_id",
                          "staff_id", "order_date"]].drop_duplicates()
    df = order_items.merge(order_keys, on="order_id", how="left")

    # Resolve date_id
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    date_lookup = dim_date[["date", "date_id"]].copy()
    date_lookup["date"] = pd.to_datetime(date_lookup["date"])
    df = df.merge(date_lookup, left_on="order_date", right_on="date", how="left")

    # Select and order fact columns
    fact_cols = [
        "order_id", "item_id",           # grain
        "date_id",                        # FK → dim_date
        "customer_id",                    # FK → dim_customer
        "product_id",                     # FK → dim_product
        "store_id",                       # FK → dim_store
        "staff_id",                       # FK → dim_staff
        "quantity",
        "list_price_usd",
        "discount",
        "final_price_usd",
        f"final_price_{TARGET_CURRENCY}",
    ]
    fact_cols = [c for c in fact_cols if c in df.columns]
    df = df[fact_cols].copy()

    # Ensure numeric types
    for col in ["quantity", "list_price_usd", "discount",
                "final_price_usd", f"final_price_{TARGET_CURRENCY}"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ── Master data model runner ──────────────────────────────────────────────────

def run_data_model(datasets: dict) -> dict:
    """
    Build all dimension and fact tables and persist them to Information_Mart.

    Returns
    -------
    dict[str, DataFrame] — keys are table names (e.g. 'fact_sales', 'dim_date')
    """
    print("\n" + "="*60)
    print("  MILESTONE 4 — DATA MODEL (Star Schema)")
    print("="*60)

    orders     = datasets["orders"]
    order_items= datasets["order_items"]
    customers  = datasets["customers"]
    products   = datasets["products"]
    stores     = datasets["stores"]
    staffs     = datasets["staffs"]

    print("\n  Building dimension tables …")
    dim_date     = build_dim_date(orders)
    dim_customer = build_dim_customer(customers)
    dim_product  = build_dim_product(products)
    dim_store    = build_dim_store(stores)
    dim_staff    = build_dim_staff(staffs)
    dim_order    = build_dim_order(orders)

    print("\n  Building fact table …")
    fact_sales = build_fact_sales(order_items, orders, dim_date)

    model = {
        "fact_sales":    fact_sales,
        "dim_date":      dim_date,
        "dim_customer":  dim_customer,
        "dim_product":   dim_product,
        "dim_store":     dim_store,
        "dim_staff":     dim_staff,
        "dim_order":     dim_order,
    }

    # Persist to Information_Mart
    print("\n  Saving to Information_Mart …")
    for table_name, df in model.items():
        path = os.path.join(INFO_MART_DIR, f"{table_name}.csv")
        df.to_csv(path, index=False)
        prefix = "FACT" if table_name.startswith("fact") else "DIM "
        print(f"  [{prefix}] {table_name:<20} → {df.shape[0]:>6} rows × {df.shape[1]:>2} cols")

    # Print schema summary
    print("\n  ── Star Schema Summary ──────────────────────────────")
    print(f"  Fact table : fact_sales          ({fact_sales.shape[0]} records)")
    print(f"  Dimensions : dim_date      ({dim_date.shape[0]} unique dates)")
    print(f"             : dim_customer  ({dim_customer.shape[0]} customers)")
    print(f"             : dim_product   ({dim_product.shape[0]} products)")
    print(f"             : dim_store     ({dim_store.shape[0]} stores)")
    print(f"             : dim_staff     ({dim_staff.shape[0]} staff)")
    print(f"             : dim_order     ({dim_order.shape[0]} orders)")
    print(f"  ─────────────────────────────────────────────────────")

    print(f"\n[MODEL] Done.\n")
    return model
