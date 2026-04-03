# =============================================================================
# extract.py — Milestone 1: Extract Data from Various Sources
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================
"""
Sources:
  1. DB_Tables   (CSV files simulating a relational DB via sqlite3 in-memory)
  2. DeltaLake   (CSV files in a local folder hierarchy)
  3. API         (openexchangerates.org – latest USD exchange rates)

Each extracted DataFrame is enriched with:
  - extraction_timestamp : ISO-8601 string of when the row was extracted
  - data_source          : label identifying the source system
"""

import os
import glob
import sqlite3
import datetime
import requests
import pandas as pd

from config import (
    DB_TABLES_DIR, DELTA_LAKE_DIR,
    OPEN_EXCHANGE_APP_ID, EXCHANGE_RATE_URL, FALLBACK_EXCHANGE_RATES,
)

# ── Metadata helpers ──────────────────────────────────────────────────────────

def _add_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Append extraction_timestamp and data_source columns to any DataFrame."""
    df = df.copy()
    df["extraction_timestamp"] = datetime.datetime.now().isoformat(timespec="seconds")
    df["data_source"] = source
    return df


# ── Source 1 : DB Tables (SQLite in-memory) ───────────────────────────────────

def _load_csv_to_sqlite(conn: sqlite3.Connection, csv_path: str, table_name: str) -> None:
    """Load a CSV into an in-memory SQLite table."""
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def extract_db_tables() -> dict[str, pd.DataFrame]:
    
    print("[EXTRACT] Loading DB_Tables into in-memory SQLite …")
    conn = sqlite3.connect(":memory:")

    csv_files = glob.glob(os.path.join(DB_TABLES_DIR, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DB_TABLES_DIR}")

    table_names = []
    for csv_path in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_path))[0]
        _load_csv_to_sqlite(conn, csv_path, table_name)
        table_names.append(table_name)
        print(f"  • Loaded '{table_name}' ({pd.read_csv(csv_path).shape[0]} rows)")

    results = {}
    for table_name in table_names:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        df = _add_metadata(df, source="SQLite/DB_Tables")
        results[table_name] = df
        print(f"  ✓ Extracted '{table_name}': {df.shape}")

    conn.close()
    return results


# ── Source 2 : Data Lake (local folder) ───────────────────────────────────────

def extract_delta_lake() -> dict[str, pd.DataFrame]:
    """
    Walk the DeltaLake directory and read every CSV/JSON/Parquet file found,
    returning one DataFrame per file (keyed by file stem).
    """
    print("[EXTRACT] Reading DeltaLake files …")
    patterns = ["*.csv", "*.json", "*.parquet"]
    results = {}

    for pattern in patterns:
        for file_path in glob.glob(os.path.join(DELTA_LAKE_DIR, "**", pattern), recursive=True):
            name = os.path.splitext(os.path.basename(file_path))[0]
            ext  = os.path.splitext(file_path)[1].lower()

            if ext == ".csv":
                df = pd.read_csv(file_path)
            elif ext == ".json":
                df = pd.read_json(file_path)
            elif ext == ".parquet":
                df = pd.read_parquet(file_path)
            else:
                continue

            df = _add_metadata(df, source="DeltaLake")
            results[name] = df
            print(f"  ✓ Extracted '{name}': {df.shape}")

    if not results:
        raise FileNotFoundError(f"No supported files found in {DELTA_LAKE_DIR}")

    return results


# ── Source 3 : Exchange-Rate API ──────────────────────────────────────────────

def extract_exchange_rates() -> pd.DataFrame:
    """
    Fetch latest USD-base exchange rates from openexchangerates.org.
    Falls back to hard-coded rates when the App ID is not configured or
    the HTTP request fails.

    Returns
    -------
    DataFrame with columns: currency_code, rate_to_usd,
                            extraction_timestamp, data_source
    """
    print("[EXTRACT] Fetching exchange rates from API …")
    rates = None

    if OPEN_EXCHANGE_APP_ID and OPEN_EXCHANGE_APP_ID != "YOUR_APP_ID_HERE":
        try:
            resp = requests.get(EXCHANGE_RATE_URL, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            rates = payload.get("conversion_rates", {})
            print(f"  ✓ API call succeeded – {len(rates)} currencies retrieved")
        except Exception as exc:
            print(f"  ⚠ API call failed ({exc}). Using fallback rates.")

    if rates is None:
        print("  ℹ Using hardcoded fallback exchange rates (USD base).")
        rates = FALLBACK_EXCHANGE_RATES

    df = pd.DataFrame(
        [{"currency_code": k, "rate_to_usd": v} for k, v in rates.items()]
    )
    df = _add_metadata(df, source="openexchangerates.org")
    print(f"  ✓ Exchange rates DataFrame: {df.shape}")
    return df


# ── Master Extract ─────────────────────────────────────────────────────────────

def run_extraction() -> dict[str, pd.DataFrame]:
    """
    Run all three extraction routines and return a single dict of DataFrames.
    Keys: 'order_items', 'orders', 'brands', 'categories', 'customers',
          'products', 'staffs', 'stocks', 'stores', 'exchange_rates'
    """
    print("\n" + "="*60)
    print("  MILESTONE 1 — EXTRACT")
    print("="*60)

    extracted = {}
    extracted.update(extract_db_tables())
    extracted.update(extract_delta_lake())
    extracted["exchange_rates"] = extract_exchange_rates()

    print(f"\n[EXTRACT] Done — {len(extracted)} datasets extracted.\n")
    return extracted
