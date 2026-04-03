# =============================================================================
# config.py — Central Configuration for the ETL Pipeline
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================

import os

# ── Project Root ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Source Folders ────────────────────────────────────────────────────────────
DB_TABLES_DIR   = os.path.join(BASE_DIR, "DB_Tables")
DELTA_LAKE_DIR  = os.path.join(BASE_DIR, "DeltaLake")

# ── Staging Folders ───────────────────────────────────────────────────────────
STAGING_1_DIR   = os.path.join(BASE_DIR, "staging_1")
STAGING_2_DIR   = os.path.join(BASE_DIR, "staging_2")

# ── Output Folders ────────────────────────────────────────────────────────────
INFO_MART_DIR   = os.path.join(BASE_DIR, "Information_Mart")
VIZ_DIR         = os.path.join(BASE_DIR, "Visualization")

# ── API Configuration ─────────────────────────────────────────────────────────

API_KEY = "a85f010497e48add32de3901"

OPEN_EXCHANGE_APP_ID = API_KEY

BASE_CURRENCY = "USD"

EXCHANGE_RATE_URL = (
    f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{BASE_CURRENCY}"
)

# Fallback rates (USD base) used when the API key is not set or call fails
FALLBACK_EXCHANGE_RATES = {
    "USD": 1.0,
    "EGP": 48.65,   # Egyptian Pound
    "EUR": 0.92,
    "GBP": 0.79,
    "SAR": 3.75,
    "AED": 3.67,
}

# Target currency for local-price conversion
TARGET_CURRENCY = "EGP"

# ── Business Rules ────────────────────────────────────────────────────────────
# Order status lookup table
ORDER_STATUS_MAP = {
    1: "Pending",
    2: "Processing",
    3: "Rejected",
    4: "Completed",
}

# Price validation range (USD)
PRICE_MIN = 0.01
PRICE_MAX = 100_000.0

# Discount range (0–1 as a fraction)
DISCOUNT_MIN = 0.0
DISCOUNT_MAX = 1.0

# Quantity validation
QTY_MIN = 1
QTY_MAX = 1_000

# Date sanity window (orders should fall inside this range)
ORDER_DATE_MIN = "2015-01-01"
ORDER_DATE_MAX = "2020-12-31"

# ── Locality Rule ─────────────────────────────────────────────────────────────
# A customer is considered "local" when they share the same STATE as their store
LOCALITY_MATCH_FIELD = "state"

# ── Ensure all output directories exist ──────────────────────────────────────
for _dir in (STAGING_1_DIR, STAGING_2_DIR, INFO_MART_DIR, VIZ_DIR):
    os.makedirs(_dir, exist_ok=True)
