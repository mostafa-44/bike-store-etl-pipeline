# 🚲 Bike Store ETL Pipeline
### DataCraft Academy — End-to-End Python Data Engineering Project

> A complete ETL pipeline that extracts data from multiple sources, cleans it, transforms it, builds a Star Schema data warehouse, and generates business intelligence charts — all in Python.

---

## 👥 Team Members
| Name | Role |
|---|---|
| Menna Allah | Data Engineer |
| Mustafa Atef | Data Engineer |

---

## 📊 Project Overview

This project processes data from a fictional bike store chain operating across **3 U.S. states (2016–2018)**. The pipeline takes raw data from a database, a data lake, and a live API — and turns it into a fully structured MySQL data warehouse with BI charts.

| Metric | Value |
|---|---|
| 💰 Total Revenue | $8.77M |
| 📦 Total Orders | 1,445 |
| 🛒 Fact Records | 4,729 |
| ⚠️ Late Deliveries | 31.7% |
| 🧹 Duplicates Removed | 47 |
| 🔧 Null Values Fixed | 1,275 |

---

## 🗂️ Project Structure

```
ETL_Pipeline/
│
├── main.py                 # ← Run this to execute the full pipeline
├── config.py               # Central config: paths, DB credentials, business rules
│
├── extract.py              # Milestone 1 — Extract from DB, Data Lake, API
├── quality_checks.py       # Milestone 2 — Clean & validate → staging_1/
├── transform.py            # Milestone 3 — Enrich & calculate → staging_2/
├── data_model.py           # Milestone 4 — Build Star Schema → Information_Mart/
├── visualize.py            # Milestone 5 — Generate BI charts → Visualization/
├── load_mysql.py           # Milestone 6 — Load into MySQL DWH
│
├── DB_Tables/              # Source: orders & order_items (simulates a database)
│   ├── orders.csv
│   └── order_items.csv
│
├── DeltaLake/              # Source: reference tables (simulates a data lake)
│   ├── brands.csv
│   ├── categories.csv
│   ├── customers.csv
│   ├── products.csv
│   ├── staffs.csv
│   ├── stocks.csv
│   └── stores.csv
│
└── SQL_DWH/                # MySQL scripts to build the DWH from scratch
    ├── 01_create_tables.sql
    ├── 02_load_data.sql
    ├── 03_views.sql
    └── 04_sample_queries.sql
```

> **Output folders** (`staging_1/`, `staging_2/`, `Information_Mart/`, `Visualization/`) are generated automatically when you run the pipeline — they are excluded from this repo via `.gitignore`.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│  SQLite (DB_Tables)  │  CSV Files (DeltaLake)  │  API   │
└──────────────────────┴─────────────────────────┴────────┘
                            │
                     [ extract.py ]
                            │
                     [ quality_checks.py ]  →  staging_1/
                            │
                     [ transform.py ]       →  staging_2/
                            │
                     [ data_model.py ]      →  Information_Mart/
                            │
              ┌─────────────┴──────────────┐
       [ visualize.py ]            [ load_mysql.py ]
       Visualization/              bike_store_dw (MySQL)
```

---

## ⭐ Star Schema (Data Model)

```
                    dim_date
                       │
   dim_customer ── fact_sales ── dim_product
                       │
   dim_store    ── fact_sales ── dim_staff
                       │
                    dim_order
```

| Table | Type | Rows | Description |
|---|---|---|---|
| `fact_sales` | Fact | 4,729 | One row per order line-item |
| `dim_date` | Dimension | 680 | Calendar breakdown |
| `dim_customer` | Dimension | 1,445 | Customer master data |
| `dim_product` | Dimension | 321 | Products with brand & category |
| `dim_store` | Dimension | 3 | Store locations |
| `dim_staff` | Dimension | 9 | Staff members |
| `dim_order` | Dimension | 1,445 | Order status & delivery info |

---

## 🚀 How to Run

### 1. Install dependencies
```bash
pip install pandas numpy matplotlib seaborn requests mysql-connector-python
```

### 2. Configure MySQL (optional)
Open `config.py` and update your credentials:
```python
MYSQL_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "your_password",   # ← change this
    "database": "bike_store_dw",
}
```

### 3. Run the pipeline
```bash
# Full pipeline (includes MySQL load)
python main.py

# Skip MySQL (CSV outputs only)
python main.py --no-db
```

### 4. Expected output
```
✅ Pipeline completed successfully in ~3s

  staging_1/        → 10 cleaned CSVs + quality report
  staging_2/        → 10 transformed CSVs
  Information_Mart/ → 7 Star Schema tables
  Visualization/    → 8 BI charts (PNG)
  MySQL             → bike_store_dw (7 tables + 6 views)
```

---

## 🗄️ MySQL DWH — Run SQL Scripts

If you prefer pure SQL (without Python):

```bash
mysql -u root -p < SQL_DWH/01_create_tables.sql
mysql -u root -p bike_store_dw < SQL_DWH/02_load_data.sql
mysql -u root -p bike_store_dw < SQL_DWH/03_views.sql
```

Then test with:
```sql
USE bike_store_dw;
SELECT * FROM vw_yearly_revenue;
SELECT * FROM vw_top_products LIMIT 10;
SELECT * FROM vw_delivery_performance;
```

---

## 📈 BI Charts Generated

| # | Chart | Insight |
|---|---|---|
| 1 | Monthly Sales Trend | Revenue grew 86% from 2016 to 2017 |
| 2 | Top 10 Products | Trek Slash 8 27.5 = $555K alone |
| 3 | Revenue by Category | Mountain Bikes dominate |
| 4 | Order Status Distribution | Most orders completed successfully |
| 5 | Delivery Performance by Store | 31.7% late rate across all stores |
| 6 | Customer Distribution by State | NY, CA, TX are top markets |
| 7 | Year-over-Year Revenue | 2017 peak year at $4.41M |
| 8 | Discount vs Revenue | No correlation — discounts applied uniformly |

---

## 🔑 API Setup (Optional)

To use live exchange rates instead of fallback values:
1. Register free at [openexchangerates.org](https://openexchangerates.org)
2. Open `config.py` and set:
```python
OPEN_EXCHANGE_APP_ID = "your_app_id_here"
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Core language |
| pandas | Data manipulation |
| numpy | Numerical operations |
| sqlite3 | In-memory database extraction |
| requests | API calls |
| matplotlib | Charts |
| seaborn | Chart styling |
| mysql-connector-python | MySQL loading |
| MySQL 8.0+ | Data warehouse |

---

## 📄 License
This project was built as part of the **DataCraft Academy** data engineering curriculum.
