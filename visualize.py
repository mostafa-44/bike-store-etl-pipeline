# =============================================================================
# visualize.py — Milestone 5: BI Reporting & Visualizations
# DataCraft Academy | End-to-End ETL Pipeline Project
# =============================================================================
"""
Charts generated (saved to Visualization/):
  1. monthly_sales_trend.png       — Time-series: monthly revenue over time
  2. top10_products.png            — Top 10 products by total revenue
  3. sales_by_category.png         — Revenue breakdown by bike category
  4. order_status_distribution.png — Donut chart of order status mix
  5. delivery_performance.png      — On-time vs late by store (stacked bar)
  6. customer_state_distribution.png — Top customer states (horizontal bar)
  7. yearly_revenue_trend.png      — Year-over-year revenue comparison
  8. discount_vs_revenue.png       — Scatter: discount rate vs final price
"""

import os
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")          # non-interactive backend for server rendering
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

from config import VIZ_DIR, TARGET_CURRENCY

warnings.filterwarnings("ignore")

# ── Global style ──────────────────────────────────────────────────────────────
PALETTE   = "viridis"
ACCENT    = "#2563EB"   
BG_COLOR  = "#F8FAFC"
GRID_CLR  = "#E2E8F0"
TEXT_CLR  = "#1E293B"

plt.rcParams.update({
    "figure.facecolor":  BG_COLOR,
    "axes.facecolor":    BG_COLOR,
    "axes.edgecolor":    GRID_CLR,
    "axes.labelcolor":   TEXT_CLR,
    "axes.titlesize":    13,
    "axes.titleweight":  "bold",
    "axes.titlepad":     12,
    "xtick.color":       TEXT_CLR,
    "ytick.color":       TEXT_CLR,
    "grid.color":        GRID_CLR,
    "grid.linewidth":    0.8,
    "font.family":       "DejaVu Sans",
    "figure.dpi":        150,
})

def _save(fig, filename: str):
    path = os.path.join(VIZ_DIR, filename)
    fig.savefig(path, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    print(f"  ✓ Saved: {filename}")
    return path


def _fmt_millions(x, _):
    if x >= 1_000_000:
        return f"${x/1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"


# ── Chart 1: Monthly Sales Trend ──────────────────────────────────────────────

def chart_monthly_sales(fact_sales: pd.DataFrame, dim_date: pd.DataFrame):
    print("  [VIZ 1] Monthly Sales Trend …")
    df = fact_sales.merge(dim_date[["date_id", "year", "month", "month_name"]],
                          on="date_id", how="left")
    df = df.dropna(subset=["year", "month"])
    df["period"] = pd.to_datetime(
        df["year"].astype(int).astype(str) + "-" +
        df["month"].astype(int).astype(str).str.zfill(2)
    )
    monthly = df.groupby("period")["final_price_usd"].sum().reset_index()
    monthly = monthly.sort_values("period")

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(monthly["period"], monthly["final_price_usd"],
            color=ACCENT, linewidth=2.2, marker="o", markersize=4, zorder=3)
    ax.fill_between(monthly["period"], monthly["final_price_usd"],
                    alpha=0.12, color=ACCENT)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    ax.set_title("Monthly Sales Revenue (USD)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue (USD)")
    ax.grid(axis="y", linestyle="--")
    fig.autofmt_xdate(rotation=30)
    _save(fig, "1_monthly_sales_trend.png")


# ── Chart 2: Top 10 Products by Revenue ──────────────────────────────────────

def chart_top10_products(fact_sales: pd.DataFrame, dim_product: pd.DataFrame):
    print("  [VIZ 2] Top 10 Products …")
    df = fact_sales.merge(dim_product[["product_id", "product_name"]],
                          on="product_id", how="left")
    top10 = (df.groupby("product_name")["final_price_usd"]
               .sum()
               .nlargest(10)
               .sort_values())

    colors = sns.color_palette(PALETTE, len(top10))
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(top10.index, top10.values, color=colors, edgecolor="none")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    for bar, val in zip(bars, top10.values):
        ax.text(val * 1.01, bar.get_y() + bar.get_height()/2,
                _fmt_millions(val, None), va="center", fontsize=8.5)
    ax.set_title("Top 10 Products by Revenue")
    ax.set_xlabel("Total Revenue (USD)")
    ax.grid(axis="x", linestyle="--")
    _save(fig, "2_top10_products.png")


# ── Chart 3: Revenue by Category ─────────────────────────────────────────────

def chart_sales_by_category(fact_sales: pd.DataFrame, dim_product: pd.DataFrame):
    print("  [VIZ 3] Revenue by Category …")
    df = fact_sales.merge(dim_product[["product_id", "category_name"]],
                          on="product_id", how="left")
    cat_rev = (df.groupby("category_name")["final_price_usd"]
                 .sum()
                 .sort_values(ascending=False))

    colors = sns.color_palette(PALETTE, len(cat_rev))
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(cat_rev.index, cat_rev.values, color=colors, edgecolor="none")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    for bar, val in zip(bars, cat_rev.values):
        ax.text(bar.get_x() + bar.get_width()/2, val * 1.005,
                _fmt_millions(val, None), ha="center", va="bottom", fontsize=8.5)
    ax.set_title("Sales Revenue by Bike Category")
    ax.set_ylabel("Total Revenue (USD)")
    ax.set_xlabel("Category")
    plt.xticks(rotation=20, ha="right")
    ax.grid(axis="y", linestyle="--")
    _save(fig, "3_sales_by_category.png")


# ── Chart 4: Order Status Distribution ───────────────────────────────────────

def chart_order_status(dim_order: pd.DataFrame):
    print("  [VIZ 4] Order Status Distribution …")
    status_counts = dim_order["order_status_label"].value_counts()

    colors = sns.color_palette(PALETTE, len(status_counts))
    fig, ax = plt.subplots(figsize=(7, 7))
    wedges, texts, autotexts = ax.pie(
        status_counts.values,
        labels=status_counts.index,
        colors=colors,
        autopct="%1.1f%%",
        startangle=140,
        wedgeprops={"edgecolor": "white", "linewidth": 2},
        pctdistance=0.75,
    )
    # Draw inner circle for donut effect
    centre_circle = plt.Circle((0, 0), 0.55, fc=BG_COLOR)
    ax.add_patch(centre_circle)
    for text in autotexts:
        text.set_fontsize(10)
        text.set_color(TEXT_CLR)
    ax.set_title("Order Status Distribution")
    _save(fig, "4_order_status_distribution.png")


# ── Chart 5: Delivery Performance by Store ───────────────────────────────────

def chart_delivery_performance(dim_order: pd.DataFrame,
                                orders_raw: pd.DataFrame,
                                dim_store: pd.DataFrame):
    print("  [VIZ 5] Delivery Performance by Store …")
    df = orders_raw[["order_id", "store_id"]].merge(
        dim_order[["order_id", "delivery_status"]], on="order_id", how="left"
    ).merge(dim_store[["store_id", "store_name"]], on="store_id", how="left")

    perf = (df.groupby(["store_name", "delivery_status"])
              .size()
              .unstack(fill_value=0))

    colors_map = {"On-Time": "#22C55E", "Late": "#EF4444", "Not Shipped": "#94A3B8"}
    colors = [colors_map.get(c, ACCENT) for c in perf.columns]

    fig, ax = plt.subplots(figsize=(9, 5))
    perf.plot(kind="bar", stacked=True, ax=ax, color=colors,
              edgecolor="none", width=0.55)
    ax.set_title("Delivery Performance by Store")
    ax.set_xlabel("Store")
    ax.set_ylabel("Number of Orders")
    plt.xticks(rotation=10, ha="right")
    ax.legend(title="Status", bbox_to_anchor=(1.01, 1), loc="upper left")
    ax.grid(axis="y", linestyle="--")
    _save(fig, "5_delivery_performance.png")


# ── Chart 6: Top Customer States ─────────────────────────────────────────────

def chart_customer_states(dim_customer: pd.DataFrame):
    print("  [VIZ 6] Customer Distribution by State …")
    state_counts = (dim_customer["state"]
                    .str.upper()
                    .value_counts()
                    .head(12)
                    .sort_values())

    colors = sns.color_palette(PALETTE, len(state_counts))
    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.barh(state_counts.index, state_counts.values,
                   color=colors, edgecolor="none")
    for bar, val in zip(bars, state_counts.values):
        ax.text(val + 2, bar.get_y() + bar.get_height()/2,
                str(val), va="center", fontsize=9)
    ax.set_title("Top 12 States by Customer Count")
    ax.set_xlabel("Number of Customers")
    ax.grid(axis="x", linestyle="--")
    _save(fig, "6_customer_state_distribution.png")


# ── Chart 7: Year-over-Year Revenue ──────────────────────────────────────────

def chart_yoy_revenue(fact_sales: pd.DataFrame, dim_date: pd.DataFrame):
    print("  [VIZ 7] Year-over-Year Revenue …")
    df = fact_sales.merge(dim_date[["date_id", "year"]], on="date_id", how="left")
    yearly = df.groupby("year")["final_price_usd"].sum().reset_index()
    yearly = yearly.sort_values("year")

    colors = sns.color_palette(PALETTE, len(yearly))
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(yearly["year"].astype(str), yearly["final_price_usd"],
                  color=colors, edgecolor="none", width=0.5)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    for bar, val in zip(bars, yearly["final_price_usd"]):
        ax.text(bar.get_x() + bar.get_width()/2, val * 1.01,
                _fmt_millions(val, None), ha="center", va="bottom", fontsize=9)
    ax.set_title("Year-over-Year Total Revenue")
    ax.set_xlabel("Year")
    ax.set_ylabel("Total Revenue (USD)")
    ax.grid(axis="y", linestyle="--")
    _save(fig, "7_yearly_revenue_trend.png")


# ── Chart 8: Discount vs Revenue Scatter ─────────────────────────────────────

def chart_discount_vs_revenue(fact_sales: pd.DataFrame, dim_product: pd.DataFrame):
    print("  [VIZ 8] Discount Rate vs Final Price …")
    df = fact_sales.merge(dim_product[["product_id", "category_name"]],
                          on="product_id", how="left").dropna(
        subset=["discount", "final_price_usd", "category_name"]
    )
    # Aggregate to product level to avoid over-plotting
    agg = (df.groupby(["product_id", "category_name"])
             .agg(avg_discount=("discount", "mean"),
                  total_revenue=("final_price_usd", "sum"))
             .reset_index())

    cats    = agg["category_name"].unique()
    palette = dict(zip(cats, sns.color_palette(PALETTE, len(cats))))

    fig, ax = plt.subplots(figsize=(9, 6))
    for cat, grp in agg.groupby("category_name"):
        ax.scatter(grp["avg_discount"] * 100, grp["total_revenue"],
                   label=cat, color=palette[cat], alpha=0.75, s=55,
                   edgecolors="none")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    ax.set_title("Average Discount (%) vs Total Revenue per Product")
    ax.set_xlabel("Average Discount (%)")
    ax.set_ylabel("Total Revenue (USD)")
    ax.legend(title="Category", bbox_to_anchor=(1.01, 1), loc="upper left",
              fontsize=8)
    ax.grid(linestyle="--")
    _save(fig, "8_discount_vs_revenue.png")


# ── Master visualization runner ───────────────────────────────────────────────

def run_visualizations(model: dict, datasets: dict):
    """
    Generate all 8 charts and save them to the Visualization folder.

    Parameters
    ----------
    model    : dict — tables from data_model.run_data_model()
    datasets : dict — transformed DataFrames from transform.run_transformations()
    """
    print("\n" + "="*60)
    print("  MILESTONE 5 — VISUALIZATIONS")
    print("="*60 + "\n")

    fact_sales   = model["fact_sales"]
    dim_date     = model["dim_date"]
    dim_product  = model["dim_product"]
    dim_store    = model["dim_store"]
    dim_order    = model["dim_order"]
    dim_customer = model["dim_customer"]
    orders       = datasets["orders"]

    chart_monthly_sales(fact_sales, dim_date)
    chart_top10_products(fact_sales, dim_product)
    chart_sales_by_category(fact_sales, dim_product)
    chart_order_status(dim_order)
    chart_delivery_performance(dim_order, orders, dim_store)
    chart_customer_states(dim_customer)
    chart_yoy_revenue(fact_sales, dim_date)
    chart_discount_vs_revenue(fact_sales, dim_product)

    print(f"\n[VIZ] All charts saved to: {VIZ_DIR}\n")
