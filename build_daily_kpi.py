import os
import pandas as pd

# =========================
# 0) Path config
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ORDERS_FILE   = os.path.join(BASE_DIR, "olist_orders_dataset.csv")
ITEMS_FILE    = os.path.join(BASE_DIR, "olist_order_items_dataset.csv")
PAYMENTS_FILE = os.path.join(BASE_DIR, "olist_order_payments_dataset.csv")  # optional

# Output (production-friendly names)
OUT_CSV  = os.path.join(BASE_DIR, "daily_ops_metrics.csv")
OUT_XLSX = os.path.join(BASE_DIR, "daily_ops_metrics.xlsx")

# =========================
# 1) Load orders (minimal cols)
# =========================
if not os.path.exists(ORDERS_FILE):
    raise FileNotFoundError(f"Missing: {ORDERS_FILE}")

orders = pd.read_csv(
    ORDERS_FILE,
    usecols=[
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
    ],
)

orders["order_purchase_timestamp"] = pd.to_datetime(
    orders["order_purchase_timestamp"], errors="coerce"
)
orders["purchase_date"] = orders["order_purchase_timestamp"].dt.date

# Use full available range (simulation mode B needs full history)
orders = orders.dropna(subset=["order_purchase_timestamp"]).copy()
if orders.empty:
    raise ValueError("No valid order_purchase_timestamp after parsing.")

min_ts = orders["order_purchase_timestamp"].min()
max_ts = orders["order_purchase_timestamp"].max()
print(f"Data coverage (orders): {min_ts} -> {max_ts}")

# =========================
# 2) Load items (minimal cols) and join to orders
# =========================
if not os.path.exists(ITEMS_FILE):
    raise FileNotFoundError(f"Missing: {ITEMS_FILE}")

items = pd.read_csv(
    ITEMS_FILE,
    usecols=["order_id", "price", "freight_value"],
)

# Keep only items that belong to orders we have
items = items.merge(
    orders[["order_id", "purchase_date"]],
    on="order_id",
    how="inner",
)

# Items-based revenue definition (includes freight as you did)
items["item_revenue"] = items["price"].fillna(0) + items["freight_value"].fillna(0)

daily_revenue_items = (
    items.groupby("purchase_date", as_index=False)["item_revenue"]
    .sum()
    .rename(columns={"purchase_date": "date", "item_revenue": "revenue_items"})
)

# =========================
# 3) Daily orders + canceled orders
# =========================
# Note: canceled_orders here = count of orders whose status is canceled/unavailable
orders_daily = (
    orders.groupby("purchase_date", as_index=False)
    .agg(
        orders_count=("order_id", "nunique"),
        canceled_orders=("order_status", lambda s: (s.isin(["canceled", "unavailable"])).sum()),
    )
    .rename(columns={"purchase_date": "date"})
)

# =========================
# 4) (Optional) Payments-based revenue
# =========================
has_payments = os.path.exists(PAYMENTS_FILE)

if has_payments:
    payments = pd.read_csv(PAYMENTS_FILE, usecols=["order_id", "payment_value"])

    payments = payments.merge(
        orders[["order_id", "purchase_date"]],
        on="order_id",
        how="inner",
    )

    daily_revenue_payments = (
        payments.groupby("purchase_date", as_index=False)["payment_value"]
        .sum()
        .rename(columns={"purchase_date": "date", "payment_value": "revenue_payments"})
    )
else:
    daily_revenue_payments = None

# =========================
# 5) Combine into final daily KPI
# =========================
daily = orders_daily.merge(daily_revenue_items, on="date", how="left")

if daily_revenue_payments is not None:
    daily = daily.merge(daily_revenue_payments, on="date", how="left")

# Choose one "official revenue" for anomaly detection
# - If payments exist, prefer payments as official (cash-based-ish)
# - Else fall back to items
if has_payments:
    daily["revenue"] = daily["revenue_payments"].fillna(0)
else:
    daily["revenue"] = daily["revenue_items"].fillna(0)

daily["avg_order_value"] = (daily["revenue"] / daily["orders_count"]).round(2)

# Format date as ISO string
daily["date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y-%m-%d")

# Sort
daily = daily.sort_values("date").reset_index(drop=True)

# =========================
# 6) Basic sanity checks
# =========================
# =========================
# 6.5) Fill missing calendar days (calendar spine)
# =========================
daily["date"] = pd.to_datetime(daily["date"])

# Build full calendar range
full_range = pd.date_range(
    start=daily["date"].min(),
    end=daily["date"].max(),
    freq="D"
)

daily = (
    daily.set_index("date")
         .reindex(full_range)
         .reset_index()
         .rename(columns={"index": "date"})
)

# Fill missing days with zeros
zero_cols = ["orders_count", "revenue", "canceled_orders", "avg_order_value"]
for c in zero_cols:
    if c in daily.columns:
        daily[c] = daily[c].fillna(0)

# Fill audit columns if they exist
for c in ["revenue_items", "revenue_payments"]:
    if c in daily.columns:
        daily[c] = daily[c].fillna(0)

# Recompute AOV safely
daily["avg_order_value"] = daily.apply(
    lambda r: round(r["revenue"] / r["orders_count"], 2)
    if r["orders_count"] > 0 else 0,
    axis=1
)

# Back to string date
daily["date"] = daily["date"].dt.strftime("%Y-%m-%d")

cols = ["date", "orders_count", "revenue", "canceled_orders", "avg_order_value"]
if has_payments:
    cols += ["revenue_items", "revenue_payments"]

daily = daily[cols]

# =========================
# 7) Export
# =========================
daily.to_csv(OUT_CSV, index=False)
daily.to_excel(OUT_XLSX, index=False)

print("✅ Done!")
print(f"- Rows (days): {len(daily)}")
print(f"- Output CSV : {OUT_CSV}")
print(f"- Output XLSX: {OUT_XLSX}")
print(f"- Payments file found: {has_payments}")
