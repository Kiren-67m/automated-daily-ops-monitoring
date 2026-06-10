"""Build daily operations KPIs from the Olist e-commerce dataset."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_DATA_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_DATA_DIR / "daily_ops_metrics.csv")
    parser.add_argument("--output-xlsx", type=Path, default=DEFAULT_DATA_DIR / "daily_ops_metrics.xlsx")
    return parser.parse_args()


def require_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return path


def build_daily_kpis(data_dir: Path) -> pd.DataFrame:
    orders_file = require_file(data_dir / "olist_orders_dataset.csv")
    items_file = require_file(data_dir / "olist_order_items_dataset.csv")
    payments_file = data_dir / "olist_order_payments_dataset.csv"

    orders = pd.read_csv(
        orders_file,
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
    orders = orders.dropna(subset=["order_purchase_timestamp"]).copy()
    if orders.empty:
        raise ValueError("No valid order_purchase_timestamp values found.")

    orders["purchase_date"] = orders["order_purchase_timestamp"].dt.date
    print(
        "Data coverage:",
        orders["order_purchase_timestamp"].min(),
        "to",
        orders["order_purchase_timestamp"].max(),
    )

    items = pd.read_csv(items_file, usecols=["order_id", "price", "freight_value"])
    items = items.merge(orders[["order_id", "purchase_date"]], on="order_id", how="inner")
    items["item_revenue"] = items["price"].fillna(0) + items["freight_value"].fillna(0)

    daily_revenue_items = (
        items.groupby("purchase_date", as_index=False)["item_revenue"]
        .sum()
        .rename(columns={"purchase_date": "date", "item_revenue": "revenue_items"})
    )

    orders_daily = (
        orders.groupby("purchase_date", as_index=False)
        .agg(
            orders_count=("order_id", "nunique"),
            canceled_orders=(
                "order_status",
                lambda status: status.isin(["canceled", "unavailable"]).sum(),
            ),
        )
        .rename(columns={"purchase_date": "date"})
    )

    daily = orders_daily.merge(daily_revenue_items, on="date", how="left")

    if payments_file.exists():
        payments = pd.read_csv(payments_file, usecols=["order_id", "payment_value"])
        payments = payments.merge(
            orders[["order_id", "purchase_date"]], on="order_id", how="inner"
        )
        daily_revenue_payments = (
            payments.groupby("purchase_date", as_index=False)["payment_value"]
            .sum()
            .rename(columns={"purchase_date": "date", "payment_value": "revenue_payments"})
        )
        daily = daily.merge(daily_revenue_payments, on="date", how="left")
        daily["revenue"] = daily["revenue_payments"].fillna(0)
    else:
        daily["revenue"] = daily["revenue_items"].fillna(0)

    daily["date"] = pd.to_datetime(daily["date"])
    full_range = pd.date_range(start=daily["date"].min(), end=daily["date"].max(), freq="D")
    daily = (
        daily.set_index("date")
        .reindex(full_range)
        .reset_index()
        .rename(columns={"index": "date"})
    )

    for column in [
        "orders_count",
        "revenue",
        "canceled_orders",
        "revenue_items",
        "revenue_payments",
    ]:
        if column in daily.columns:
            daily[column] = daily[column].fillna(0)

    daily["avg_order_value"] = daily.apply(
        lambda row: round(row["revenue"] / row["orders_count"], 2)
        if row["orders_count"] > 0
        else 0,
        axis=1,
    )
    daily["date"] = daily["date"].dt.strftime("%Y-%m-%d")

    columns = ["date", "orders_count", "revenue", "canceled_orders", "avg_order_value"]
    for audit_column in ["revenue_items", "revenue_payments"]:
        if audit_column in daily.columns:
            columns.append(audit_column)

    return daily[columns].sort_values("date").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    daily = build_daily_kpis(args.data_dir)

    daily.to_csv(args.output_csv, index=False)
    daily.to_excel(args.output_xlsx, index=False)

    print("Done")
    print(f"Rows: {len(daily)}")
    print(f"CSV: {args.output_csv}")
    print(f"XLSX: {args.output_xlsx}")


if __name__ == "__main__":
    main()
