"""Detect daily e-commerce operations anomalies and optionally notify n8n."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_THRESHOLDS = {
    "revenue_drop_medium": 0.15,
    "revenue_drop_high": 0.25,
    "orders_drop_medium": 0.12,
    "orders_drop_high": 0.20,
    "aov_drop_medium": 0.15,
    "aov_drop_high": 0.25,
    "cancel_spike_medium": 0.60,
    "cancel_spike_high": 1.20,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=BASE_DIR / "daily_ops_metrics.csv")
    parser.add_argument("--state-file", type=Path, default=BASE_DIR / "run_state.json")
    parser.add_argument("--webhook-url", default=os.getenv("OPS_WEBHOOK_URL"))
    parser.add_argument("--sim-start-date", default=os.getenv("OPS_SIM_START_DATE", "2017-01-12"))
    parser.add_argument("--rolling-days", type=int, default=int(os.getenv("OPS_ROLLING_DAYS", "7")))
    parser.add_argument("--dry-run", action="store_true", help="Print the payload without posting.")
    return parser.parse_args()


def pct_change(today: float, baseline: float) -> float:
    if baseline == 0 or pd.isna(baseline):
        return 0.0
    return (today - baseline) / baseline


def load_state(path: Path) -> dict[str, int]:
    if not path.exists():
        return {"cursor": 0}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"cursor": 0}


def save_state(path: Path, state: dict[str, int]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def severity(change: float, high_threshold: float) -> str:
    return "high" if abs(change) >= high_threshold else "medium"


def build_payload(
    data_file: Path,
    state_file: Path,
    sim_start_date: str,
    rolling_days: int,
    thresholds: dict[str, float],
) -> dict[str, Any]:
    if not data_file.exists():
        raise FileNotFoundError(f"Missing input file: {data_file}")

    df = pd.read_csv(data_file)
    required = {"date", "orders_count", "revenue", "canceled_orders", "avg_order_value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{data_file.name} missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    for column in ["orders_count", "revenue", "canceled_orders", "avg_order_value"]:
        df[f"base_{column}"] = df[column].rolling(rolling_days).mean().shift(1)

    baseline_columns = [
        f"base_{column}"
        for column in ["orders_count", "revenue", "canceled_orders", "avg_order_value"]
    ]
    candidates = df.dropna(subset=baseline_columns).copy()
    candidates = candidates[candidates["date"] >= pd.to_datetime(sim_start_date)].reset_index(drop=True)
    if candidates.empty:
        raise ValueError("No candidate rows after sim_start_date. Check the input data range.")

    state = load_state(state_file)
    cursor = int(state.get("cursor", 0))
    if cursor >= len(candidates):
        cursor = 0

    row = candidates.iloc[cursor]
    signals: list[dict[str, str]] = []

    metric_specs = [
        ("revenue", "down", "revenue_drop_medium", "revenue_drop_high", "Revenue"),
        ("orders_count", "down", "orders_drop_medium", "orders_drop_high", "Orders"),
        ("avg_order_value", "down", "aov_drop_medium", "aov_drop_high", "AOV"),
        ("canceled_orders", "up", "cancel_spike_medium", "cancel_spike_high", "Cancellations"),
    ]

    for metric, direction, medium_key, high_key, label in metric_specs:
        today = float(row[metric])
        baseline = float(row[f"base_{metric}"])
        change = pct_change(today, baseline)
        breached = change <= -thresholds[medium_key] if direction == "down" else change >= thresholds[medium_key]
        if breached:
            sign = "+" if change > 0 else ""
            signals.append(
                {
                    "metric": metric,
                    "direction": direction,
                    "severity": severity(change, thresholds[high_key]),
                    "details": f"{label} {sign}{round(change * 100)}% vs {rolling_days}-day avg",
                }
            )

    status = "anomaly_detected" if signals else "normal"
    date_str = row["date"].strftime("%Y-%m-%d")
    summary_lines = [
        f"Date: {date_str} | Status: {status}",
        (
            f"Orders: {int(row['orders_count'])} (avg {row['base_orders_count']:.1f}) | "
            f"Revenue: {float(row['revenue']):.2f} (avg {row['base_revenue']:.2f})"
        ),
        (
            f"Canceled: {int(row['canceled_orders'])} (avg {row['base_canceled_orders']:.1f}) | "
            f"AOV: {float(row['avg_order_value']):.2f} (avg {row['base_avg_order_value']:.2f})"
        ),
        "Signals:",
    ]
    summary_lines += [
        f"- {signal['metric']} ({signal['direction']}, {signal['severity']}): {signal['details']}"
        for signal in signals
    ] or ["- none"]

    save_state(state_file, {"cursor": cursor + 1})

    return {
        "run_time": datetime.now().isoformat(timespec="seconds"),
        "sim_cursor": cursor,
        "date": date_str,
        "status": status,
        "signals_count": len(signals),
        "signals": signals,
        "summary": "\n".join(summary_lines),
    }


def main() -> None:
    args = parse_args()
    payload = build_payload(
        data_file=args.input,
        state_file=args.state_file,
        sim_start_date=args.sim_start_date,
        rolling_days=args.rolling_days,
        thresholds=DEFAULT_THRESHOLDS,
    )

    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if args.dry_run or not args.webhook_url:
        return

    import requests

    response = requests.post(args.webhook_url, json=payload, timeout=8)
    print("Webhook status:", response.status_code)


if __name__ == "__main__":
    main()
