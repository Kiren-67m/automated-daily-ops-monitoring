import json
import os
from datetime import datetime

import pandas as pd
import requests

# =========================
# Config
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "daily_ops_metrics.csv")
STATE_FILE = os.path.join(BASE_DIR, "run_state.json")

ROLLING_DAYS = 7

# 你的 n8n production webhook
WEBHOOK_URL = "http://127.0.0.1:5678/webhook/ops-insight"

# ✅ 从这里开始模拟：让 2017-01-05~2017-01-11 做 baseline
SIM_START_DATE = "2017-01-12"

TH = {
    "revenue_drop_medium": 0.15,
    "revenue_drop_high": 0.25,
    "orders_drop_medium": 0.12,
    "orders_drop_high": 0.20,
    "aov_drop_medium": 0.15,
    "aov_drop_high": 0.25,
    "cancel_spike_medium": 0.60,
    "cancel_spike_high": 1.20,
}

# =========================
# Helpers
# =========================
def pct_change(today, baseline):
    if baseline == 0 or pd.isna(baseline):
        return 0.0
    return (today - baseline) / baseline

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"cursor": 0}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"cursor": 0}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# =========================
# Main
# =========================
def main():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Missing input file: {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)

    required = {"date", "orders_count", "revenue", "canceled_orders", "avg_order_value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"daily_ops_metrics.csv missing columns: {missing}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    # rolling baseline from previous 7 calendar days (shift avoids leakage)
    for col in ["orders_count", "revenue", "canceled_orders", "avg_order_value"]:
        df[f"base_{col}"] = df[col].rolling(ROLLING_DAYS).mean().shift(1)

    candidates = df.dropna(
        subset=[f"base_{c}" for c in ["orders_count", "revenue", "canceled_orders", "avg_order_value"]]
    ).copy()

    # ✅ 只从 2017-01-12 之后开始模拟（保证 01/05~01/11 是 baseline）
    candidates = candidates[candidates["date"] >= pd.to_datetime(SIM_START_DATE)].reset_index(drop=True)

    if candidates.empty:
        raise ValueError("No candidate rows after SIM_START_DATE. Check SIM_START_DATE or data coverage.")

    state = load_state()
    cursor = int(state.get("cursor", 0))

    # 防止越界：跑到最后就回到 0（你也可以改成 stop）
    if cursor >= len(candidates):
        cursor = 0

    row = candidates.iloc[cursor]

    date_str = row["date"].strftime("%Y-%m-%d")
    signals = []

    # Revenue drop
    rev_today = float(row["revenue"])
    rev_base = float(row["base_revenue"])
    rev_change = pct_change(rev_today, rev_base)
    if rev_change <= -TH["revenue_drop_medium"]:
        sev = "high" if abs(rev_change) >= TH["revenue_drop_high"] else "medium"
        signals.append({
            "metric": "revenue",
            "direction": "down",
            "severity": sev,
            "details": f"Revenue {round(rev_change*100)}% vs {ROLLING_DAYS}-day avg"
        })

    # Orders drop
    ord_today = float(row["orders_count"])
    ord_base = float(row["base_orders_count"])
    ord_change = pct_change(ord_today, ord_base)
    if ord_change <= -TH["orders_drop_medium"]:
        sev = "high" if abs(ord_change) >= TH["orders_drop_high"] else "medium"
        signals.append({
            "metric": "orders",
            "direction": "down",
            "severity": sev,
            "details": f"Orders {round(ord_change*100)}% vs {ROLLING_DAYS}-day avg"
        })

    # AOV drop
    aov_today = float(row["avg_order_value"])
    aov_base = float(row["base_avg_order_value"])
    aov_change = pct_change(aov_today, aov_base)
    if aov_change <= -TH["aov_drop_medium"]:
        sev = "high" if abs(aov_change) >= TH["aov_drop_high"] else "medium"
        signals.append({
            "metric": "avg_order_value",
            "direction": "down",
            "severity": sev,
            "details": f"AOV {round(aov_change*100)}% vs {ROLLING_DAYS}-day avg"
        })

    # Cancellation spike
    can_today = float(row["canceled_orders"])
    can_base = float(row["base_canceled_orders"])
    can_change = pct_change(can_today, can_base)
    if can_change >= TH["cancel_spike_medium"]:
        sev = "high" if can_change >= TH["cancel_spike_high"] else "medium"
        signals.append({
            "metric": "canceled_orders",
            "direction": "up",
            "severity": sev,
            "details": f"Cancellations +{round(can_change*100)}% vs {ROLLING_DAYS}-day avg"
        })

    status = "anomaly_detected" if signals else "normal"

    summary_lines = [
        f"Date: {date_str} | Status: {status}",
        f"Orders: {int(ord_today)} (avg {ord_base:.1f}) | Revenue: {rev_today:.2f} (avg {rev_base:.2f})",
        f"Canceled: {int(can_today)} (avg {can_base:.1f}) | AOV: {aov_today:.2f} (avg {aov_base:.2f})",
    ]
    if signals:
        summary_lines.append("Signals:")
        summary_lines += [f"- {s['metric']} ({s['direction']}, {s['severity']}): {s['details']}" for s in signals]
    else:
        summary_lines.append("Signals: none")

    payload = {
        "run_time": datetime.now().isoformat(timespec="seconds"),
        "sim_cursor": cursor,
        "date": date_str,
        "status": status,
        "signals_count": len(signals),
        "signals": signals,
        "summary": "\n".join(summary_lines),
    }

    # stdout（你终端能看到）
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    # push to n8n
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        print("Webhook status:", r.status_code)
    except Exception as e:
        print(f"Webhook POST failed: {e}")

    # cursor + 1
    state["cursor"] = cursor + 1
    save_state(state)

if __name__ == "__main__":
    main()
