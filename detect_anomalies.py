"""Replay historical operations or monitor a completed business day."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import requests
from dotenv import load_dotenv
from filelock import FileLock, Timeout

BASE_DIR = Path(__file__).resolve().parent
METRICS = ("orders_count", "revenue", "canceled_orders", "avg_order_value")
RULES = (
    ("revenue", "down", "revenue_drop", "Revenue"),
    ("orders_count", "down", "orders_drop", "Orders"),
    ("avg_order_value", "down", "aov_drop", "AOV"),
    ("canceled_orders", "up", "cancel_spike", "Cancellations"),
)
DEFAULT_THRESHOLDS = {
    "revenue_drop_medium": 0.15, "revenue_drop_high": 0.25,
    "orders_drop_medium": 0.12, "orders_drop_high": 0.20,
    "aov_drop_medium": 0.15, "aov_drop_high": 0.25,
    "cancel_spike_medium": 0.60, "cancel_spike_high": 1.20,
}


class DataNotReady(ValueError):
    """The requested date or its required history is missing."""


class DeliveryError(RuntimeError):
    """The configured receiver did not acknowledge delivery."""


def parse_args(argv=None):
    load_dotenv(BASE_DIR / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("demo", "live"), default=os.getenv("OPS_MODE", "demo"))
    parser.add_argument("--input", type=Path, default=os.getenv("OPS_INPUT") or None)
    parser.add_argument("--state-file", type=Path, default=os.getenv("OPS_STATE_FILE") or None)
    parser.add_argument("--webhook-url", default=os.getenv("OPS_WEBHOOK_URL"))
    parser.add_argument("--sim-start-date", type=date.fromisoformat,
                        default=os.getenv("OPS_SIM_START_DATE", "2017-01-12"))
    parser.add_argument("--rolling-days", type=int, default=os.getenv("OPS_ROLLING_DAYS", "7"))
    parser.add_argument("--date", type=date.fromisoformat, help="Completed date to check in live mode.")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate without delivering or changing state.")
    args = parser.parse_args(argv)
    if args.mode not in {"demo", "live"}:
        parser.error("OPS_MODE must be demo or live.")
    if args.date and args.mode != "live":
        parser.error("--date requires live mode.")
    if args.rolling_days < 1:
        parser.error("Rolling days must be at least 1.")
    if not args.input:
        args.input = BASE_DIR / ("daily_ops_metrics.csv" if args.mode == "demo" else "data/daily_ops_metrics.csv")
    if not args.state_file:
        args.state_file = BASE_DIR / ("run_state.json" if args.mode == "demo" else "var/live_state.json")
    try:
        args.business_timezone = ZoneInfo(os.getenv("OPS_TIMEZONE", "UTC"))
        args.cancel_min = int(os.getenv("OPS_CANCEL_ZERO_BASELINE_MIN", "3"))
        if args.cancel_min < 1:
            raise ValueError("OPS_CANCEL_ZERO_BASELINE_MIN must be at least 1.")
        overrides = json.loads(os.getenv("OPS_THRESHOLDS") or "{}")
        if not isinstance(overrides, dict) or overrides.keys() - DEFAULT_THRESHOLDS.keys():
            raise ValueError("OPS_THRESHOLDS must be a JSON object using documented threshold names.")
        args.thresholds = DEFAULT_THRESHOLDS | overrides
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in args.thresholds.values()):
            raise ValueError("Thresholds must be positive finite numbers.")
        for _, _, rule, _ in RULES:
            if args.thresholds[f"{rule}_high"] < args.thresholds[f"{rule}_medium"]:
                raise ValueError(f"{rule}: high threshold must be at least the medium threshold.")
    except ZoneInfoNotFoundError:
        parser.error("OPS_TIMEZONE must be an IANA time zone, such as America/Chicago.")
    except ValueError as exc:
        parser.error(str(exc))
    args.webhook_token = os.getenv("OPS_WEBHOOK_TOKEN")
    return args


def read_metrics(path):
    frame = pd.read_csv(path)
    missing = {"date", *METRICS} - set(frame.columns)
    if missing:
        raise ValueError(f"Missing KPI CSV columns: {', '.join(sorted(missing))}.")
    if frame.empty:
        raise DataNotReady("The KPI CSV has no rows.")
    dates = frame["date"].astype("string")
    if not dates.str.fullmatch(r"\d{4}-\d{2}-\d{2}", na=False).all():
        raise ValueError("Every KPI date must use YYYY-MM-DD.")
    frame["date"] = pd.to_datetime(dates, format="%Y-%m-%d")
    if frame["date"].duplicated().any():
        raise ValueError("The KPI CSV must have exactly one row per date.")
    for metric in METRICS:
        values = pd.to_numeric(frame[metric], errors="coerce")
        if values.isna().any() or not values.map(math.isfinite).all():
            raise ValueError(f"{metric} must contain finite numeric values.")
        if metric in {"orders_count", "canceled_orders"} and ((values < 0) | (values % 1 != 0)).any():
            raise ValueError(f"{metric} must contain nonnegative whole counts.")
        frame[metric] = values
    if (frame["canceled_orders"] > frame["orders_count"]).any():
        raise ValueError("Canceled orders cannot exceed total orders for a date.")
    return frame.set_index("date")[list(METRICS)].sort_index()


def build_payload(row, baseline, day, args, cursor=None):
    signals = []
    for metric, direction, rule, label in RULES:
        value, average = float(row[metric]), float(baseline[metric])
        if average <= 0:
            if (metric == "canceled_orders" and average == 0 and args.mode == "live"
                    and value >= args.cancel_min):
                signals.append({"metric": metric, "direction": "up", "severity": "medium",
                                "details": f"{int(value)} cancellations vs 0 in the baseline"})
            continue
        change = (value - average) / average
        magnitude = -change if direction == "down" else change
        if magnitude >= args.thresholds[f"{rule}_medium"]:
            severity = "high" if magnitude >= args.thresholds[f"{rule}_high"] else "medium"
            signals.append({"metric": metric, "direction": direction, "severity": severity,
                            "details": f"{label} {change:+.0%} vs {args.rolling_days}-day avg"})
    status = "anomaly_detected" if signals else "normal"
    summary = [
        f"Date: {day} | Status: {status}",
        f"Orders: {int(row['orders_count'])} (avg {baseline['orders_count']:.1f}) | "
        f"Revenue: {row['revenue']:.2f} (avg {baseline['revenue']:.2f})",
        f"Canceled: {int(row['canceled_orders'])} (avg {baseline['canceled_orders']:.1f}) | "
        f"AOV: {row['avg_order_value']:.2f} (avg {baseline['avg_order_value']:.2f})",
        "Signals:",
    ]
    summary += [f"- {signal['metric']} ({signal['direction']}, {signal['severity']}): {signal['details']}"
                for signal in signals] or ["- none"]
    payload = {
        "run_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": args.mode,
        "date": day,
        "status": status,
        "signals_count": len(signals),
        "signals": signals,
        "summary": "\n".join(summary),
    }
    if cursor is not None:
        payload["sim_cursor"] = cursor
    else:
        payload["event_id"] = f"daily-ops:{day}"
        payload["timezone"] = args.business_timezone.key
    return payload


def load_state(path, default):
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("Run state is unreadable. Restore it before retrying.") from exc
    key, expected = next(iter(default.items()))
    if not isinstance(state, dict) or type(state.get(key)) is not type(expected):
        raise ValueError(f"Run state has invalid {key} data. Check that the mode uses its own state file.")
    if key == "cursor" and state[key] < 0:
        raise ValueError("The demo cursor cannot be negative.")
    if key == "completed" and not all(isinstance(day, str) for day in state[key]):
        raise ValueError("Live state must contain completed dates.")
    return state


def save_state(path, state):
    f = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False)
    temporary = Path(f.name)
    try:
        with f:
            json.dump(state, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def deliver(payload, args):
    if args.dry_run or not args.webhook_url or (args.mode == "live" and not payload["signals"]):
        return
    headers = {}
    if "event_id" in payload:
        headers["Idempotency-Key"] = payload["event_id"]
    if args.webhook_token:
        headers["Authorization"] = f"Bearer {args.webhook_token}"
    try:
        response = requests.post(args.webhook_url, json=payload, headers=headers, timeout=8,
                                 allow_redirects=False)
    except requests.RequestException as exc:
        raise DeliveryError(f"Webhook request failed ({type(exc).__name__}).") from None
    if not 200 <= response.status_code < 300:
        raise DeliveryError(f"Webhook returned HTTP {response.status_code}.")


def run_with_state(args, target_date):
    if args.mode == "demo":
        state = load_state(args.state_file, {"cursor": 0})
        frame = read_metrics(args.input)
        baseline = frame.rolling(args.rolling_days).mean().shift(1)
        candidates = baseline.dropna().loc[str(args.sim_start_date):]
        if candidates.empty:
            raise DataNotReady("The demo has no rows with a complete baseline after the start date.")
        cursor = state["cursor"] % len(candidates)
        day = candidates.index[cursor]
        payload = build_payload(frame.loc[day], candidates.loc[day], day.date().isoformat(), args, cursor)
        next_state = {"cursor": cursor + 1}
    else:
        today = datetime.now(args.business_timezone).date()
        day = target_date or args.date or (today - timedelta(days=1))
        if day >= today:
            raise ValueError("Live mode only checks dates completed in the configured business time zone.")
        date_text = day.isoformat()
        state = {"completed": []} if args.dry_run else load_state(args.state_file, {"completed": []})
        if date_text in state["completed"]:
            return {"mode": "live", "date": date_text, "status": "already_processed",
                    "event_id": f"daily-ops:{date_text}", "signals_count": 0, "signals": []}
        frame = read_metrics(args.input)
        window = pd.date_range(end=day, periods=args.rolling_days + 1, freq="D")
        missing = window.difference(frame.index)
        if len(missing):
            dates = ", ".join(missing.strftime("%Y-%m-%d"))
            raise DataNotReady(f"KPI CSV is missing required dates: {dates}.")
        rows = frame.loc[window]
        payload = build_payload(rows.iloc[-1], rows.iloc[:-1].mean(), date_text, args)
        next_state = {"completed": sorted([*state["completed"], date_text])}

    deliver(payload, args)
    if not args.dry_run:
        save_state(args.state_file, next_state)
    return payload


def run_once(args, target_date=None):
    if args.dry_run:
        return run_with_state(args, target_date)
    args.state_file.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(f"{args.state_file}.lock", timeout=0):
        return run_with_state(args, target_date)


def main():
    try:
        print(json.dumps(run_once(parse_args()), ensure_ascii=False, indent=2))
    except (OSError, ValueError, DeliveryError, Timeout) as exc:
        print(f"Monitoring failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
