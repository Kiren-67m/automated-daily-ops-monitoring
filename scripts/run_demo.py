"""Generate synthetic orders and exercise the real Auto monitor end to end."""
from __future__ import annotations

import csv
import json
import os
import random
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from zoneinfo import ZoneInfo

import requests

from render_demo import render_preview

ROOT = Path(__file__).resolve().parents[1]
ZONE = "America/Chicago"


def write_csv(path, header, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)


def generate_orders(output, start, normal_day, incident_day):
    rng = random.Random(67912)
    orders, items, payments = [], [], []
    prices = (790, 1290, 1690, 1990, 2490, 2890, 3490, 3990, 5490, 6890)
    for offset in range(42):
        day = start + timedelta(days=offset)
        weekend = day.weekday() >= 5
        count = round(rng.gauss(52 if weekend else 39, 5) + offset * 0.12)
        if day == normal_day:
            count = 44
        if day == incident_day:
            count = 22
        if day == incident_day:
            cancel_count = 8
        elif day == normal_day:
            cancel_count = 1
        else:
            cancel_count = sum(rng.random() < 0.036 for _ in range(count))
        canceled_indices = set(rng.sample(range(count), cancel_count))
        for index in range(count):
            key = f"SYN-{day:%Y%m%d}-{index + 1:03d}"
            hour = rng.choices((7, 9, 11, 13, 15, 17, 19, 21, 23), (2, 6, 10, 12, 13, 16, 20, 17, 4))[0]
            timestamp = f"{day} {hour:02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}"
            canceled = index in canceled_indices
            status = ("unavailable" if rng.random() < 0.12 else "canceled") if canceled else "delivered"
            delivery_day = None if canceled else day + timedelta(days=rng.randint(2, 6))
            if delivery_day and delivery_day > incident_day:
                status = "shipped"
                delivery_day = None
            delivered = f"{delivery_day} 14:00:00" if delivery_day else ""
            orders.append((key, status, timestamp, delivered))
            count_items = rng.choices((1, 2, 3, 4), (72, 20, 7, 1))[0]
            total_cents = 0
            for item in range(1, count_items + 1):
                price = rng.choice(prices[:6] if day == incident_day else prices)
                shipping = rng.choice((399, 499, 599, 699)) if item == 1 else 0
                total_cents += price + shipping
                items.append((key, item, f"{price / 100:.2f}", f"{shipping / 100:.2f}"))
            if not canceled:
                split = rng.random() < 0.07
                first = total_cents // 3 if split else total_cents
                payments.append((key, 1, "credit_card", f"{first / 100:.2f}"))
                if split:
                    payments.append((key, 2, "voucher", f"{(total_cents - first) / 100:.2f}"))

    raw = output / "synthetic-orders"
    raw.mkdir(exist_ok=True)
    write_csv(raw / "olist_orders_dataset.csv",
              ("order_id", "order_status", "order_purchase_timestamp", "order_delivered_customer_date"), orders)
    write_csv(raw / "olist_order_items_dataset.csv",
              ("order_id", "order_item_id", "price", "freight_value"), items)
    write_csv(raw / "olist_order_payments_dataset.csv",
              ("order_id", "payment_sequential", "payment_type", "payment_value"), payments)
    return raw, {"orders": len(orders), "items": len(items), "payment_records": len(payments),
                 "cancellations": sum(row[1] in {"canceled", "unavailable"} for row in orders)}


def main():
    runs = ROOT / "data" / "demo"
    runs.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="run-", dir=runs))
    print(f"Demo output: {output}", flush=True)

    incident_day = datetime.now(ZoneInfo(ZONE)).date() - timedelta(days=1)
    normal_day = incident_day - timedelta(days=1)
    start = incident_day - timedelta(days=41)
    raw, counts = generate_orders(output, start, normal_day, incident_day)
    metrics = output / "daily_ops_metrics.csv"
    subprocess.run([sys.executable, "build_daily_kpi.py", "--data-dir", str(raw),
                    "--output-csv", str(metrics), "--output-xlsx", str(output / "daily_ops_metrics.xlsx")],
                   cwd=ROOT, check=True)

    alerts = []
    received = Event()
    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            alerts.append((message, self.headers.get("Idempotency-Key")))
            self.send_response(204)
            self.end_headers()
            received.set()

        def log_message(self, *_):
            pass

    state = output / "run_state.json"
    with ThreadingHTTPServer(("127.0.0.1", 0), Receiver) as receiver:
        Thread(target=receiver.serve_forever, daemon=True).start()
        token = secrets.token_urlsafe(24)
        env = {key: value for key, value in os.environ.items() if not key.startswith("OPS_")}
        env.update(PYTHON_DOTENV_DISABLED="1", NO_PROXY="127.0.0.1", no_proxy="127.0.0.1",
                   OPS_MODE="live", OPS_TIMEZONE=ZONE, OPS_INPUT=str(metrics), OPS_STATE_FILE=str(state),
                   OPS_AGENT_TOKEN=token, OPS_WEBHOOK_TOKEN="", OPS_HOST="127.0.0.1",
                   OPS_ROLLING_DAYS="7", OPS_SIM_START_DATE="2017-01-12",
                   OPS_THRESHOLDS="{}", OPS_CANCEL_ZERO_BASELINE_MIN="3",
                   OPS_WEBHOOK_URL=f"http://127.0.0.1:{receiver.server_port}/alerts")
        report = json.loads(subprocess.check_output([sys.executable, "detect_anomalies.py",
                             "--date", str(normal_day)], cwd=ROOT, env=env, text=True))
        assert report["status"] == "normal", report["summary"]
        assert not alerts
        with socket.socket() as port:
            port.bind(("127.0.0.1", 0))
            service_port = port.getsockname()[1]
        env.update(OPS_PORT=str(service_port), OPS_RUN_AT="00:00")
        with (output / "service.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen([sys.executable, "run_service.py"], cwd=ROOT, env=env, stdout=log, stderr=log)
            try:
                assert received.wait(30), "The scheduled run did not send an alert. Inspect service.log."
                assert len(alerts) == 1
                alert, key = alerts[0]
                assert alert["date"] == incident_day.isoformat() and alert["status"] == "anomaly_detected"
                assert key == alert["event_id"]
                base_url = f"http://127.0.0.1:{service_port}"
                with requests.Session() as client:
                    client.trust_env = False
                    for _ in range(100):
                        try:
                            healthy = client.get(base_url + "/health", timeout=1).json() == {"ok": True, "mode": "live"}
                        except requests.ConnectionError:
                            healthy = False
                        if healthy and state.exists():
                            if str(incident_day) in state.read_text(encoding="utf-8"):
                                duplicate = client.post(base_url + "/run", headers={"X-OPS-TOKEN": token}, timeout=5)
                                if duplicate.status_code != 409:
                                    break
                        time.sleep(0.05)
                    else:
                        raise RuntimeError("The service did not complete the scheduled run. Inspect service.log.")
                assert duplicate.status_code == 200 and duplicate.json()["result"]["status"] == "already_processed"
                assert len(alerts) == 1
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        receiver.shutdown()

    results = {
        "dataset": {"synthetic": True, "days": 42, "first_date": str(start), "last_date": str(incident_day),
                    "timezone": ZONE, "currency": "USD", **counts},
        "normal_report": report,
        "received_webhook": {"http_status": 204, "idempotency_key": key, "payload": alert},
        "webhook_calls_for_normal_day": 0,
        "total_webhook_calls": len(alerts),
        "repeat_run": duplicate.json()["result"],
    }
    for name, value in [("run-results.json", results), ("normal-report.json", report), ("webhook-alert.json", alert)]:
        (output / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    (output / "normal-report.txt").write_text(report["summary"] + "\n", encoding="utf-8")
    render_preview(output, results)
    print(json.dumps(results["dataset"], indent=2))
    print("\nNormal daily report\n" + report["summary"])
    print("\nReceived alert\n" + alert["summary"])
    print(f"\nWebhook deliveries: {len(alerts)}. Repeat result: {results['repeat_run']['status']}.")
    print(f"Open {output / 'demo-preview.html'} to compare both outputs.")


if __name__ == "__main__":
    main()
