"""Run Auto against an isolated, locally installed n8n instance."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from zoneinfo import ZoneInfo

import requests

from run_demo import ROOT, ZONE, generate_orders


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextmanager
def service(command, env, log_path):
    with log_path.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=log)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def wait_ready(client, url, process, log_path):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and process.poll() is None:
        try:
            if client.get(url, timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Service did not become ready. See {log_path}.")


def configure_n8n(binary, env, output, auto_url, credentials):
    log_path = output / "n8n.log"
    with tempfile.TemporaryDirectory(prefix="auto-n8n-import-") as staging:
        directory = Path(staging)
        exports = directory / "workflows"
        exports.mkdir()
        auto_paths = {"Run Auto monitor": "/run", "Read alert delivery receipt": "/reports"}
        for source in (ROOT / "n8n").glob("*.json"):
            workflow = json.loads(source.read_text(encoding="utf-8"))
            for node in workflow["nodes"]:
                if node["name"] in auto_paths:
                    node["parameters"]["url"] = auto_url + auto_paths[node["name"]]
            (exports / source.name).write_text(json.dumps(workflow), encoding="utf-8")
        credentials_file = directory / "credentials.json"
        credentials_file.write_text(json.dumps([
            {"id": key, "name": name, "type": "httpHeaderAuth", "data": {"name": header, "value": value}}
            for key, (name, header, value) in credentials.items()
        ]), encoding="utf-8")
        commands = [
            ["import:credentials", f"--input={credentials_file}"],
            ["import:workflow", "--separate", f"--input={exports}"],
            ["publish:workflow", "--id=AutoRegistryN8nDemo"],
            ["publish:workflow", "--id=AutoQueueN8nDemo"],
            ["publish:workflow", "--id=AutoAlertsN8nDemo"],
            ["publish:workflow", "--id=AutoIncidentsN8nDemo"],
            ["publish:workflow", "--id=AutoDailyN8nDemo"],
            ["publish:workflow", "--id=AutoHandoffN8nDemo"],
        ]
        with log_path.open("a", encoding="utf-8") as log:
            for command in commands:
                completed = subprocess.run([binary, *command], env=env, cwd=ROOT, stdout=log, stderr=log)
                if completed.returncode:
                    raise RuntimeError(f"n8n {command[0]} failed. See {log_path}.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n8n", default=shutil.which("n8n"), help="Path to a local n8n 2.x executable.")
    parser.add_argument("--keep-open", action="store_true", help="Leave the demo services running until Ctrl+C.")
    args = parser.parse_args()
    if not args.n8n:
        parser.error("Install n8n 2.x and put it on PATH, or pass --n8n.")
    runs = ROOT / "data" / "n8n-demo"
    runs.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="run-", dir=runs))
    print(f"n8n demo output: {output}", flush=True)
    incident = datetime.now(ZoneInfo(ZONE)).date() - timedelta(days=1)
    normal = incident - timedelta(days=1)
    raw, _ = generate_orders(output, incident - timedelta(days=41), normal, incident)
    metrics = output / "daily_ops_metrics.csv"
    subprocess.run([sys.executable, "build_daily_kpi.py", "--data-dir", str(raw),
                    "--output-csv", str(metrics), "--output-xlsx", str(output / "daily_ops_metrics.xlsx")],
                   cwd=ROOT, check=True)

    ports = set()
    while len(ports) < 3:
        ports.add(free_port())
    n8n_port, auto_port, broker_port = sorted(ports)
    n8n_url, auto_url = f"http://127.0.0.1:{n8n_port}", f"http://127.0.0.1:{auto_port}"
    service_token, alert_token, operator_token = (secrets.token_urlsafe(24) for _ in range(3))
    base_env = {key: value for key, value in os.environ.items()
                if not key.startswith(("OPS_", "N8N_", "DB_", "EXECUTIONS_", "QUEUE_"))
                and key not in {"WEBHOOK_URL", "GENERIC_TIMEZONE"}}
    base_env.update(NO_PROXY="127.0.0.1", no_proxy="127.0.0.1")
    n8n_env = {**base_env, "N8N_USER_FOLDER": str(output), "N8N_LISTEN_ADDRESS": "127.0.0.1",
               "N8N_HOST": "127.0.0.1", "N8N_PORT": str(n8n_port), "N8N_SECURE_COOKIE": "false",
               "N8N_DIAGNOSTICS_ENABLED": "false", "N8N_VERSION_NOTIFICATIONS_ENABLED": "false",
               "N8N_TEMPLATES_ENABLED": "false", "N8N_ENFORCE_SETTINGS_FILE_PERMISSIONS": "true",
               "N8N_PYTHON_ENABLED": "false", "N8N_RUNNERS_BROKER_PORT": str(broker_port),
               "GENERIC_TIMEZONE": ZONE, "WEBHOOK_URL": n8n_url + "/"}
    auto_env = {**base_env, "PYTHON_DOTENV_DISABLED": "1", "OPS_MODE": "live", "OPS_TIMEZONE": ZONE,
                "OPS_HOST": "127.0.0.1", "OPS_PORT": str(auto_port), "OPS_INPUT": str(metrics),
                "OPS_STATE_FILE": str(output / "run_state.json"), "OPS_AGENT_TOKEN": service_token,
                "OPS_WEBHOOK_TOKEN": alert_token, "OPS_WEBHOOK_URL": n8n_url + "/webhook/auto-monitor/alerts"}
    credentials = {
        "AutoServiceToken": ("Auto Service Token", "X-OPS-TOKEN", service_token),
        "AutoAlertBearer": ("Auto Alert Bearer", "Authorization", f"Bearer {alert_token}"),
        "AutoOperatorToken": ("Auto Operator Token", "Authorization", f"Bearer {operator_token}"),
    }
    configure_n8n(args.n8n, n8n_env, output, auto_url, credentials)
    with requests.Session() as client, service([args.n8n, "start"], n8n_env, output / "n8n.log") as n8n, service(
        [sys.executable, "run_service.py"], auto_env, output / "auto.log"
    ) as auto:
        client.trust_env = False
        wait_ready(client, n8n_url + "/healthz", n8n, output / "n8n.log")
        wait_ready(client, auto_url + "/health", auto, output / "auto.log")
        endpoint = n8n_url + "/webhook/auto-monitor/run"
        headers = {"Authorization": f"Bearer {operator_token}"}
        queue_url = n8n_url + "/webhook/auto-monitor/incidents"
        handoff_url = n8n_url + "/webhook/auto-monitor/handoff"
        for method, url in (("POST", endpoint), ("GET", queue_url), ("GET", handoff_url)):
            deadline = time.monotonic() + 60
            while True:
                unauthorized = client.request(method, url, timeout=5)
                if unauthorized.status_code != 404 or time.monotonic() >= deadline:
                    break
                time.sleep(0.25)
            if unauthorized.status_code not in {401, 403}:
                raise RuntimeError(f"n8n webhook did not require authentication (HTTP {unauthorized.status_code}).")

        def read_workflow(url):
            response = client.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            return response.json()

        empty_queue = read_workflow(queue_url)
        assert empty_queue == {"active_count": 0, "incidents": []}
        unavailable = client.post(endpoint, json={"date": str(incident - timedelta(days=41))},
                                  headers=headers, timeout=25)
        assert unavailable.status_code == 500, unavailable.text
        results = []
        for day in (normal, incident, incident):
            response = client.post(endpoint, json={"date": day.isoformat()}, headers=headers, timeout=40)
            if not response.ok:
                raise RuntimeError(f"n8n returned HTTP {response.status_code}. See n8n.log and auto.log.")
            result = response.json()
            results.append(result)
            print(f"{day}: {result['status']}", flush=True)
        assert [result["status"] for result in results] == ["normal", "anomaly_detected", "already_processed"]
        assert results[1]["alert_delivery"] == "delivered"
        history = client.get(auto_url + "/reports", headers={"X-OPS-TOKEN": service_token}, timeout=5)
        history.raise_for_status()
        assert [report["delivery"] for report in history.json()["reports"]] == ["delivered", "not_needed"]
        receipt = client.get(auto_url + "/reports", params={"date": str(incident)},
                             headers={"X-OPS-TOKEN": service_token}, timeout=5).json()["report"]
        assert len(receipt["attempts"]) == 1 and 200 <= receipt["attempts"][0]["http_status"] < 300
        action_url = n8n_url + "/webhook/auto-monitor/incidents/action"
        event_id = results[1]["event_id"]
        open_queue = read_workflow(queue_url)
        assert open_queue["active_count"] == 1
        assert open_queue["incidents"][0]["event_id"] == event_id
        assert open_queue["incidents"][0]["status"] == "open"
        handoff_open = read_workflow(handoff_url)
        assert handoff_open["active_count"] == handoff_open["open_count"] == handoff_open["high_priority_count"] == 1
        assert handoff_open["priority_queue"][0]["event_id"] == event_id
        assert handoff_open["handoff_status"] == "escalate" and handoff_open["escalation_event_ids"] == [event_id]
        command = {"event_id": event_id, "operator": "Demo Operator"}
        resolve = {**command, "action": "resolve",
                   "note": "Synthetic example. Restored checkout and verified a test order."}
        invalid = client.post(action_url, json={**resolve, "note": ""}, headers=headers, timeout=15)
        assert invalid.status_code == 400, invalid.text
        early = client.post(action_url, json=resolve, headers=headers, timeout=15)
        assert early.status_code == 409, early.text
        acknowledged = client.post(action_url, json={**command, "action": "acknowledge"},
                                   headers=headers, timeout=15)
        acknowledged.raise_for_status()
        ack = acknowledged.json()["incident"]
        assert ack["status"] == "acknowledged" and ack["acknowledged_by"] == "Demo Operator"
        handoff_acknowledged = read_workflow(handoff_url)
        assert handoff_acknowledged["open_count"] == 0 and handoff_acknowledged["acknowledged_count"] == 1
        assert handoff_acknowledged["priority_queue"][0]["acknowledged_by"] == "Demo Operator"
        assert handoff_acknowledged["handoff_status"] == "follow_up" and handoff_acknowledged["escalation_event_ids"] == []
        resolved = client.post(action_url, json={**resolve, "operator": "Demo Lead"}, headers=headers, timeout=15)
        resolved.raise_for_status()
        closed = resolved.json()["incident"]
        assert closed["id"] == ack["id"] and closed["status"] == "resolved"
        assert closed["resolution_note"] == resolve["note"] and closed["resolved_by"] == "Demo Lead"
        replay = client.post(n8n_url + "/webhook/auto-monitor/alerts", json=results[1],
                             headers={"Authorization": f"Bearer {alert_token}", "Idempotency-Key": event_id},
                             timeout=15)
        replay.raise_for_status()
        replay_receipt = replay.json()
        assert replay_receipt["incident_id"] == closed["id"] and replay_receipt["incident_status"] == "resolved"
        repeated_action = client.post(action_url, json=resolve, headers=headers, timeout=15)
        assert repeated_action.status_code == 409, repeated_action.text
        closed_queue = read_workflow(queue_url)
        handoff_closed = read_workflow(handoff_url)
        assert closed_queue == empty_queue and handoff_closed["active_count"] == 0
        assert handoff_closed["handoff_status"] == "clear" and handoff_closed["escalation_event_ids"] == []
        (output / "n8n-results.json").write_text(json.dumps(
            {"reports": results, "history": history.json()["reports"], "alert_receipt": receipt,
             "incident": closed, "replay_receipt": replay_receipt,
             "queues": {"before_alert": empty_queue, "open": open_queue, "after_closure": closed_queue},
             "handoffs": {"open": handoff_open, "acknowledged": handoff_acknowledged, "closed": handoff_closed},
             "unavailable_input_status": unavailable.status_code,
             "invalid_action_status": invalid.status_code, "out_of_order_status": early.status_code,
             "repeated_action_status": repeated_action.status_code}, indent=2
        ) + "\n", encoding="utf-8")
        print(f"Incident {event_id}: opened → acknowledged → resolved.", flush=True)
        print("Replayed alert kept the same resolved incident. Invalid and out-of-order actions were rejected.", flush=True)
        print("Shift handoff tracked the incident from open to acknowledged, then omitted it after closure.", flush=True)
        print(f"Workflow UI: {n8n_url}", flush=True)
        if args.keep_open:
            print("Keep this terminal open while viewing n8n. Press Ctrl+C to stop both services.", flush=True)
            try:
                while n8n.poll() is None and auto.poll() is None:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
