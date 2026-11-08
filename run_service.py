"""Run the daily monitor as a local HTTP service with an optional schedule."""

from __future__ import annotations

import hmac
import json
import logging
import os
from datetime import date, datetime, timedelta
from threading import Event, Thread
from time import monotonic

from filelock import Timeout
from flask import Flask, jsonify, request

from detect_anomalies import DataNotReady, DeliveryError, load_state, parse_args, run_once

LOG = logging.getLogger("daily_ops")


def create_app(args=None, token=None):
    app = Flask(__name__)
    args = args if args is not None else parse_args([])
    token = token if token is not None else os.getenv("OPS_AGENT_TOKEN", "")

    @app.get("/health")
    def health():
        return jsonify(ok=True, mode=args.mode)

    def authorization_error():
        if not token or token == "change-me":
            return jsonify(error="OPS_AGENT_TOKEN is not configured"), 503
        if not hmac.compare_digest(request.headers.get("X-OPS-TOKEN", "").encode(), token.encode()):
            return jsonify(error="unauthorized"), 401
        return None

    @app.get("/reports")
    def reports():
        error = authorization_error()
        if error is not None:
            return error
        if args.mode != "live":
            return jsonify(reports=[])
        try:
            history = load_state(args.state_file, {"completed": []}).get("reports", {})
        except (OSError, ValueError) as exc:
            return jsonify(error=str(exc)), 422
        day = request.args.get("date")
        if day is not None:
            report = history.get(day)
            if report is None:
                return jsonify(error="report not found"), 404
            return jsonify(report=report)
        return jsonify(reports=[
            {"date": day, "status": item["payload"]["status"], "delivery": item["delivery"],
             "attempts": len(item["attempts"])}
            for day, item in sorted(history.items(), reverse=True)
        ])

    @app.post("/run")
    def run():
        error = authorization_error()
        if error is not None:
            return error
        target = None
        if request.is_json:
            body = request.get_json(silent=True)
            if not isinstance(body, dict):
                return jsonify(error="expected a JSON object"), 400
            if body.get("date") is not None:
                value = body["date"]
                try:
                    target = date.fromisoformat(value)
                    if args.mode != "live" or target.isoformat() != value:
                        raise ValueError
                except (TypeError, ValueError):
                    return jsonify(error="date must be YYYY-MM-DD in live mode"), 400
        try:
            payload = run_once(args, target_date=target)
        except (DataNotReady, FileNotFoundError, Timeout) as exc:
            return jsonify(error=str(exc)), 409
        except DeliveryError as exc:
            return jsonify(error=str(exc)), 502
        except (OSError, ValueError) as exc:
            return jsonify(error=str(exc)), 422
        return jsonify(exit_code=0, stdout=json.dumps(payload, ensure_ascii=False), stderr="", result=payload)

    return app


def run_schedule(args, start_time, stop):
    next_attempt = 0.0
    completed_date = None
    while not stop.is_set():
        now = datetime.now(args.business_timezone)
        target = now.date() - timedelta(days=1)
        if (now.time() >= start_time and target != completed_date
                and monotonic() >= next_attempt):
            try:
                payload = run_once(args, target_date=target)
            except (OSError, ValueError, DeliveryError, Timeout) as exc:
                LOG.warning("Scheduled check failed for %s: %s. Retrying in 5 minutes.", target, exc)
                next_attempt = monotonic() + 300
            else:
                if "summary" in payload:
                    LOG.info("Scheduled check\n%s", payload["summary"])
                completed_date = target
        stop.wait(30)


def main():
    from waitress import serve

    args = parse_args([])
    token = os.getenv("OPS_AGENT_TOKEN", "")
    if not token or token == "change-me":
        raise SystemExit("Set OPS_AGENT_TOKEN in .env before starting the service.")
    schedule = os.getenv("OPS_RUN_AT")
    stop = Event()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if schedule:
        if args.mode != "live":
            raise SystemExit("OPS_RUN_AT requires OPS_MODE=live.")
        try:
            start_time = datetime.strptime(schedule, "%H:%M").time()
        except ValueError:
            raise SystemExit("OPS_RUN_AT must use 24-hour HH:MM format.") from None
        if start_time.strftime("%H:%M") != schedule:
            raise SystemExit("OPS_RUN_AT must use 24-hour HH:MM format.")
        Thread(target=run_schedule, args=(args, start_time, stop), daemon=True).start()
    try:
        serve(create_app(args, token), host=os.getenv("OPS_HOST", "127.0.0.1"),
              port=int(os.getenv("OPS_PORT", "5001")))
    finally:
        stop.set()


if __name__ == "__main__":
    main()
