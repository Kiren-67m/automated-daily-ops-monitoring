"""Small Flask wrapper for triggering the anomaly detector from n8n."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from flask import Flask, jsonify, request


app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
SCRIPT = BASE_DIR / "detect_anomalies.py"
TOKEN = os.getenv("OPS_AGENT_TOKEN")


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.post("/run")
def run():
    if not TOKEN:
        return jsonify({"error": "OPS_AGENT_TOKEN is not configured"}), 500

    if request.headers.get("X-OPS-TOKEN", "") != TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    status_code = 200 if result.returncode == 0 else 500
    return (
        jsonify(
            {
                "exit_code": result.returncode,
                "stdout": result.stdout[-4000:],
                "stderr": result.stderr[-4000:],
            }
        ),
        status_code,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001)
