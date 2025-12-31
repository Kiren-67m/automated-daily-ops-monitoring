from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE_DIR, "detect_anomalies.py")

# 简单鉴权：防止别人随便触发你本地脚本
TOKEN = os.environ.get("OPS_AGENT_TOKEN", "kiren-ops-123")

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/run")
def run():
    if request.headers.get("X-OPS-TOKEN", "") != TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    result = subprocess.run(
        ["python3", SCRIPT],
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        timeout=120
    )

    return jsonify({
        "exit_code": result.returncode,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }), 200

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001)