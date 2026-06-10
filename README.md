# Automated Daily Operations Monitoring Pipeline

Production-style Python + n8n pipeline for monitoring daily e-commerce operations. The project turns raw Olist order data into daily KPIs, detects operational anomalies, and exposes a small local service that can be triggered by n8n.

## What It Does

- Builds daily order, revenue, cancellation, and average-order-value KPIs.
- Fills missing calendar dates so rolling baselines are stable.
- Detects revenue drops, order drops, AOV drops, and cancellation spikes against a 7-day rolling baseline.
- Sends structured anomaly payloads to an n8n webhook.
- Provides a Flask endpoint so scheduled tools can trigger the detector safely.

## Repository Contents

| File | Purpose |
| --- | --- |
| `build_daily_kpi.py` | Creates `daily_ops_metrics.csv` and `daily_ops_metrics.xlsx` from raw Olist CSV exports. |
| `detect_anomalies.py` | Simulates daily monitoring, evaluates threshold rules, and optionally posts to n8n. |
| `run_service.py` | Local authenticated Flask service for `/health` and `/run`. |
| `daily_ops_metrics.csv` | Lightweight sample output used by the detector demo. |
| `.env.example` | Required environment variables for local runs. |

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run anomaly detection against the included sample KPI file:

```bash
python detect_anomalies.py --dry-run
```

Start the local trigger service:

```bash
export OPS_AGENT_TOKEN="replace-with-a-strong-token"
export OPS_WEBHOOK_URL="http://127.0.0.1:5678/webhook/ops-insight"
python run_service.py
```

Trigger it:

```bash
curl -X POST http://127.0.0.1:5001/run \
  -H "X-OPS-TOKEN: replace-with-a-strong-token"
```

## Rebuild KPI Data

Download the Brazilian E-Commerce Public Dataset by Olist and place these files in the project root:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`

Then run:

```bash
python build_daily_kpi.py
```

Raw dataset exports are intentionally ignored by Git to keep the repository lightweight.

## n8n Integration

Configure an n8n workflow to call:

- Method: `POST`
- URL: `http://127.0.0.1:5001/run`
- Header: `X-OPS-TOKEN: <your OPS_AGENT_TOKEN>`

The detector posts a JSON payload to `OPS_WEBHOOK_URL` when a webhook URL is configured. Use `--dry-run` for local testing without sending data.

## Notes

- Do not commit OAuth client secrets, `.env`, or local webhook tokens.
- `run_state.json` is generated locally to advance the simulation cursor and is ignored by Git.
- The thresholds in `detect_anomalies.py` are intentionally simple and explainable for operations monitoring demos.
