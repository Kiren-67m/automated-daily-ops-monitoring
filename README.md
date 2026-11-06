# Automated Daily Operations Monitoring Pipeline

A local Python prototype for reviewing daily e-commerce operations. It turns historical Olist orders into daily KPIs, compares each replayed day with prior days, and produces an explainable alert payload. An optional Flask service and webhook connect it to n8n.

## What It Does

- Builds daily order, revenue, cancellation, and average-order-value KPIs.
- Fills missing calendar dates so rolling baselines are stable.
- Detects revenue drops, order drops, AOV drops, and cancellation spikes against a 7-day rolling baseline.
- Sends structured anomaly payloads to an n8n webhook.
- Provides a token-protected local Flask endpoint so scheduled tools can trigger the detector.

## How the pipeline works

```text
Olist CSVs → daily KPI table → previous 7-day baseline → threshold rules
                                                            ↓
                                                   JSON + text summary
                                                            ↓ optional
                                                        n8n webhook
```

The baseline excludes the day being evaluated. Default alert thresholds are:

| Metric | Medium | High |
| --- | --- | --- |
| Revenue | Drop ≥15% | Drop ≥25% |
| Orders | Drop ≥12% | Drop ≥20% |
| Average order value | Drop ≥15% | Drop ≥25% |
| Canceled/unavailable order count | Rise ≥60% | Rise ≥120% |

These are fixed rules, not a trained anomaly model. Cancellation alerts compare counts, not cancellation rates. Small baselines can produce large percentage changes; a zero baseline currently yields no percentage-change alert.

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

With a fresh `run_state.json`, the bundled sample starts at `2017-01-12`. The JSON payload includes this summary:

```text
Date: 2017-01-12 | Status: normal
Orders: 13 (avg 9.9) | Revenue: 2502.69 (avg 1337.31)
Canceled: 0 (avg 0.1) | AOV: 192.51 (avg 205.44)
Signals:
- none
```

Each run advances the local simulation cursor, including `--dry-run`; after the last eligible row it wraps to the beginning. To replay from the start without altering existing state, use a new path such as `--state-file demo_state.json`.

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
- `olist_order_payments_dataset.csv` (optional: when present, revenue uses payment values; otherwise it uses item prices plus freight)

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

An n8n workflow export is not included; configure the trigger and receiving workflow in your own instance. The detector posts a JSON payload, including normal-status results, to `OPS_WEBHOOK_URL` when a webhook URL is configured. Use `--dry-run` for local testing without sending data.

## Notes

- Do not commit OAuth client secrets, `.env`, or local webhook tokens.
- `run_state.json` is generated locally to advance the simulation cursor and is ignored by Git.
- This is a historical-data replay, not a deployed daily ingestion service. Alert precision, business impact and production reliability have not been measured.
