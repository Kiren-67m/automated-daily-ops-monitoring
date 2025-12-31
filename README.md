# Automated Daily Operations Monitoring Pipeline

This project demonstrates a production-style Python + n8n pipeline for monitoring daily e-commerce operations.

## What it does
- Aggregates raw order, item, and payment data into daily KPIs
- Applies rolling 7-day baseline anomaly detection
- Automatically reports daily insights to Google Sheets

## Tech Stack
- Python (pandas)
- n8n (workflow automation)
- Google Sheets API

## Metrics
- Orders count
- Revenue
- Cancellation volume
- Average order value (AOV)

## Use Case
Designed for operations and analytics teams to detect revenue drops or cancellation spikes early.

> Sample data included for demonstration only.
