"""Render a side-by-side view of the demo's actual report and alert."""

from html import escape
from pathlib import Path
from string import Template


METRICS = (
    ("orders_count", "Orders", ",.0f", ",.1f", ""),
    ("revenue", "Captured revenue", ",.2f", ",.2f", "$"),
    ("avg_order_value", "Average order value", ",.2f", ",.2f", "$"),
    ("canceled_orders", "Canceled orders", ",.0f", ",.1f", ""),
)


def render_card(payload, *, receipt=None):
    day = payload["date"]
    metrics = payload["metrics"]
    alert = receipt is not None
    signals = {signal["metric"]: signal for signal in payload["signals"]}
    rows = []
    for key, label, actual_format, baseline_format, prefix in METRICS:
        actual, prior = metrics[key]["actual"], metrics[key]["baseline"]
        change = f"{(actual - prior) / prior:+.0%}" if prior else "—"
        style = ' class="breach"' if key in signals else ""
        rows.append(f'<tr><th scope="row">{label}</th><td>{prefix}{actual:{actual_format}}</td>'
                    f'<td>{prefix}{prior:{baseline_format}}</td><td{style}>{change}</td></tr>')
    if alert:
        items = "".join(f'<li><b>{escape(s["severity"].upper())}</b> {escape(s["details"])}</li>'
                        for s in payload["signals"])
        detail = f'<ul class="signals">{items}</ul>'
        delivery = (f'Local receiver acknowledged HTTP {receipt["http_status"]}. '
                    f'Event <code>{escape(payload["event_id"])}</code>.')
        title, badge, kind = "Exception alert", f"{len(signals)} alert signals", "alert"
    else:
        detail = '<p class="clear">All four metrics stayed within their alert thresholds.</p>'
        delivery = "The daily check completed with 0 webhook deliveries."
        title, badge, kind = "Daily report", "Normal", "normal"
    return f'''<section class="card {kind}" id="{kind}">
      <div class="card-head"><span class="pill">{badge}</span><time datetime="{escape(day)}">{escape(day)}</time></div>
      <h2>{title}</h2>
      <div class="table-wrap"><table><thead><tr><th scope="col">Metric</th><th scope="col">Actual</th><th scope="col">Prior 7d</th><th scope="col">Change</th></tr></thead>
      <tbody>{"".join(rows)}</tbody></table></div>
      {detail}
      <footer>{delivery}</footer>
    </section>'''


def render_preview(output, results):
    dataset = results["dataset"]
    receipt = results["received_webhook"]
    template = Template(Path(__file__).with_name("demo-preview.html").read_text(encoding="utf-8"))
    html = template.substitute(
        timezone=escape(dataset["timezone"]), currency=escape(dataset["currency"]), days=dataset["days"],
        orders=f'{dataset["orders"]:,}', items=f'{dataset["items"]:,}', payment_records=f'{dataset["payment_records"]:,}',
        normal_card=render_card(results["normal_report"]),
        alert_card=render_card(receipt["payload"], receipt=receipt),
        repeat_status=escape(results["repeat_run"]["status"]), deliveries=results["successful_deliveries"],
        failed_http=results["alert_attempts"][0]["http_status"], delivered_http=receipt["http_status"],
        history_count=len(results["history"]),
    )
    (output / "demo-preview.html").write_text(html, encoding="utf-8")
