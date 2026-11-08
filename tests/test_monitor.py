import csv
import json
import os
import tempfile
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from filelock import FileLock

from detect_anomalies import DataNotReady, DeliveryError, parse_args, run_once
from run_service import create_app, run_schedule


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.input = root / "metrics.csv"
        self.state = root / "state" / "test.json"
        self.day = date(2025, 3, 9)
        self.rows = [
            [(self.day - timedelta(days=offset)).isoformat(), 10, 1000, 0, 100]
            for offset in range(7, 0, -1)
        ] + [[self.day.isoformat(), 5, 300, 3, 60]]
        self.write_metrics()
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {"OPS_TIMEZONE": "America/Chicago"}, clear=True).start()
        patch("detect_anomalies.load_dotenv").start()

    def write_metrics(self):
        with self.input.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "orders_count", "revenue", "canceled_orders", "avg_order_value"])
            writer.writerows(self.rows)

    def live_args(self, *options):
        return parse_args(["--mode", "live", "--date", self.day.isoformat(), "--input", str(self.input),
                           "--state-file", str(self.state), *options])

    def test_replay_preserves_cursor_until_delivery_succeeds(self):
        args = parse_args(["--state-file", str(self.state), "--webhook-url", "http://localhost:9"])
        args.dry_run = True
        self.assertEqual(run_once(args)["date"], "2017-01-12")
        self.assertFalse(self.state.parent.exists())
        args.dry_run = False
        with patch("detect_anomalies.requests.post", return_value=SimpleNamespace(status_code=500)):
            with self.assertRaises(DeliveryError):
                run_once(args)
        self.assertFalse(self.state.exists())
        with patch("detect_anomalies.requests.post", return_value=SimpleNamespace(status_code=204)):
            self.assertEqual(run_once(args)["sim_cursor"], 0)
            self.assertEqual(run_once(args)["sim_cursor"], 1)

    def test_live_webhook_failure_is_retryable_and_success_is_deduplicated(self):
        args = self.live_args("--webhook-url", "http://localhost:9")
        with patch("detect_anomalies.requests.post", side_effect=[SimpleNamespace(status_code=500),
                                                                  SimpleNamespace(status_code=204)]) as post:
            with self.assertRaises(DeliveryError):
                run_once(args)
            self.assertFalse(self.state.exists())
            result = run_once(args)
            self.assertEqual(result["date"], self.day.isoformat())
            self.assertEqual(result["status"], "anomaly_detected")
            self.assertEqual({s["metric"] for s in result["signals"]},
                             {"revenue", "orders_count", "avg_order_value", "canceled_orders"})
            self.assertEqual(run_once(args)["status"], "already_processed")
            self.assertEqual(post.call_count, 2)
            self.assertEqual({call.kwargs["headers"]["Idempotency-Key"] for call in post.call_args_list},
                             {result["event_id"]})
            self.assertEqual(json.loads(self.state.read_text()), {"completed": [self.day.isoformat()]})

    def test_missing_and_duplicate_days_do_not_mark_completion(self):
        removed = self.rows.pop(1)
        self.write_metrics()
        with self.assertRaisesRegex(DataNotReady, removed[0]):
            run_once(self.live_args())
        self.assertFalse(self.state.exists())
        self.rows.extend([removed, self.rows[-1]])
        self.write_metrics()
        with self.assertRaisesRegex(ValueError, "exactly one row per date"):
            run_once(self.live_args())
        self.assertFalse(self.state.exists())

    def test_live_dry_run_does_not_deliver_or_create_state(self):
        args = self.live_args("--dry-run", "--webhook-url", "http://localhost:9")
        with patch("detect_anomalies.requests.post") as post:
            self.assertEqual(run_once(args)["signals_count"], 4)
            post.assert_not_called()
        self.assertFalse(self.state.parent.exists())

    def test_service_requires_token_and_rejects_overlapping_run(self):
        client = create_app(self.live_args(), "test-token").test_client()
        self.assertTrue(client.get("/health").json["ok"])
        self.assertEqual(client.post("/run").status_code, 401)
        headers = {"X-OPS-TOKEN": "test-token"}
        self.state.parent.mkdir()
        with FileLock(f"{self.state}.lock"):
            self.assertEqual(client.post("/run", headers=headers).status_code, 409)
        response = client.post("/run", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["result"]["status"], "anomaly_detected")
        self.assertEqual(client.post("/run", headers=headers).json["result"]["status"], "already_processed")

    def test_scheduler_retries_after_delay_and_only_completes_yesterday_once(self):
        args = self.live_args()
        now = datetime(2025, 3, 10, 8, tzinfo=args.business_timezone)
        stop = Mock()
        stop.is_set.side_effect = [False, False, False, False, True]
        with patch("run_service.datetime") as clock, patch("run_service.monotonic", side_effect=[0, 0, 240, 300]), patch(
                "run_service.run_once", side_effect=[DataNotReady("CSV has not arrived"), {"date": "2025-03-09"}]
        ) as run, self.assertLogs("daily_ops", level="WARNING"):
            clock.now.return_value = now
            run_schedule(args, time(8), stop)
        self.assertEqual(run.call_args_list, [call(args, target_date=self.day)] * 2)


if __name__ == "__main__":
    unittest.main()
