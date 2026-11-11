"""Fast correctness tests (no Spark, no network): labels, price features, monitor logic, replay records.

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.evaluation.week4 import (  # noqa: E402
    best_f1_threshold, confusion, future_rise_label, month_key, month_number, price_features,
)
from foodshield.streaming.monitor import OnlineMonitor  # noqa: E402
from foodshield.streaming.records import annual_records, kafka_key, price_records  # noqa: E402


class LabelTests(unittest.TestCase):
    prices = {month_key(month_number("2015-01") + i): p for i, p in enumerate([100, 100, 105, 112, 90, 95, 99, 98])}

    def test_rise_within_horizon(self):
        # 2015-01: max(t+1..t+3) = 112 -> +12% >= 10% -> stress
        self.assertEqual(future_rise_label(self.prices, "2015-01", 3, 10), 1)
        # 2015-04 (112): next three months 90, 95, 99 -> no rise
        self.assertEqual(future_rise_label(self.prices, "2015-04", 3, 10), 0)

    def test_exactly_threshold_counts(self):
        prices = {"2015-01": 100.0, "2015-02": 110.0, "2015-03": 100.0, "2015-04": 100.0}
        self.assertEqual(future_rise_label(prices, "2015-01", 3, 10), 1)

    def test_missing_future_and_holdout_guard(self):
        self.assertIsNone(future_rise_label(self.prices, "2015-07", 3, 10))       # future months missing
        self.assertIsNone(future_rise_label(self.prices, "2015-02", 3, 10, last_allowed="2015-04"))  # window ends 2015-05


class PriceFeatureTests(unittest.TestCase):
    def test_mom_and_z(self):
        prices = {month_key(month_number("2014-01") + i): 100.0 + i for i in range(13)}  # 100..112
        mom, z = price_features(prices, "2015-01")
        self.assertAlmostEqual(mom, 112 / 111 - 1)
        window = [100.0 + i for i in range(12)]
        mean = sum(window) / 12
        sd = math.sqrt(sum((v - mean) ** 2 for v in window) / 11)
        self.assertAlmostEqual(z, (112 - mean) / sd)

    def test_z_needs_twelve_prior_months(self):
        prices = {month_key(month_number("2014-02") + i): 100.0 + i for i in range(12)}
        self.assertIsNone(price_features(prices, "2015-01")[1])


class MetricTests(unittest.TestCase):
    def test_confusion_and_threshold(self):
        c = confusion([0.9, 0.8, 0.2, 0.1], [1, 0, 1, 0], 0.5)
        self.assertEqual((c["true_positive"], c["false_positive"], c["false_negative"]), (1, 1, 1))
        self.assertAlmostEqual(c["f1"], 0.5)
        self.assertLessEqual(best_f1_threshold([0.9, 0.8, 0.2, 0.1], [1, 1, 0, 0]), 0.8)


def tiny_monitor(lag: int = 2) -> OnlineMonitor:
    model = {"coefficients": {"price_real_mom_change": 2.0, "price_real_anomaly_z_12m": -0.5}, "intercept": -1.0,
             "cutoff": 0.3, "version": "test"}
    return OnlineMonitor({"a": 0.75, "b": 0.25}, {"M0": model}, lag)


def partial(cell, month, values, day_from=1):
    """Build one cell-month partial aggregate from daily (p, tx, tn) tuples starting at day_from."""
    mask = 0
    for i in range(len(values)):
        mask |= 1 << (day_from + i)
    return (cell, month, sum(v[0] for v in values), sum(v[1] for v in values), sum(v[2] for v in values),
            len(values), len(values), len(values), len(values), mask, 1000.0)


class MonitorTests(unittest.TestCase):
    def test_month_closes_only_when_all_cells_complete(self):
        m = tiny_monitor()
        m.add_weather_partials([partial("a", "2015-02", [(1.0, 10.0, 0.0)] * 28)])
        self.assertNotIn("2015-02", m.weather)
        m.add_weather_partials([partial("b", "2015-02", [(3.0, 14.0, 4.0)] * 28)])
        p, tx, tn = m.weather["2015-02"]
        self.assertAlmostEqual(p, 0.75 * 28 + 0.25 * 84)        # precipitation is a monthly sum
        self.assertAlmostEqual(tx, 0.75 * 10 + 0.25 * 14)       # temperatures are monthly means

    def test_split_batches_and_duplicate_days(self):
        m = tiny_monitor()
        m.add_weather_partials([partial("a", "2015-02", [(1.0, 1.0, 1.0)] * 10)])
        m.add_weather_partials([partial("a", "2015-02", [(1.0, 1.0, 1.0)] * 10)])  # same days again
        self.assertEqual(m.counters["day_overlaps"], 1)
        m.add_weather_partials([partial("a", "2015-02", [(1.0, 1.0, 1.0)] * 18, day_from=11)])
        self.assertIn("a", m.cells_done["2015-02"])
        self.assertAlmostEqual(m.cells_done["2015-02"]["a"][0], 28.0)  # not double counted

    def test_weights_renormalised_over_missing_values(self):
        m = tiny_monitor()
        a = partial("a", "2015-02", [(2.0, 1.0, 1.0)] * 28)
        b = list(partial("b", "2015-02", [(0.0, 3.0, 3.0)] * 28))
        b[5] = 0  # no precipitation values for cell b
        m.add_weather_partials([a, tuple(b)])
        self.assertAlmostEqual(m.weather["2015-02"][0], 56.0)  # only cell a counts, weight renormalised

    def test_climatology_z_uses_prior_years_only(self):
        m = tiny_monitor()
        for y, v in zip(range(2005, 2016), [10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 100]):
            m.weather[f"{y}-06"] = [float(v), 0.0, 0.0]
        prior = [float(v) for v in [12, 14, 16, 18, 20, 22, 24, 26, 28, 10]]  # 2005-2014, last 10 years
        mean = sum(prior) / 10
        sd = math.sqrt(sum((v - mean) ** 2 for v in prior) / 9)
        self.assertAlmostEqual(m._clim_z("2015-06", 0), (100 - mean) / sd)
        m.weather = {f"{y}-06": [1.0, 0.0, 0.0] for y in range(2011, 2016)}
        self.assertIsNone(m._clim_z("2015-06", 0))  # only 4 prior years

    def test_yoy_bounded_and_zero_safe(self):
        m = tiny_monitor()
        m.add_annual("x", 2013, 0.0)
        m.add_annual("x", 2014, 5.0)
        self.assertAlmostEqual(m._yoy("x", 2014), 2.0)
        m.add_annual("x", 2015, 0.0)
        self.assertAlmostEqual(m._yoy("x", 2015), -2.0)
        m.add_annual("y", 2014, 0.0)
        m.add_annual("y", 2015, 0.0)
        self.assertIsNone(m._yoy("y", 2015))

    def test_annual_feature_year_follows_lag(self):
        m = tiny_monitor(lag=2)
        for y in range(2010, 2016):
            m.add_annual("production_t", y, 100.0 + y)
        m.weather["2015-03"] = [1.0, 1.0, 1.0]
        self.assertAlmostEqual(m.features("2015-03")["production_t_yoy_sym_change"], m._yoy("production_t", 2013))

    def test_score_is_logistic_of_frozen_coefficients(self):
        m = tiny_monitor()
        for i in range(14):
            k = month_key(month_number("2014-01") + i)
            m.prices[k] = 100.0 + (i % 5)
            m.weather[k] = [1.0, 1.0, 1.0]
        alerts = [a for a in m.score_ready() if a["observation_month"] == "2015-02"]
        mom, z = price_features(m.prices, "2015-02")
        expected = 1 / (1 + math.exp(-(-1.0 + 2.0 * mom - 0.5 * z)))
        self.assertAlmostEqual(alerts[0]["score"], expected)
        self.assertEqual(alerts[0]["alert"], expected >= 0.3)

    def test_state_round_trip(self):
        m = tiny_monitor()
        m.add_price("2015-01", 123.0, 5.0)
        m.last_batch_id = 7
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "state.json"
            m.save(path)
            n = tiny_monitor()
            self.assertTrue(n.load(path))
        self.assertEqual((n.prices, n.last_batch_id), ({"2015-01": 123.0}, 7))


class RecordTests(unittest.TestCase):
    def test_availability_rules_and_keys(self):
        prices = price_records(ROOT / "data" / "processed" / "week4_ukraine_wheat_farmgate_realprice_candidate.csv")
        dec = next(r for _, _, r in prices if r["observation_date"] == "2015-12-01")
        self.assertEqual(dec["available_at"], "2016-01-01T00:00:00Z")
        self.assertEqual(kafka_key(dec), "UKR:wheat")
        annual = annual_records(ROOT / "data" / "raw" / "faostat_ukraine_wheat_production_2026-11.csv",
                                ROOT / "data" / "raw" / "faostat_ukraine_wheat_trade_all_measures_2026-11.csv", 2)
        r2015 = [r for _, _, r in annual if r["data_year"] == 2015]
        self.assertEqual(len(r2015), 7)
        self.assertTrue(all(r["available_at"] == "2017-01-01T00:00:00Z" for r in r2015))
        self.assertEqual([t[:2] for t in annual], sorted(t[:2] for t in annual))

    def test_frozen_protocol_unchanged(self):
        import hashlib
        protocol = ROOT / "configs" / "week5_frozen_protocol.json"
        # Compare with LF line endings so a Windows checkout that converted them does not count as a change.
        self.assertTrue(hashlib.sha256(protocol.read_bytes().replace(b"\r\n", b"\n")).hexdigest().startswith("c6fc95aa"))
        opened = json.loads((ROOT / "data" / "processed" / "final_test" / "OPENED.json").read_text(encoding="utf-8"))
        frozen_on = json.loads(protocol.read_text(encoding="utf-8"))["frozen_on"]
        self.assertEqual(opened["protocol"], "configs/week5_frozen_protocol.json")
        self.assertGreaterEqual(opened["opened_utc"][:10], frozen_on)


class DashboardServerTests(unittest.TestCase):
    def test_live_payload_follows_newest_run(self):
        import os
        sys.path.insert(0, str(ROOT / "scripts"))
        from serve_dashboard import live_payload
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            self.assertIsNone(live_payload(work, None)["run"])
            for name, stamp in (("old", 1_000_000), ("demo", 2_000_000)):
                run = work / name
                (run / "alerts").mkdir(parents=True)
                (run / "effective_config.json").write_text("{}", encoding="utf-8")
                os.utime(run / "effective_config.json", (stamp, stamp))
            alert = {"observation_month": "2022-03", "model_stage": "M4", "score": 0.41, "cutoff": 0.25, "alert": True,
                     "latency_s": 2.1, "reason_codes": ["price_real_mom_change:+0.500"], "features": {}, "batch_id": 7, "extra": 1}
            (work / "demo" / "alerts" / "batch-000007.jsonl").write_text(json.dumps(alert) + "\n", encoding="utf-8")
            (work / "demo" / "consumer_status.json").write_text('{"rows": 10}', encoding="utf-8")
            payload = live_payload(work, None)
            self.assertEqual((payload["run"], payload["status"], payload["finished"]), ("demo", {"rows": 10}, False))
            self.assertEqual(len(payload["alerts"]), 1)
            self.assertNotIn("extra", payload["alerts"][0])
            self.assertEqual(live_payload(work, "old")["alerts"], [])


if __name__ == "__main__":
    unittest.main()
