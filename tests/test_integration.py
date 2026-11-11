"""Slower integration tests on the real Ukraine inputs (about 1-2 minutes; set FOODSHIELD_SKIP_SLOW_TESTS=1 to skip).

1. As-of (no future data): replaying all 1,864,486 records, the features the monitor computes
   at the moment each month closes must equal the features recomputed after every record has
   arrived. If any feature used later data, the two would differ.
2. Streaming = batch: for every month, the 13 corrected features computed by the streaming
   monitor equal the features built by the Spark batch pipeline the frozen models were trained on
   (same 24-point weighted weather, same price series, same FAOSTAT extracts, 2-year lag).

    python -m unittest tests.test_integration -v
"""

from __future__ import annotations

import csv
import json
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from foodshield.streaming.monitor import OnlineMonitor, load_cell_weights, load_frozen_models, replay_offline  # noqa: E402

SLOW = os.environ.get("FOODSHIELD_SKIP_SLOW_TESTS") != "1"
FEATURES = ["price_real_mom_change", "price_real_anomaly_z_12m", "precipitation_sum_mm_clim_z", "t2m_max_mean_c_clim_z",
            "t2m_min_mean_c_clim_z", "production_t_yoy_sym_change", "yield_kg_ha_yoy_sym_change",
            "area_harvested_ha_yoy_sym_change", "import_quantity_t_yoy_sym_change", "export_quantity_t_yoy_sym_change",
            "import_value_1000_usd_yoy_sym_change", "export_value_1000_usd_yoy_sym_change", "m4_price_precip_clim_interaction"]


def same(a, b, tol=1e-9) -> bool:
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) <= tol * max(1.0, abs(a)))


def models():
    return load_frozen_models(ROOT / "data/processed/final_test/final_test_report.json", "corrected", 2, ["M0", "M4"])


class CapturingMonitor(OnlineMonitor):
    """Records each month's features at the moment the month closes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.at_close: dict[str, dict] = {}

    def score_ready(self):
        for month in self.ready_months():
            self.at_close[month] = self.features(month)
        return super().score_ready()


@unittest.skipUnless(SLOW, "slow tests skipped")
class AsOfTest(unittest.TestCase):
    def test_features_at_close_equal_features_with_all_data(self):
        from foodshield.streaming.records import GridWeather, replay_records

        grid = GridWeather(ROOT / "data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.csv")
        monitor = CapturingMonitor(load_cell_weights(ROOT / "configs/d9b_grid_cells.json"), models(), 2)
        replay_offline(replay_records(grid, ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv",
                                      ROOT / "data/raw/faostat_ukraine_wheat_production_2026-11.csv",
                                      ROOT / "data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv", 2), monitor)
        self.assertEqual(len(monitor.at_close), 156)
        mismatches = [(m, f) for m, feats in monitor.at_close.items() for f in FEATURES
                      if not same(feats[f], monitor.features(m)[f], tol=0)]
        self.assertEqual(mismatches, [])


@unittest.skipUnless(SLOW, "slow tests skipped")
class StreamingEqualsBatchTest(unittest.TestCase):
    def test_all_features_match_spark_pipeline(self):
        from foodshield.features.week4 import build_week4_selected_features
        from foodshield.features.week5 import add_annual_change_features, add_weather_climatology_anomalies
        from foodshield.streaming.records import annual_records
        from pyspark.sql import functions as F
        from run_week4_rolling_origin import make_spark

        week4_path = ROOT / "configs/week4_v2.local.json"
        week4 = json.loads(week4_path.read_text(encoding="utf-8"))
        spark = make_spark()
        try:
            panel, _ = build_week4_selected_features(spark, ROOT, week4_path, annual_lag_override=2)
            panel = add_annual_change_features(spark, ROOT, panel, week4["week2_config"])
            panel = add_weather_climatology_anomalies(spark, ROOT, panel, week4["weather_monthly_csv"], week4["week2_config"])
            batch = {r["origin"]: r.asDict() for r in panel.select(F.date_format("month", "yyyy-MM").alias("origin"), *FEATURES).collect()}
        finally:
            spark.stop()

        monitor = OnlineMonitor({"x": 1.0}, models(), 2)
        with (ROOT / week4["price_csv"]).open(encoding="utf-8-sig") as stream:
            for r in csv.DictReader(stream):
                monitor.prices[f"{r['year']}-{int(r['month']):02d}"] = float(r["real_price_dec_2000_lcu_per_tonne"])
        with (ROOT / week4["weather_monthly_csv"]).open(encoding="utf-8") as stream:
            for r in csv.DictReader(stream):
                monitor.weather[r["month"]] = [float(r[k]) for k in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")]
        for _, _, r in annual_records(ROOT / "data/raw/faostat_ukraine_wheat_production_2026-11.csv",
                                      ROOT / "data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv", 2):
            monitor.add_annual(r["annual_field"], r["data_year"], r["value"])

        months = sorted(m for m in batch if m in monitor.weather)
        self.assertGreaterEqual(len(months), 156)
        mismatches = [(m, f, batch[m][f], monitor.features(m)[f]) for m in months for f in FEATURES
                      if not same(batch[m][f], monitor.features(m)[f])]
        self.assertEqual(mismatches, [], mismatches[:5])
        complete = sum(all(batch[m][f] is not None for f in FEATURES) for m in months)
        self.assertGreater(complete, 100)


if __name__ == "__main__":
    unittest.main()
