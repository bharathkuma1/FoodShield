"""Week 8: export the monthly model inputs used by the dashboard's "What if?" tab.

Builds the same panel as the Week 5 ablation (primary settings: corrected features, 2-year
annual lag) and writes one row per month with every input of the M0-M4 stages. It then
checks that the frozen one-time-test models reproduce their 2021-2022 scores from these
inputs, so the dashboard's calculator gives exactly the reported scores before any change.

Run once (needs Spark); the dashboard builder only reads the CSV:

    python scripts/export_week8_dashboard_features.py
        -> data/processed/week8_dashboard/monthly_features.csv
"""

from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from foodshield.features.week4 import build_week4_selected_features  # noqa: E402
from foodshield.features.week5 import add_annual_change_features, add_weather_climatology_anomalies  # noqa: E402
from foodshield.models.staged import FEATURE_VARIANTS  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402

OUT = ROOT / "data" / "processed" / "week8_dashboard" / "monthly_features.csv"


def main() -> int:
    week4_path = ROOT / "configs" / "week4_v2.local.json"
    week4 = json.loads(week4_path.read_text(encoding="utf-8"))
    features = sorted({f for feats in FEATURE_VARIANTS["corrected"].values() for f in feats})
    spark = make_spark()
    try:
        panel, _ = build_week4_selected_features(spark, ROOT, week4_path, annual_lag_override=2)
        panel = add_annual_change_features(spark, ROOT, panel, week4["week2_config"])
        panel = add_weather_climatology_anomalies(spark, ROOT, panel, week4["weather_monthly_csv"], week4["week2_config"])
        rows = [r.asDict() for r in panel.select(F.date_format("month", "yyyy-MM").alias("origin"), *features)
                .where(F.col("month") <= F.lit("2022-12-31")).orderBy("month").collect()]
    finally:
        spark.stop()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["origin", *features])
        for r in rows:
            writer.writerow([r["origin"], *("" if r[f] is None else repr(float(r[f])) for f in features)])

    by_month = {r["origin"]: r for r in rows}
    test = json.loads((ROOT / "data" / "processed" / "final_test" / "final_test_report.json").read_text(encoding="utf-8"))
    primary = next(r for r in test["runs"] if r["variant"] == "corrected" and r["annual_lag"] == 2)
    worst = 0.0
    for stage, s in primary["stages"].items():
        for m in s["monthly"]:
            x = by_month[m["origin"]]
            eta = s["intercept"] + sum(c * float(x[f]) for f, c in s["coefficients"].items())
            worst = max(worst, abs(1.0 / (1.0 + math.exp(-eta)) - m["score"]))
    print(f"Wrote {OUT} ({len(rows)} months, {len(features)} inputs); "
          f"largest difference from the reported test scores: {worst:.2e}")
    if worst > 1e-9:
        raise SystemExit("Frozen models do not reproduce the test scores from these inputs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
