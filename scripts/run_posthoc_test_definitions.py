"""Post-hoc scoring of the 2021-2022 test window under the other stress definitions.

The one-time test (``run_final_test_once.py``) was opened once, for the project definition only
(at least a 10% real-price rise within 3 months). This script scores the same window for the other
rise / horizon combinations offered by the dashboard (5/10/15% within 1/3/6 months). It is
exploratory: the test outcomes were already known when it was written, so its results are never
the reported result. It follows the frozen protocol in every other respect (training 2011-2017,
alert cutoff from validation 2018-2020, the same estimator and features, annual lags 2 and 3) and
writes one report per definition in the same format as the one-time test, so the dashboard and the
streaming replay can read them. It never touches ``data/processed/final_test/``.

    python scripts/run_posthoc_test_definitions.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from foodshield.evaluation.week4 import (  # noqa: E402
    best_f1_threshold, confusion, episode_lead_times, future_rise_label, month_number, read_real_prices, roc_auc,
)
from foodshield.features.week4 import build_week4_selected_features  # noqa: E402
from foodshield.features.week5 import add_annual_change_features, add_weather_climatology_anomalies  # noqa: E402
from foodshield.models.staged import FEATURE_VARIANTS, fit_and_score  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402

OUT = ROOT / "data" / "processed" / "posthoc_test_definitions"
RISES, HORIZONS, VARIANT = (5, 10, 15), (1, 3, 6), "corrected"


def main() -> int:
    protocol = json.loads((ROOT / "configs" / "week5_frozen_protocol.json").read_text(encoding="utf-8"))
    week4_path = ROOT / protocol["week4_config"]
    week4 = json.loads(week4_path.read_text(encoding="utf-8"))
    split, model_config = protocol["split"], protocol["estimator"]
    prices = read_real_prices(ROOT / week4["price_csv"])
    last_outcome = max(prices)
    definitions = [(r, h) for r in RISES for h in HORIZONS if (r, h) != (10, 3)]
    reports = {d: {"mode": "posthoc_test", "rise_pct": d[0], "horizon_months": d[1],
                   "note": "Exploratory, scored after the one-time test was opened; not a reported result.",
                   "generated_utc": datetime.now(timezone.utc).isoformat(), "runs": []} for d in definitions}
    spark = make_spark()
    try:
        for lag in (protocol["annual_lags"]["primary"], protocol["annual_lags"]["sensitivity"]):
            panel, _ = build_week4_selected_features(spark, ROOT, week4_path, annual_lag_override=lag)
            panel = add_annual_change_features(spark, ROOT, panel, week4["week2_config"])
            panel = add_weather_climatology_anomalies(spark, ROOT, panel, week4["weather_monthly_csv"], week4["week2_config"])
            needed = sorted({f for v in FEATURE_VARIANTS.values() for feats in v.values() for f in feats})
            rows_by_origin = {r["origin"]: r.asDict() for r in panel.select(
                F.date_format("month", "yyyy-MM").alias("origin"), *needed).collect()}
            feats_all = sorted({f for feats in FEATURE_VARIANTS[VARIANT].values() for f in feats})
            for rise, horizon in definitions:
                def rows_between(first, last):
                    out = []
                    for o, row in sorted(rows_by_origin.items()):
                        if month_number(o) < month_number(first) or month_number(o) + horizon > month_number(last):
                            continue
                        if any(row.get(f) is None for f in feats_all):
                            continue
                        label = future_rise_label(prices, o, horizon, rise, last)
                        if label is not None:
                            out.append({**row, "origin": o, "label": label})
                    return out

                train = rows_between(split["training_start"], split["training_end"])
                valid = rows_between(split["validation_start"], split["validation_end"])
                test = rows_between(split["test_start"], last_outcome)
                labels = [r["label"] for r in test]
                run = {"annual_lag": lag, "variant": VARIANT, "train_origins": len(train), "validation_origins": len(valid),
                       "validation_stress_months": sum(r["label"] for r in valid), "test_origins": len(test),
                       "test_window": [test[0]["origin"], test[-1]["origin"]] if test else None,
                       "test_stress_months": sum(labels), "stages": {}}
                for stage, feats in FEATURE_VARIANTS[VARIANT].items():
                    scores, fit = fit_and_score(spark, train, valid + test, feats, model_config)
                    cut = best_f1_threshold([scores[r["origin"]] for r in valid], [r["label"] for r in valid])
                    t_scores = [scores[r["origin"]] for r in test]
                    alerts = [int(s >= cut) for s in t_scores]
                    run["stages"][stage] = {
                        "cutoff_from_validation": cut, **confusion(t_scores, labels, cut), "auc": roc_auc(t_scores, labels),
                        "episodes": episode_lead_times(prices, [r["origin"] for r in test], labels, alerts, horizon, rise),
                        "monthly": [{"origin": r["origin"], "label": r["label"], "score": s, "alert": a}
                                    for r, s, a in zip(test, t_scores, alerts)],
                        **fit,
                    }
                run["baselines"] = {"always_alert": confusion([1.0] * len(test), labels, 0.5)}
                reports[(rise, horizon)]["runs"].append(run)
                print("lag", lag, f"{rise}% {horizon}m", "test stress", run["test_stress_months"], "of", len(test),
                      {s: round(d["f1"] or 0, 2) for s, d in run["stages"].items()}, flush=True)
    finally:
        spark.stop()
    for (rise, horizon), report in reports.items():
        folder = OUT / f"rise{rise}_h{horizon}"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "final_test_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
