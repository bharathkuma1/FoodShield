"""One-time evaluation of M0-M4 on the real 2021-2022 test period (decision D8).

Guards, in order:
1. ``--confirm-open-test`` must be passed; without it the script stops.
2. The feature lists in ``models/staged.py`` must equal the lists frozen in
   ``configs/week5_frozen_protocol.json``; any change after the freeze stops it.
3. ``data/processed/final_test/OPENED.json`` must not exist; the script writes it
   before computing any test label, so a second run stops.

``--dry-run`` exercises the exact same code on stand-in windows that all end
before the real test: train to 2014, validate 2015-2017, "test" 2018-2020. It reads no 2021+ outcome and
writes nothing under ``final_test/``; use it to check the pipeline before the
real run.

    python scripts/run_final_test_once.py --dry-run
    python scripts/run_final_test_once.py --confirm-open-test     # once, after team go-ahead
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from foodshield.evaluation.week4 import (  # noqa: E402
    best_f1_threshold, confusion, episode_lead_times, future_rise_label, month_key, month_number,
    read_real_prices, roc_auc,
)
from foodshield.features.week4 import build_week4_selected_features  # noqa: E402
from foodshield.features.week5 import add_annual_change_features, add_weather_climatology_anomalies  # noqa: E402
from foodshield.models.staged import FEATURE_VARIANTS, fit_and_score  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", type=Path, default=ROOT / "configs" / "week5_frozen_protocol.json")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--confirm-open-test", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    protocol = json.loads(args.protocol.read_text(encoding="utf-8"))
    if not protocol.get("frozen"):
        sys.exit("Protocol is not marked frozen.")
    if json.loads(json.dumps(FEATURE_VARIANTS)) != protocol["feature_lists"]:
        sys.exit("Feature lists in models/staged.py differ from the frozen protocol. Re-freeze explicitly before any test run.")
    marker = ROOT / protocol["opened_marker"]
    if args.confirm_open_test and marker.exists():
        sys.exit(f"The real test was already opened ({marker}). It is evaluated once only.")

    week4_path = ROOT / protocol["week4_config"]
    week4 = json.loads(week4_path.read_text(encoding="utf-8"))
    split, target, model_config = protocol["split"], protocol["target"], protocol["estimator"]
    horizon, rise = int(target["forecast_horizon_months"]), float(target["minimum_real_price_rise_pct"])
    prices = read_real_prices(ROOT / week4["price_csv"])

    if args.dry_run:
        test_first, last_outcome = split["validation_start"], split["validation_end"]
        # Stand-in windows, all before the real test: train <= 2014, validate 2015-2017, "test" 2018-2020.
        mode_name = "dry_run_pre_test_windows"
    else:
        test_first, last_outcome = split["test_start"], max(prices)
        mode_name = "real_test"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps({"opened_utc": datetime.now(timezone.utc).isoformat(),
                                      "protocol": str(args.protocol.relative_to(ROOT)).replace("\\", "/")}, indent=2) + "\n",
                          encoding="utf-8")

    results = {"mode": mode_name, "generated_utc": datetime.now(timezone.utc).isoformat(), "runs": []}
    spark = make_spark()
    try:
        for lag in (protocol["annual_lags"]["primary"], protocol["annual_lags"]["sensitivity"]):
            panel, _ = build_week4_selected_features(spark, ROOT, week4_path, annual_lag_override=lag)
            panel = add_annual_change_features(spark, ROOT, panel, week4["week2_config"])
            panel = add_weather_climatology_anomalies(spark, ROOT, panel, week4["weather_monthly_csv"], week4["week2_config"])
            needed = sorted({f for v in FEATURE_VARIANTS.values() for feats in v.values() for f in feats})
            rows_by_origin = {r["origin"]: r.asDict() for r in panel.select(
                F.date_format("month", "yyyy-MM").alias("origin"), *needed).collect()}
            for variant in [protocol["variants"]["primary"], *protocol["variants"]["robustness"]]:
                feats_all = sorted({f for feats in FEATURE_VARIANTS[variant].values() for f in feats})

                def rows_between(first, last_outcome_month):
                    out = []
                    for o, row in sorted(rows_by_origin.items()):
                        if month_number(o) < month_number(first) or month_number(o) + horizon > month_number(last_outcome_month):
                            continue
                        if any(row.get(f) is None for f in feats_all):
                            continue
                        label = future_rise_label(prices, o, horizon, rise, last_outcome_month)
                        if label is not None:
                            out.append({**row, "origin": o, "label": label})
                    return out

                if args.dry_run:
                    train = rows_between(split["training_start"], "2014-12")
                    valid = rows_between("2015-01", split["training_end"])
                else:
                    train = rows_between(split["training_start"], split["training_end"])
                    valid = rows_between(split["validation_start"], split["validation_end"])
                test = rows_between(test_first, last_outcome)
                run = {"annual_lag": lag, "variant": variant, "train_origins": len(train), "validation_origins": len(valid),
                       "test_origins": len(test), "test_window": [test[0]["origin"], test[-1]["origin"]] if test else None,
                       "test_stress_months": sum(r["label"] for r in test), "stages": {}}
                for stage, feats in FEATURE_VARIANTS[variant].items():
                    scores, fit = fit_and_score(spark, train, valid + test, feats, model_config)
                    cut = best_f1_threshold([scores[r["origin"]] for r in valid], [r["label"] for r in valid])
                    t_scores = [scores[r["origin"]] for r in test]
                    t_labels = [r["label"] for r in test]
                    alerts = [int(s >= cut) for s in t_scores]
                    run["stages"][stage] = {
                        "cutoff_from_validation": cut, **confusion(t_scores, t_labels, cut), "auc": roc_auc(t_scores, t_labels),
                        "episodes": episode_lead_times(prices, [r["origin"] for r in test], t_labels, alerts, horizon, rise),
                        "monthly": [{"origin": r["origin"], "label": r["label"], "score": s, "alert": a}
                                    for r, s, a in zip(test, t_scores, alerts)],
                        **fit,
                    }
                run["baselines"] = {
                    "always_alert": confusion([1.0] * len(test), [r["label"] for r in test], 0.5),
                    "momentum_rule": confusion([float(r["price_real_mom_change"] > 0) for r in test], [r["label"] for r in test], 0.5),
                }
                results["runs"].append(run)
    finally:
        spark.stop()

    out_dir = ROOT / "data" / "processed" / ("final_test" if args.confirm_open_test else "final_test_dry_run")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "final_test_report.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    for run in results["runs"]:
        print(mode_name, "lag", run["annual_lag"], run["variant"], "test", run["test_origins"], run["test_stress_months"],
              {s: (round(d["f1"] or 0, 2), None if d["auc"] is None else round(d["auc"], 2)) for s, d in run["stages"].items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
