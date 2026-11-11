"""Week 5 ablation: compare M0-M4 on identical origins with one shared estimator.

For each feature variant (see models/staged.py) the script:
1. builds the selected-source Week 4 panel (wheat-area-weighted weather) with the
   configured annual availability lag and adds year-on-year annual change features;
2. fixes one common origin set: origins whose outcome window ends by 2020-12 and
   where every M4 feature is present, so every stage is scored on the same months;
3. validation calibration: fit on 2011-2017 (purged), choose the alert cutoff by F1
   on 2018-2020 (purged);
4. rolling-origin backtest 2014-2020: refit on all earlier origins, cutoff by F1 on
   the training origins, score the year once;
5. reports always-alert and momentum baselines on the same origins.

No outcome after 2020-12 is computed (holdout guard).

    python scripts/run_week5_ablation.py                          # D6 primary: 2-year lag
    python scripts/run_week5_ablation.py --annual-lag 3 --output data/processed/week5_ablation_lag3
"""

from __future__ import annotations

import argparse
import csv
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
from foodshield.models.staged import FEATURE_VARIANTS, STAGES, fit_and_score  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402


def evaluate_variant(spark, rows_by_origin, labels, prices, variant, split, horizon, rise, model_config, first_year):
    stage_features = FEATURE_VARIANTS[variant]
    all_features = sorted({f for feats in stage_features.values() for f in feats})
    origins = sorted(o for o, label in labels.items() if label is not None and o in rows_by_origin
                     and month_number(o) >= month_number(split["training_start"])
                     and all(rows_by_origin[o].get(f) is not None for f in all_features))
    rows = [{**rows_by_origin[o], "origin": o, "label": labels[o]} for o in origins]
    train = [r for r in rows if month_number(r["origin"]) + horizon <= month_number(split["training_end"])]
    valid = [r for r in rows if month_number(r["origin"]) >= month_number(split["validation_start"])
             and month_number(r["origin"]) + horizon <= month_number(split["validation_end"])]
    last_year = int(split["validation_end"][:4])
    years = [y for y in range(first_year, last_year + 1) if any(r["origin"].startswith(f"{y}-") for r in rows)]

    momentum = [float(r["price_real_mom_change"] > 0) for r in rows]
    y_all = [r["label"] for r in rows]
    result = {"variant": variant, "common_origins": len(origins), "first_origin": origins[0], "last_origin": origins[-1],
              "train_origins": len(train), "train_positives": sum(r["label"] for r in train),
              "validation_origins": len(valid), "validation_positives": sum(r["label"] for r in valid),
              "backtest_years": years, "stages": {}, "predictions": []}

    for stage in STAGES:
        feats = stage_features[stage]
        # Validation calibration (D3 split).
        scores, fit = fit_and_score(spark, train, valid, feats, model_config)
        v_scores = [scores[r["origin"]] for r in valid]
        v_labels = [r["label"] for r in valid]
        cut = best_f1_threshold(v_scores, v_labels)
        calibration = {**confusion(v_scores, v_labels, cut), "auc": roc_auc(v_scores, v_labels), **fit}
        # Rolling-origin backtest.
        folds, pooled_alerts, pooled_labels, pooled_origins = [], [], [], []
        for year in years:
            tr = [r for r in rows if month_number(r["origin"]) + horizon <= month_number(f"{year - 1}-12")]
            te = [r for r in rows if r["origin"].startswith(f"{year}-")]
            if len({r["label"] for r in tr}) < 2:
                folds.append({"test_year": year, "skipped": "one class in training"})
                continue
            sc, _ = fit_and_score(spark, tr, tr + te, feats, model_config)
            fold_cut = best_f1_threshold([sc[r["origin"]] for r in tr], [r["label"] for r in tr])
            te_scores = [sc[r["origin"]] for r in te]
            te_labels = [r["label"] for r in te]
            folds.append({"test_year": year, "train_origins": len(tr), "cutoff": fold_cut,
                          **{k: v for k, v in confusion(te_scores, te_labels, fold_cut).items() if k != "threshold"},
                          "auc": roc_auc(te_scores, te_labels)})
            for r, s in zip(te, te_scores):
                pooled_origins.append(r["origin"])
                pooled_labels.append(r["label"])
                pooled_alerts.append(int(s >= fold_cut))
                result["predictions"].append({"variant": variant, "stage": stage, "origin": r["origin"], "test_year": year,
                                              "label": r["label"], "score": s, "cutoff": fold_cut, "alert": int(s >= fold_cut)})
        pooled = confusion([float(a) for a in pooled_alerts], pooled_labels, 0.5)
        aucs = [f["auc"] for f in folds if f.get("auc") is not None]
        episodes = episode_lead_times(prices, pooled_origins, pooled_labels, pooled_alerts, horizon, rise)
        result["stages"][stage] = {
            "features": feats,
            "validation_calibration": calibration,
            "backtest": {**{k: v for k, v in pooled.items() if k != "threshold"},
                         "mean_fold_auc": sum(aucs) / len(aucs) if aucs else None, "folds_with_auc": len(aucs),
                         "episodes": len(episodes), "episodes_detected": sum(e["detected"] for e in episodes),
                         "lead_times_months": [e["lead_time_months"] for e in episodes if e["lead_time_months"] is not None],
                         "folds": folds},
        }
    backtest_rows = [r for r in rows if any(r["origin"].startswith(f"{y}-") for y in years)]
    bl = [r["label"] for r in backtest_rows]
    result["baselines_on_backtest_origins"] = {
        "always_alert": confusion([1.0] * len(bl), bl, 0.5),
        "momentum_rule": confusion([float(r["price_real_mom_change"] > 0) for r in backtest_rows], bl, 0.5),
    }
    del momentum, y_all
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week5_ablation.local.json")
    parser.add_argument("--annual-lag", type=int, default=None, help="Override the annual availability lag (D6 sensitivity).")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--rise-pct", type=float, default=None,
                        help="Label sensitivity (D1): minimum real-price rise, e.g. 5 or 15. Backtest only.")
    parser.add_argument("--horizon", type=int, default=None,
                        help="Label sensitivity (D2): forecast horizon in months, e.g. 1 or 6. Backtest only.")
    parser.add_argument("--variants", nargs="+", default=None, help="Subset of feature variants to run.")
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    week4_path = ROOT / cfg["week4_config"]
    week4 = json.loads(week4_path.read_text(encoding="utf-8"))
    lag = args.annual_lag if args.annual_lag is not None else int(cfg["annual_availability_lag_years"])
    out = args.output or (ROOT / cfg["output_dir"])
    target, split, model_config = dict(week4["target"]), week4["candidate_split"], week4["m0_model"]
    if args.rise_pct is not None:
        target["minimum_real_price_rise_pct"] = args.rise_pct
    if args.horizon is not None:
        target["forecast_horizon_months"] = args.horizon
    if args.rise_pct is not None or args.horizon is not None:
        target["label_rule_id"] = (f"realrise_{target['minimum_real_price_rise_pct']:g}pct_futuremax_"
                                   f"{target['forecast_horizon_months']}m (D1/D2 sensitivity)")
    if args.variants:
        cfg["feature_variants"] = args.variants
    horizon, rise = int(target["forecast_horizon_months"]), float(target["minimum_real_price_rise_pct"])
    last_allowed = month_key(month_number(split["test_start"]) - 1)

    prices = read_real_prices(ROOT / week4["price_csv"])
    labels = {k: future_rise_label(prices, k, horizon, rise, last_allowed) for k in prices}
    if any(v is not None and month_number(k) >= month_number(split["test_start"]) for k, v in labels.items()):
        raise RuntimeError("Holdout guard: a test-period origin was labelled")

    spark = make_spark()
    try:
        panel, _ = build_week4_selected_features(spark, ROOT, week4_path, annual_lag_override=lag)
        panel = add_annual_change_features(spark, ROOT, panel, week4["week2_config"])
        panel = add_weather_climatology_anomalies(spark, ROOT, panel, week4["weather_monthly_csv"], week4["week2_config"])
        needed = sorted({f for v in cfg["feature_variants"] for feats in FEATURE_VARIANTS[v].values() for f in feats})
        rows_by_origin = {r["origin"]: r.asDict() for r in panel.select(
            F.date_format("month", "yyyy-MM").alias("origin"), *needed).collect()}
        results = [evaluate_variant(spark, rows_by_origin, labels, prices, v, split, horizon, rise, model_config,
                                    int(cfg["backtest_first_test_year"])) for v in cfg["feature_variants"]]
    finally:
        spark.stop()

    out.mkdir(parents=True, exist_ok=True)
    predictions = [p for r in results for p in r.pop("predictions")]
    with (out / "backtest_predictions.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(predictions[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(predictions)
    table = []
    for r in results:
        for stage, s in r["stages"].items():
            b, v = s["backtest"], s["validation_calibration"]
            table.append({"variant": r["variant"], "stage": stage, "features": len(s["features"]),
                          "backtest_origins": b["n"], "backtest_stress_months": b["positives"],
                          "backtest_precision": b["precision"], "backtest_recall": b["recall"], "backtest_f1": b["f1"],
                          "backtest_false_alarm_rate": b["false_positive_rate"], "backtest_mean_fold_auc": b["mean_fold_auc"],
                          "episodes_detected": f"{b['episodes_detected']}/{b['episodes']}",
                          "validation_f1": v["f1"], "validation_auc": v["auc"]})
    with (out / "ablation_table.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(table[0]), lineterminator="\n")
        writer.writeheader()
        for row in table:
            writer.writerow({k: (f"{x:.4f}" if isinstance(x, float) else x) for k, x in row.items()})
    # Uncertainty: resample backtest months (same resample for every stage, seed fixed) and
    # report 90% intervals for F1 and for each stage's F1 minus its variant's M0 F1.
    import random
    rng = random.Random(20261107)
    uncertainty = {}
    for r in results:
        by_stage = {}
        for p in predictions:
            if p["variant"] == r["variant"]:
                by_stage.setdefault(p["stage"], {})[p["origin"]] = (p["label"], p["alert"])
        months = sorted(by_stage["M0"])
        draws = {stage: [] for stage in by_stage}
        diffs = {stage: [] for stage in by_stage}
        for _ in range(2000):
            sample = [rng.choice(months) for _ in months]
            f1s = {}
            for stage, preds in by_stage.items():
                y = [preds[m][0] for m in sample]
                a = [float(preds[m][1]) for m in sample]
                f1s[stage] = confusion(a, y, 0.5)["f1"] or 0.0
                draws[stage].append(f1s[stage])
            for stage in by_stage:
                diffs[stage].append(f1s[stage] - f1s["M0"])

        def interval(xs):
            xs = sorted(xs)
            return [round(xs[int(0.05 * len(xs))], 3), round(xs[int(0.95 * len(xs)) - 1], 3)]

        uncertainty[r["variant"]] = {stage: {"f1_90pct_interval": interval(draws[stage]),
                                             "f1_minus_M0_90pct_interval": interval(diffs[stage])} for stage in by_stage}
    report_uncertainty = {
        "method": "Bootstrap over backtest months (2,000 resamples, seed 20261107); same resampled months for every stage within a variant. An interval for F1 minus M0 that contains 0 means the difference is within noise.",
        "by_variant": uncertainty,
    }
    for variant, stages in uncertainty.items():
        print(variant, {s: v["f1_minus_M0_90pct_interval"] for s, v in stages.items()})

    report = {
        "pipeline": "week5_m0_m4_ablation",
        "uncertainty": report_uncertainty,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "annual_availability_lag_years": lag,
        "weather": week4.get("weather_monthly_csv"),
        "target": target, "split": split, "estimator": model_config,
        "holdout_guard": f"Labels require the outcome window to end by {last_allowed}; 2021-2022 is not labelled.",
        "recommended_variant": cfg["recommended_variant"],
        "variants": results,
    }
    (out / "ablation_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for row in table:
        print(row["variant"], row["stage"], row["features"], row["backtest_origins"], row["backtest_stress_months"],
              *(f"{row[k]:.2f}" if isinstance(row[k], float) else row[k] for k in
                ("backtest_precision", "backtest_recall", "backtest_f1", "backtest_false_alarm_rate", "backtest_mean_fold_auc")),
              row["episodes_detected"], *(f"{row[k]:.2f}" if isinstance(row[k], float) else row[k] for k in ("validation_f1", "validation_auc")))
    for r in results:
        print(r["variant"], "origins", r["common_origins"], r["first_origin"], r["last_origin"], "train", r["train_origins"],
              r["train_positives"], "valid", r["validation_origins"], r["validation_positives"], "years", r["backtest_years"],
              "baselines F1", round(r["baselines_on_backtest_origins"]["always_alert"]["f1"], 3),
              round(r["baselines_on_backtest_origins"]["momentum_rule"]["f1"], 3))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
