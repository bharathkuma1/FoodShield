"""Rolling-origin (expanding-window) backtest of the M0 price-only baseline.

Uses only origins whose full outcome window ends before the candidate test
period (2021-01 onward), so the held-out 2021-2022 labels stay unopened.

For each test year Y the model is refit on every eligible origin whose
3-month outcome window ends by December of Y-1 (a horizon-length purge), the
alert cutoff is chosen by F1 on those training origins, and the year-Y origins
are scored out of sample. Same Spark MLlib estimator and settings as
models/m0.py. Results across folds are pooled to give a far larger test
sample than the nine 2022 M4 origins alone.

    python scripts/run_week4_rolling_origin.py --config configs/week4_v2.local.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.evaluation.week4 import (  # noqa: E402
    best_f1_threshold, confusion, episode_lead_times, future_rise_label, month_key, month_number,
    read_eligible, read_real_prices, roc_auc,
)
from foodshield.models.m0 import M0_FEATURES  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import LogisticRegression  # noqa: E402
from pyspark.ml.feature import VectorAssembler  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def make_spark() -> SparkSession:
    hadoop_home = os.environ.get("HADOOP_HOME")
    if os.name == "nt" and hadoop_home:
        os.environ["PATH"] = f"{Path(hadoop_home, 'bin')}{os.pathsep}{os.environ.get('PATH', '')}"
    temp_root = ROOT / "data" / "processed" / ".spark-week4-tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(temp_root)
    os.environ["TEMP"] = os.environ["TMP"] = str(temp_root)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("FoodShield-Week4-RollingOrigin")
        .master("local[2]")
        .config("spark.driver.memory", os.environ.get("FOODSHIELD_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("FOODSHIELD_SHUFFLE_PARTITIONS", "4"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week4_v2.local.json")
    parser.add_argument("--eligibility-column", default="M4_features_complete",
                        help="Origin set; M4_features_complete keeps the M0-M4 common-origin contract.")
    parser.add_argument("--first-test-year", type=int, default=2014)
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "processed" / "week4_rolling_origin")
    args = parser.parse_args()
    config = json.loads(args.config.resolve().read_text(encoding="utf-8"))
    target, split, model_config = config["target"], config["candidate_split"], config["m0_model"]
    horizon, rise = int(target["forecast_horizon_months"]), float(target["minimum_real_price_rise_pct"])
    last_allowed = month_key(month_number(split["test_start"]) - 1)  # outcome window must end before test period

    prices = read_real_prices(ROOT / config["price_csv"])
    eligible = read_eligible(ROOT / config["eligibility_csv"], args.eligibility_column)
    labels = {o: future_rise_label(prices, o, horizon, rise, last_allowed) for o in eligible}
    labelled = [o for o in eligible if labels[o] is not None]
    if any(month_number(o) >= month_number(split["test_start"]) for o in labelled):
        raise RuntimeError("Holdout guard: a test-period origin was labelled")

    spark = make_spark()
    try:
        features = spark.read.parquet(str(ROOT / config["output_root"] / "features"))
        rows = (features.select(F.date_format("month", "yyyy-MM").alias("origin"), *M0_FEATURES)
                .filter(F.col("origin").isin(labelled)).collect())
        feature_rows = {r["origin"]: [float(r[f]) for f in M0_FEATURES] for r in rows
                        if all(r[f] is not None for f in M0_FEATURES)}
        missing = sorted(set(labelled) - set(feature_rows))
        if missing:
            raise ValueError(f"Eligible origins lack M0 features: {missing[:5]}")

        last_year = int(last_allowed[:4])
        fold_results, predictions = [], []
        for year in range(args.first_test_year, last_year + 1):
            train = [o for o in labelled if month_number(o) + horizon <= month_number(f"{year - 1}-12")]
            test = [o for o in labelled if o.startswith(f"{year}-")]
            if not test:
                fold_results.append({"test_year": year, "skipped": "no eligible origins in this year"})
                continue
            y_train = [labels[o] for o in train]
            if len(set(y_train)) < 2:
                fold_results.append({"test_year": year, "skipped": "training origins have one class only"})
                continue
            frame = spark.createDataFrame(
                [(o, float(labels[o]), *feature_rows[o]) for o in train + test],
                ["origin", "label", *M0_FEATURES],
            )
            fitted = Pipeline(stages=[
                VectorAssembler(inputCols=M0_FEATURES, outputCol="x", handleInvalid="error"),
                LogisticRegression(featuresCol="x", labelCol="label", family="binomial",
                                   regParam=float(model_config["regularization_l2"]), elasticNetParam=0.0,
                                   maxIter=int(model_config["max_iterations"]), standardization=True),
            ]).fit(frame.filter(F.col("origin").isin(train)))
            scored = {r["origin"]: float(r["score"]) for r in fitted.transform(frame)
                      .select("origin", vector_to_array("probability")[1].alias("score")).collect()}
            train_scores = [scored[o] for o in train]
            cutoff = best_f1_threshold(train_scores, y_train)
            test_scores = [scored[o] for o in test]
            y_test = [labels[o] for o in test]
            lr = fitted.stages[-1]
            fold_results.append({
                "test_year": year, "train_origins": len(train), "train_positives": sum(y_train),
                "train_window": [train[0], train[-1]], "test_window": [test[0], test[-1]],
                "coefficients": dict(zip(M0_FEATURES, map(float, lr.coefficients))), "intercept": float(lr.intercept),
                "alert_cutoff_from_training": cutoff,
                "test": confusion(test_scores, y_test, cutoff), "test_auc": roc_auc(test_scores, y_test),
            })
            for o, s, y in zip(test, test_scores, y_test):
                predictions.append({"origin": o, "test_year": year, "label": y, "m0_score": s, "alert_cutoff": cutoff,
                                    "m0_alert": int(s >= cutoff), "price_real_mom_change": feature_rows[o][0],
                                    "price_real_anomaly_z_12m": feature_rows[o][1],
                                    "momentum_alert": int(feature_rows[o][0] > 0)})
    finally:
        spark.stop()

    origins = [p["origin"] for p in predictions]
    y = [p["label"] for p in predictions]
    s = [p["m0_score"] for p in predictions]
    alerts = [p["m0_alert"] for p in predictions]
    pooled = confusion([float(a) for a in alerts], y, 0.5)
    pooled["threshold"] = "per-fold cutoff chosen on that fold's training origins"
    baselines = {
        "always_alert": confusion([1.0] * len(y), y, 0.5),
        "momentum_rule_alert_if_real_price_rose_last_month": confusion(
            [float(p["momentum_alert"]) for p in predictions], y, 0.5),
    }
    lead = episode_lead_times(prices, origins, y, alerts, horizon, rise)

    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "rolling_origin_predictions.csv").open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(predictions[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(predictions)
    report = {
        "pipeline": "week4_m0_rolling_origin_backtest",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "Real-data evidence on pre-test origins only; target, split and model settings remain review needed.",
        "config": str(args.config.resolve().relative_to(ROOT)).replace("\\", "/"),
        "origin_set": args.eligibility_column,
        "target": target,
        "holdout_guard": f"Labels require the full outcome window to end by {last_allowed}; no 2021-2022 outcome is used.",
        "design": "Expanding window. Train on origins whose outcome window ends by December of the year before the test year; choose the alert cutoff by F1 on the training origins; score the test-year origins once.",
        "estimator": "Spark MLlib binomial LogisticRegression, standardization on, L2 regParam "
                     f"{model_config['regularization_l2']}, maxIter {model_config['max_iterations']} (same as M0).",
        "folds": fold_results,
        "pooled_out_of_sample": {**pooled, "origins": len(y),
                                 "test_years": sorted({p["test_year"] for p in predictions}),
                                 "mean_fold_auc": (lambda a: sum(a) / len(a) if a else None)(
                                     [f["test_auc"] for f in fold_results if f.get("test_auc") is not None]),
                                 "folds_with_auc": sum(1 for f in fold_results if f.get("test_auc") is not None),
                                 "pooled_score_auc_not_comparable": roc_auc(s, y),
                                 "auc_note": "Use mean_fold_auc. Each fold has its own refitted model, so raw scores are not on a common scale across folds and the pooled-score AUC understates within-year ranking."},
        "baselines_on_same_origins": baselines,
        "episodes": lead,
        "episodes_detected": f"{sum(e['detected'] for e in lead)} of {len(lead)}",
        "limits": [
            "Small sample: each fold has at most 11 origins and some test years have no or few stress months.",
            "The alert cutoff is chosen on training origins, so it is not tuned to the test year.",
            "Outcome uses the same FAOSTAT price series as the M0 inputs, strictly after each origin.",
        ],
    }
    (args.output / "rolling_origin_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("pooled_out_of_sample", "baselines_on_same_origins", "episodes_detected")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
