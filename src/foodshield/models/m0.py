"""Price-only anomaly detector for the provisional Week 4 target."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


M0_FEATURES = ["price_real_mom_change", "price_real_anomaly_z_12m"]


def _f1_counts(rows: list[tuple[float, int]], threshold: float) -> dict[str, float | int]:
    tp = sum(1 for score, label in rows if score >= threshold and label == 1)
    fp = sum(1 for score, label in rows if score >= threshold and label == 0)
    fn = sum(1 for score, label in rows if score < threshold and label == 1)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "threshold": threshold,
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def fit_m0_price_only(
    spark: SparkSession,
    features: DataFrame,
    root: Path,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Calibrate a transparent M0 score on train and its alert cutoff on validation.

    The function never loads or scores final-test labels. M4 eligibility is
    applied first so later M0–M4 comparisons can use identical origins.
    """
    labels_path = root / config["label_csv"]
    eligibility_path = root / config["eligibility_csv"]
    output_root = root / config["m0_output_root"]
    target = config["target"]
    split = config["candidate_split"]

    labels = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(str(labels_path))
        .filter(F.col("label_rule_id") == target["label_rule_id"])
        .select(
            F.to_date(F.concat_ws("-", F.col("origin_month"), F.lit("01"))).alias("month"),
            F.col("split").alias("split"),
            F.col("future_stress_label").cast("int").alias("label"),
        )
    )
    eligible = (
        spark.read.option("header", "true").option("inferSchema", "false").csv(str(eligibility_path))
        .filter(F.col("M4_features_complete") == "1")
        .select(F.to_date(F.concat_ws("-", F.col("month"), F.lit("01"))).alias("month"))
        .dropDuplicates(["month"])
    )
    required = ["month", *M0_FEATURES]
    absent = sorted(set(required) - set(features.columns))
    if absent:
        raise ValueError(f"Week 4 feature panel is missing M0 columns: {absent}")
    samples = (
        features.join(eligible, "month", "inner")
        .join(labels, "month", "inner")
        .filter(F.col("split").isin("train", "validation"))
        .filter(F.col("label").isNotNull())
        .filter(F.expr(" AND ".join(f"`{name}` IS NOT NULL" for name in M0_FEATURES)))
        .select("month", "split", "label", *M0_FEATURES)
    )

    training = samples.filter(F.col("split") == "train")
    validation = samples.filter(F.col("split") == "validation")
    training_count, validation_count = training.count(), validation.count()
    if training_count == 0 or validation_count == 0:
        raise ValueError(f"M0 requires non-empty train and validation samples; got {training_count}, {validation_count}")
    train_classes = {int(row["label"]): row["count"] for row in training.groupBy("label").count().collect()}
    if set(train_classes) != {0, 1}:
        raise ValueError(f"M0 training sample needs both target classes; found {train_classes}")

    # Use only the training period to scale the current-month change. The
    # anomaly already compares the current real price with its prior 12 months.
    scale = training.agg(
        F.avg("price_real_mom_change").alias("mean"),
        F.stddev_samp("price_real_mom_change").alias("sd"),
    ).first()
    mom_mean, mom_sd = float(scale["mean"]), float(scale["sd"] or 0.0)
    if mom_sd <= 0:
        raise ValueError("M0 training month-over-month change has zero or undefined standard deviation")

    scored = samples.withColumn(
        "m0_score",
        F.col("price_real_anomaly_z_12m")
        + (F.col("price_real_mom_change") - F.lit(mom_mean)) / F.lit(mom_sd),
    )
    validation_scores = scored.filter(F.col("split") == "validation").select("m0_score", "label").collect()
    pairs = [(float(row["m0_score"]), int(row["label"])) for row in validation_scores]
    threshold_candidates = sorted({0.0, 1.0, *(score for score, _ in pairs)})
    threshold_results = [_f1_counts(pairs, threshold) for threshold in threshold_candidates]
    best_f1 = max(result["f1"] for result in threshold_results)
    chosen = max((result for result in threshold_results if result["f1"] == best_f1), key=lambda row: row["threshold"])

    predictions = (
        scored.withColumn("selected_alert_threshold", F.lit(float(chosen["threshold"])))
        .withColumn("m0_alert", (F.col("m0_score") >= F.lit(float(chosen["threshold"]))).cast("integer"))
        .select("month", "split", "label", *M0_FEATURES, "m0_score", "selected_alert_threshold", "m0_alert")
    )

    output_root.mkdir(parents=True, exist_ok=True)
    predictions.write.mode("overwrite").parquet(str(output_root / "train_validation_predictions"))
    class_counts = {
        label_split: {str(int(row["label"])): row["count"] for row in frame.groupBy("label").count().collect()}
        for label_split, frame in (("train", training), ("validation", validation))
    }
    report = {
        "pipeline": "week4_m0_price_only_anomaly_detector",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "working candidate; target and candidate split marked review needed",
        "target": target,
        "candidate_split": split,
        "features": M0_FEATURES,
        "score_definition": "price_real_anomaly_z_12m + train-standardized price_real_mom_change",
        "training_mom_change_mean": mom_mean,
        "training_mom_change_sample_sd": mom_sd,
        "training_origins": training_count,
        "validation_origins": validation_count,
        "class_counts": class_counts,
        "validation_alert_threshold_selection": {
            "metric": "F1",
            "threshold": chosen["threshold"],
            "calibration_counts": chosen,
            "candidate_threshold_count": len(threshold_candidates),
            "caveat": "These validation figures are used to select the alert threshold and are not an unbiased final performance estimate.",
        },
        "holdout": {
            "test_labels_loaded": False,
            "test_predictions_generated": False,
            "test_window": [split["test_start"], "2022-12"],
        },
        "limits": [
            "Small training and validation samples; treat estimates as exploratory.",
            "Outcome is measured from the same FAOSTAT producer-price source as the M0 price history, though strictly after each origin.",
            "The input weather signal is a single-point screening series; M0 itself uses no weather, production, or trade features.",
            "This is a transparent anomaly-detector baseline, not evidence of superiority over a multi-signal model.",
        ],
        "predictions_path": str((output_root / "train_validation_predictions").relative_to(root)).replace("\\", "/"),
    }
    (output_root / "m0_profile.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report
