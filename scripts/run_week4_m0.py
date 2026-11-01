"""Run the bounded selected-source Week 4 Spark features and M0 baseline."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.features.week4 import build_week4_selected_features  # noqa: E402
from foodshield.models.m0 import fit_m0_price_only  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week4.local.json")
    args = parser.parse_args()
    config_path = args.config.resolve()
    if not config_path.is_file():
        parser.error(f"Config not found: {config_path}")
    config = json.loads(config_path.read_text(encoding="utf-8"))

    hadoop_home = os.environ.get("HADOOP_HOME")
    hadoop_bin = Path(hadoop_home, "bin") if hadoop_home else None
    if os.name == "nt" and not (
        hadoop_bin and (hadoop_bin / "winutils.exe").is_file() and (hadoop_bin / "hadoop.dll").is_file()
    ):
        parser.error("On Windows, set HADOOP_HOME to a compatible Hadoop native home with winutils.exe and hadoop.dll.")
    if os.name == "nt" and hadoop_bin:
        os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{os.environ.get('PATH', '')}"

    temp_root = ROOT / "data" / "processed" / ".spark-week4-tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    prior_temp, prior_tmp, prior_tempdir = os.environ.get("TEMP"), os.environ.get("TMP"), tempfile.tempdir
    tempfile.tempdir = str(temp_root)
    os.environ["TEMP"] = os.environ["TMP"] = str(temp_root)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("FoodShield-Week4-M0")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        features, profile = build_week4_selected_features(spark, ROOT, config_path)
        feature_root = ROOT / config["output_root"]
        feature_root.mkdir(parents=True, exist_ok=True)
        features.write.mode("overwrite").parquet(str(feature_root / "features"))
        profile["input_path"] = config["price_csv"]
        profile["output_path"] = str((feature_root / "features").relative_to(ROOT)).replace("\\", "/")
        profile["target_review_needed"] = bool(config["target"].get("review_needed", True))
        (feature_root / "profile.json").write_text(json.dumps(profile, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        model_report = fit_m0_price_only(spark, features, ROOT, config)
        print(json.dumps({
            "feature_output": profile["output_path"],
            "feature_row_count": profile["row_count"],
            "feature_stages": profile["stages"],
            "m0_report": model_report,
        }, indent=2, ensure_ascii=False))
    finally:
        if spark is not None:
            spark.stop()
        for key, value in (("TEMP", prior_temp), ("TMP", prior_tmp)):
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        tempfile.tempdir = prior_tempdir
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
