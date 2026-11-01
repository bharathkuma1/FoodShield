"""Run the bounded Week 3 Spark feature build."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.features.week3 import run_week3  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week3.local.json")
    args = parser.parse_args()
    config_path = args.config.resolve()
    if not config_path.is_file():
        parser.error(f"Config not found: {config_path}")
    hadoop_home = os.environ.get("HADOOP_HOME")
    hadoop_bin = Path(hadoop_home, "bin") if hadoop_home else None
    if os.name == "nt" and not (hadoop_bin and (hadoop_bin / "winutils.exe").is_file() and (hadoop_bin / "hadoop.dll").is_file()):
        parser.error("On Windows, set HADOOP_HOME to a compatible Hadoop native home with winutils.exe and hadoop.dll.")
    if os.name == "nt" and hadoop_bin:
        os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    prior_temp, prior_tmp, prior_tempdir = os.environ.get("TEMP"), os.environ.get("TMP"), tempfile.tempdir
    short_temp = ROOT / "data" / "processed" / ".spark-tmp"
    shutil.rmtree(short_temp, ignore_errors=True)
    short_temp.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(short_temp)
    os.environ["TEMP"] = os.environ["TMP"] = str(short_temp)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = None
    try:
        spark = (SparkSession.builder.appName("FoodShield-Week3-Features").master("local[2]")
                 .config("spark.driver.memory", "1g").config("spark.sql.shuffle.partitions", "4")
                 .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        profile = run_week3(spark, ROOT, config_path)
        print(json.dumps({"output_path": profile["output_path"], "row_count": profile["row_count"], "stages": profile["stages"]}, indent=2))
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
