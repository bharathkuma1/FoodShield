"""Run the bounded Week 2 Spark ETL using a checked-in local JSON config."""

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

from foodshield.etl.week2 import run_week2  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "configs" / "week2.local.json",
        help="Path to a local JSON config (default: configs/week2.local.json)",
    )
    args = parser.parse_args()
    config_path = args.config.resolve()
    if not config_path.is_file():
        parser.error(f"Config not found: {config_path}")
    hadoop_home = os.environ.get("HADOOP_HOME")
    hadoop_bin = Path(hadoop_home, "bin") if hadoop_home else None
    winutils = hadoop_bin / "winutils.exe" if hadoop_bin else None
    hadoop_dll = hadoop_bin / "hadoop.dll" if hadoop_bin else None
    if os.name == "nt" and not (
        winutils and winutils.is_file() and hadoop_dll and hadoop_dll.is_file()
    ):
        parser.error(
            "On Windows, set HADOOP_HOME to a trusted Hadoop build matching "
            "PySpark's Hadoop client; its bin folder must contain winutils.exe "
            "and hadoop.dll."
        )
    if os.name == "nt" and hadoop_bin:
        os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{os.environ.get('PATH', '')}"

    prior_temp = os.environ.get("TEMP")
    prior_tmp = os.environ.get("TMP")
    prior_tempdir = tempfile.tempdir
    short_temp = ROOT / "data" / "processed" / ".spark-tmp"
    shutil.rmtree(short_temp, ignore_errors=True)
    short_temp.mkdir(parents=True, exist_ok=True)

    # The short path avoids Windows AF_UNIX path-length failures in JDK selectors.
    tempfile.tempdir = str(short_temp)
    os.environ["TEMP"] = str(short_temp)
    os.environ["TMP"] = str(short_temp)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("FoodShield-Week2-ETL")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        report = run_week2(spark, ROOT, config_path)
        print(json.dumps({"output_root": report["output_root"], "counts": report["counts"]}, indent=2))
    finally:
        if spark is not None:
            spark.stop()
        if prior_temp is None:
            os.environ.pop("TEMP", None)
        else:
            os.environ["TEMP"] = prior_temp
        if prior_tmp is None:
            os.environ.pop("TMP", None)
        else:
            os.environ["TMP"] = prior_tmp
        tempfile.tempdir = prior_tempdir
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
