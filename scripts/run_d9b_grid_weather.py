"""D9-B: Spark aggregation of gridded daily NASA POWER weather to a wheat-weighted national series.

Input: data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.csv (from
scripts/fetch_d9b_gridded_weather.py; ~1.9 million daily cell rows) and
configs/d9b_grid_cells.json (cell -> oblast -> weight).

Steps, all in Spark DataFrames:
1. daily cell rows -> monthly per cell (sum of precipitation, mean of daily
   max/min temperature; Week 2 rules, missing values excluded);
2. monthly cells -> national monthly value, weighted by each cell's share of
   oblast wheat area (weights renormalised over cells with data);
3. compare with the 24-point series used by the models (correlation per variable).

Outputs data/processed/d9b_grid_weather/ (monthly CSV + profile JSON with row
counts, timings and the comparison). This is a data-engineering scale step and
a weather sensitivity input; the real test is already opened, so any model
re-run with this series is post-test exploration.

    python scripts/run_d9b_grid_weather.py
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, StringType, StructField, StructType  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402

DAILY_SCHEMA = StructType([
    StructField("cell_id", StringType(), False), StructField("date", StringType(), False),
    StructField("prectotcorr_mm", DoubleType(), True), StructField("t2m_max_c", DoubleType(), True),
    StructField("t2m_min_c", DoubleType(), True),
])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", type=Path, default=ROOT / "data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.csv")
    parser.add_argument("--cells", type=Path, default=ROOT / "configs/d9b_grid_cells.json")
    parser.add_argument("--compare", type=Path, default=ROOT / "data/processed/week4_ukraine_weather_wheat_area_weighted_monthly.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "data/processed/d9b_grid_weather")
    args = parser.parse_args()
    os.environ.setdefault("FOODSHIELD_DRIVER_MEMORY", "2g")
    os.environ.setdefault("FOODSHIELD_SHUFFLE_PARTITIONS", "8")
    cells = json.loads(args.cells.read_text(encoding="utf-8"))["cells"]

    spark = make_spark()
    timings = {}
    try:
        weights = spark.createDataFrame([(c["cell_id"], c["katottg"], float(c["weight"])) for c in cells],
                                        ["cell_id", "katottg", "weight"])
        t = time.perf_counter()
        daily = (spark.read.schema(DAILY_SCHEMA).option("header", "true").option("mode", "FAILFAST").csv(str(args.daily))
                 .withColumn("month", F.substring("date", 1, 7)))
        daily_rows = daily.count()
        timings["read_and_count_daily_seconds"] = round(time.perf_counter() - t, 2)

        t = time.perf_counter()
        monthly_cell = daily.groupBy("cell_id", "month").agg(
            F.sum("prectotcorr_mm").alias("precipitation_sum_mm"),
            F.avg("t2m_max_c").alias("t2m_max_mean_c"),
            F.avg("t2m_min_c").alias("t2m_min_mean_c"),
            F.count("prectotcorr_mm").alias("days"),
        ).join(F.broadcast(weights), "cell_id", "inner")
        national = monthly_cell.groupBy("month").agg(
            *[(F.sum(F.col(v) * F.col("weight")) / F.sum(F.when(F.col(v).isNotNull(), F.col("weight")))).alias(v)
              for v in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")],
            F.countDistinct("cell_id").alias("cells_present"),
        ).orderBy("month")
        result = national.collect()
        cell_months = monthly_cell.count()
        timings["aggregate_seconds"] = round(time.perf_counter() - t, 2)
    finally:
        spark.stop()

    args.output.mkdir(parents=True, exist_ok=True)
    out_csv = args.output / "nasa_power_grid_wheat_weighted_monthly.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["month", "precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c", "cells_present"])
        for r in result:
            writer.writerow([r["month"], f"{r['precipitation_sum_mm']:.4f}", f"{r['t2m_max_mean_c']:.4f}",
                             f"{r['t2m_min_mean_c']:.4f}", r["cells_present"]])

    comparison = {}
    if args.compare.exists():
        points = {row["month"]: row for row in csv.DictReader(args.compare.open(encoding="utf-8"))}
        for v in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c"):
            pairs = [(r["month"], float(points[r["month"]][v]), r[v]) for r in result if r["month"] in points]
            _, a, b = zip(*pairs)
            # Raw monthly values share the seasonal cycle, so also compare after removing
            # each series' own calendar-month mean (what the corrected M1 features use).
            by_cal = {}
            for month, x, y in pairs:
                by_cal.setdefault(month[5:7], []).append((x, y))
            cal_mean = {k: (statistics.mean(x for x, _ in v), statistics.mean(y for _, y in v)) for k, v in by_cal.items()}
            da = [x - cal_mean[m[5:7]][0] for m, x, _ in pairs]
            db = [y - cal_mean[m[5:7]][1] for m, _, y in pairs]
            comparison[v] = {"months": len(pairs), "correlation_with_24_point_series": round(statistics.correlation(a, b), 4),
                             "correlation_after_removing_calendar_month_means": round(statistics.correlation(da, db), 4),
                             "mean_absolute_difference": round(statistics.mean(abs(x - y) for x, y in zip(a, b)), 3),
                             "mean_24_point": round(statistics.mean(a), 3), "mean_grid": round(statistics.mean(b), 3)}
    profile = {"pipeline": "d9b_grid_weather", "generated_utc": datetime.now(timezone.utc).isoformat(),
               "cells": len(cells), "daily_rows": daily_rows, "cell_months": cell_months, "months": len(result),
               "timings": timings, "comparison": comparison, "output": out_csv.name}
    (args.output / "grid_weather_profile.json").write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(profile, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
