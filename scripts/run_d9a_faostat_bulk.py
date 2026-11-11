"""D9-A: full FAOSTAT bulk archives through Spark, with timings and reconciliation.

Put the official archives (names as published by FAO) in ``--bulk-dir``:

    Production_Crops_Livestock_E_All_Data_(Normalized).zip   (QCL, ~33 MB)
    Trade_CropsLivestock_E_All_Data_(Normalized).zip          (TCL, ~267 MB)
    Prices_E_All_Data_(Normalized).zip                        (PP,  ~11 MB)
    Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip    (TM, ~411 MB, optional)

Then run:

    python scripts/run_d9a_faostat_bulk.py

Outputs:
- data/interim/faostat_bulk/      extracted CSV and year-partitioned Parquet (large, Git-ignored)
- data/processed/d9a_faostat_bulk/bulk_profile.json   counts, timings, reconciliation (shared)

Reconciliation: Ukraine (M49 804) wheat rows are filtered from the global
Parquet and compared value by value with the committed Week 2 extracts, so the
at-scale path is shown to reproduce the inputs the models use.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from foodshield.etl.bulk import DOMAINS, extract_main_csv, read_bronze, sha256_file, write_parquet  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402

EXTRACTS = {
    "QCL": "data/raw/faostat_ukraine_wheat_production_2026-11.csv",
    "TCL": "data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv",
    "PP": "data/raw/faostat_ukraine_monthly_wheat_producer_prices_2026-11.csv",
}


def timed(fn):
    start = time.perf_counter()
    value = fn()
    return value, round(time.perf_counter() - start, 2)


def reconcile(spark, parquet_dir: Path, domain: str, extract_path: Path) -> dict:
    with extract_path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    key = (lambda r: (r["Element"], int(r["Year"]), int(r.get("Months Code") or 0)))
    expected = {key(r): float(r["Value"]) for r in rows if r["Value"] not in ("", None)}
    elements = sorted({r["Element"] for r in rows})
    df = spark.read.parquet(str(parquet_dir)).filter(
        (F.col("area_m49") == "804") & (F.col("item_code") == 15) & F.col("element").isin(elements))
    cols = ["element", "year", F.coalesce(F.col("months_code"), F.lit(0)).alias("m") if "months_code" in df.columns
            else F.lit(0).alias("m"), "value"]
    got = {(r["element"], r["year"], r["m"]): r["value"] for r in df.select(*cols).collect() if r["value"] is not None}
    common = set(expected) & set(got)
    mismatched = [k for k in common if abs(expected[k] - got[k]) > 1e-6 * max(1.0, abs(expected[k]))]
    return {"extract_rows": len(expected), "bulk_rows": len(got), "matched_keys": len(common),
            "value_mismatches": len(mismatched), "only_in_extract": len(set(expected) - set(got)),
            "only_in_bulk": len(set(got) - set(expected)),
            "status": "exact match" if not mismatched and len(common) == len(expected) else "differences found"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bulk-dir", type=Path, default=ROOT / "data" / "raw" / "faostat_bulk")
    parser.add_argument("--work-dir", type=Path, default=ROOT / "data" / "interim" / "faostat_bulk")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "processed" / "d9a_faostat_bulk")
    parser.add_argument("--driver-memory", default="3g")
    args = parser.parse_args()
    os.environ["FOODSHIELD_DRIVER_MEMORY"] = args.driver_memory
    os.environ.setdefault("FOODSHIELD_SHUFFLE_PARTITIONS", "16")

    found = {code: args.bulk_dir / f"{stem}.zip" for code, stem in DOMAINS.items() if (args.bulk_dir / f"{stem}.zip").exists()}
    if not found:
        sys.exit(f"No FAOSTAT archives found in {args.bulk_dir}")

    profile = {"pipeline": "d9a_faostat_bulk", "generated_utc": datetime.now(timezone.utc).isoformat(),
               "machine": {"os": platform.platform(), "cpu_count": os.cpu_count(), "python": platform.python_version()},
               "spark": {"master": "local[2]", "driver_memory": args.driver_memory}, "domains": {}}
    spark = make_spark()
    try:
        profile["spark"]["version"] = spark.version
        for code, archive in found.items():
            stem = DOMAINS[code]
            entry = {"archive": archive.name, "archive_sha256": sha256_file(archive)}
            csv_path = args.work_dir / "csv" / f"{stem}.csv"
            entry.update(extract_main_csv(archive, f"{stem}.csv", csv_path))
            bronze = read_bronze(spark, csv_path, code)
            entry["csv_rows"], entry["count_csv_seconds"] = timed(lambda: bronze.count())
            parquet_dir = args.work_dir / "parquet" / code
            entry.update(write_parquet(bronze, parquet_dir))
            pq = spark.read.parquet(str(parquet_dir))
            entry["parquet_rows"], entry["count_parquet_seconds"] = timed(lambda: pq.count())
            entry["years"] = list(pq.agg(F.min("year"), F.max("year")).collect()[0])
            if code != "TM":
                entry["areas"] = pq.select("area_code").distinct().count()
                entry["items"] = pq.select("item_code").distinct().count()
                # Same question on raw CSV vs Parquet: all-country wheat rows 2010-2022.
                query = lambda df: df.filter((F.col("item_code") == 15) & F.col("year").between(2010, 2022)).count()
                entry["query_wheat_2010_2022_rows"], entry["query_csv_seconds"] = timed(lambda: query(read_bronze(spark, csv_path, code)))
                _, entry["query_parquet_seconds"] = timed(lambda: query(pq))
            if code in EXTRACTS and (ROOT / EXTRACTS[code]).exists():
                entry["ukraine_wheat_reconciliation"] = reconcile(spark, parquet_dir, code, ROOT / EXTRACTS[code])
            if code == "PP":
                monthly = (pq.filter((F.col("item_code") == 15) & (F.col("months_code") >= 7001) & (F.col("months_code") <= 7012)
                                     & F.col("year").between(2010, 2022) & F.col("element").contains("LCU"))
                           .groupBy("area_m49", "area").agg(F.count("*").alias("months")))
                coverage = [r.asDict() for r in monthly.filter(F.col("months") >= 120).orderBy(F.desc("months")).collect()]
                entry["d9d_scoping_countries_with_120plus_monthly_wheat_prices_2010_2022"] = {
                    "count": len(coverage), "countries": [f"{c['area']} ({c['months']})" for c in coverage]}
            profile["domains"][code] = entry
            print(code, {k: v for k, v in entry.items() if k not in ("d9d_scoping_countries_with_120plus_monthly_wheat_prices_2010_2022",)})
    finally:
        spark.stop()

    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "bulk_profile.json").write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output / 'bulk_profile.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
