"""D9-A: process full FAOSTAT normalized bulk archives (all countries, all items) with Spark.

Week 2 used small Ukraine-wheat extracts filtered outside Spark. This module
lets Spark do the whole job at source scale:

1. stream the main ``*_All_Data_(Normalized).csv`` out of each official ZIP;
2. read it with an explicit all-string schema taken from the file header,
   clean identifiers and cast types (Bronze);
3. write Parquet partitioned by year, so later queries prune by year and read
   only the columns they need.

The archives and Parquet are local, large and Git-ignored; the profile JSON
(row counts, timings, reconciliation) is the shared evidence.
"""

from __future__ import annotations

import csv
import hashlib
import shutil
import time
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

DOMAINS = {
    "QCL": "Production_Crops_Livestock_E_All_Data_(Normalized)",
    "TCL": "Trade_CropsLivestock_E_All_Data_(Normalized)",
    "PP": "Prices_E_All_Data_(Normalized)",
    "TM": "Trade_DetailedTradeMatrix_E_All_Data_(Normalized)",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def extract_main_csv(archive: Path, member: str, target: Path) -> dict:
    """Stream the main CSV out of the ZIP (no full in-memory copy). Skips work if already extracted."""
    start = time.perf_counter()
    with zipfile.ZipFile(archive) as zf:
        info = zf.getinfo(member)
        if not (target.exists() and target.stat().st_size == info.file_size):
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(target.name + ".tmp")
            with zf.open(member) as src, tmp.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=8 << 20)
            tmp.replace(target)
    return {"archive_bytes": archive.stat().st_size, "csv_bytes": target.stat().st_size,
            "extract_seconds": round(time.perf_counter() - start, 2)}


def header_schema(csv_path: Path) -> StructType:
    with csv_path.open(encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream))
    return StructType([StructField(name, StringType(), True) for name in header])


def read_bronze(spark: SparkSession, csv_path: Path, domain: str) -> DataFrame:
    schema = header_schema(csv_path)
    names = set(schema.fieldNames())
    raw = (spark.read.schema(schema).option("header", "true").option("mode", "FAILFAST")
           .option("encoding", "UTF-8").csv(str(csv_path)))
    strip_quote = lambda c: F.regexp_replace(F.trim(F.col(f"`{c}`")), "^'", "")
    cols = [
        F.lit(domain).alias("domain"),
        F.col("`Area Code`").cast("int").alias("area_code"),
        strip_quote("Area Code (M49)").alias("area_m49"),
        F.col("Area").alias("area"),
        F.col("`Item Code`").cast("int").alias("item_code"),
        strip_quote("Item Code (CPC)").alias("item_cpc"),
        F.col("Item").alias("item"),
        F.col("`Element Code`").cast("int").alias("element_code"),
        F.col("Element").alias("element"),
        F.col("Year").cast("int").alias("year"),
        F.col("Unit").alias("unit"),
        F.col("Value").cast("double").alias("value"),
        F.col("Flag").alias("flag"),
    ]
    if "Months Code" in names:
        cols += [F.col("`Months Code`").cast("int").alias("months_code"), F.col("Months").alias("months")]
    if "Note" in names:
        cols.append(F.col("Note").alias("note"))
    if "Reporter Country Code (M49)" in names:  # detailed trade matrix
        cols = [
            F.lit(domain).alias("domain"),
            strip_quote("Reporter Country Code (M49)").alias("reporter_m49"),
            F.col("`Reporter Countries`").alias("reporter"),
            strip_quote("Partner Country Code (M49)").alias("partner_m49"),
            F.col("`Partner Countries`").alias("partner"),
            F.col("`Item Code`").cast("int").alias("item_code"),
            F.col("Item").alias("item"),
            F.col("`Element Code`").cast("int").alias("element_code"),
            F.col("Element").alias("element"),
            F.col("Year").cast("int").alias("year"),
            F.col("Unit").alias("unit"),
            F.col("Value").cast("double").alias("value"),
            F.col("Flag").alias("flag"),
        ]
    return raw.select(*cols)


def write_parquet(df: DataFrame, target: Path) -> dict:
    start = time.perf_counter()
    df.repartition("year").write.mode("overwrite").partitionBy("year").parquet(str(target))
    seconds = time.perf_counter() - start
    size = sum(p.stat().st_size for p in target.rglob("*.parquet"))
    return {"write_parquet_seconds": round(seconds, 2), "parquet_bytes": size,
            "year_partitions": sum(1 for p in target.iterdir() if p.is_dir() and p.name.startswith("year="))}
