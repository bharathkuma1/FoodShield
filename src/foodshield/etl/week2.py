"""Bounded Spark ETL for the Week 2 Ukraine wheat pilot.

Raw files remain untouched under data/raw. This pipeline writes source-specific
bronze, typed silver, and an M0 monthly-price gold table under data/processed.
The NASA file is a single-point screening sample and is labeled accordingly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


FAOSTAT_COLUMNS = [
    "Area Code",
    "Area Code (M49)",
    "Area",
    "Item Code",
    "Item Code (CPC)",
    "Item",
    "Element Code",
    "Element",
    "Year Code",
    "Year",
    "Unit",
    "Value",
    "Flag",
    "Note",
]

FAOSTAT_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in FAOSTAT_COLUMNS]
)

FPMA_DATAPOINT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("price_value", DoubleType(), True),
        StructField("price_value_real", DoubleType(), True),
        StructField("price_value_dollar", DoubleType(), True),
        StructField("conversion_factor", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("periodicity", StringType(), True),
        StructField("comment", StringType(), True),
    ]
)

FPMA_SCHEMA = StructType(
    [
        StructField("count", IntegerType(), True),
        StructField("next", StringType(), True),
        StructField("previous", StringType(), True),
        StructField(
            "results",
            ArrayType(
                StructType(
                    [
                        StructField("uuid", StringType(), True),
                        StructField("datapoints", ArrayType(FPMA_DATAPOINT), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

NASA_SCHEMA = StructType(
    [
        StructField(
            "geometry",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(DoubleType()), True),
                ]
            ),
            True,
        ),
        StructField(
            "properties",
            StructType(
                [
                    StructField(
                        "parameter",
                        StructType(
                            [
                                StructField("PRECTOTCORR", MapType(StringType(), DoubleType()), True),
                                StructField("T2M_MAX", MapType(StringType(), DoubleType()), True),
                                StructField("T2M_MIN", MapType(StringType(), DoubleType()), True),
                            ]
                        ),
                        True,
                    )
                ]
            ),
            True,
        ),
        StructField(
            "header",
            StructType(
                [
                    StructField("title", StringType(), True),
                    StructField(
                        "api",
                        StructType(
                            [
                                StructField("version", StringType(), True),
                                StructField("name", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("fill_value", DoubleType(), True),
                    StructField("time_standard", StringType(), True),
                    StructField("start", StringType(), True),
                    StructField("end", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


def _source_path(root: Path, config: dict[str, Any], key: str) -> str:
    return str(root / config["inputs"][key])


def read_fpma_bronze(spark: SparkSession, root: Path, config: dict[str, Any]) -> DataFrame:
    source = _source_path(root, config, "fpma_price_json")
    raw = (
        spark.read.schema(FPMA_SCHEMA)
        .option("multiLine", "true")
        .json(source)
    )
    series = raw.select(F.explode("results").alias("series"))
    points = series.select(
        F.col("series.uuid").alias("series_uuid"),
        F.explode("series.datapoints").alias("point"),
    )
    return points.select(
        F.lit(config["country_code"]).alias("country_code"),
        F.lit(config["country_name"]).alias("country_name"),
        F.lit(config["commodity_code"]).alias("commodity_code"),
        F.lit(config["commodity_name"]).alias("commodity_name"),
        "series_uuid",
        F.col("point.id").cast(LongType()).alias("source_record_id"),
        F.to_date("point.date", "yyyy-MM-dd").alias("price_date"),
        F.lower("point.periodicity").alias("frequency"),
        F.col("point.price_value").cast(DoubleType()).alias("price_nominal"),
        F.col("point.price_value_real").cast(DoubleType()).alias("price_real"),
        F.col("point.price_value_dollar").cast(DoubleType()).alias("price_usd"),
        F.col("point.conversion_factor").cast(DoubleType()).alias("conversion_factor"),
        F.lit(config["price_currency"]).alias("currency"),
        F.lit(config["price_unit"]).alias("unit"),
        F.col("point.comment").alias("source_comment"),
        F.lit("fpma_price_json").alias("source_file_key"),
    )


def read_faostat_bronze(
    spark: SparkSession, root: Path, config: dict[str, Any], input_key: str
) -> DataFrame:
    source = _source_path(root, config, input_key)
    raw = (
        spark.read.schema(FAOSTAT_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .csv(source)
    )
    area_m49 = F.regexp_replace(F.trim(F.col("`Area Code (M49)`")), "^'", "")
    item_code = F.regexp_replace(F.trim(F.col("`Item Code`")), "^'", "")
    return (
        raw.withColumn("area_code_m49", area_m49)
        .withColumn("item_code_clean", item_code)
        .filter(
            (F.col("area_code_m49") == F.lit(str(config["faostat_area_m49"])))
            & (F.col("item_code_clean") == F.lit(str(config["commodity_code"])))
        )
        .select(
            F.lit(config["country_code"]).alias("country_code"),
            F.col("area_code_m49").alias("area_m49"),
            F.col("Area").alias("country_name"),
            F.col("item_code_clean").alias("commodity_code"),
            F.lower(F.col("Item")).alias("commodity_name"),
            F.col("Element Code").cast(IntegerType()).alias("element_code"),
            F.col("Element").alias("element"),
            F.col("Year").cast(IntegerType()).alias("data_year"),
            F.col("Unit").alias("unit"),
            F.col("Value").cast("decimal(24,6)").alias("value"),
            F.col("Flag").alias("flag"),
            F.col("Note").alias("note"),
            F.lit(input_key).alias("source_file_key"),
        )
    )


def silver_annual(df: DataFrame, lag_years: int) -> DataFrame:
    return df.withColumn(
        "feature_available_from",
        F.make_date(F.col("data_year") + F.lit(lag_years), F.lit(1), F.lit(1)),
    ).withColumn("availability_lag_years", F.lit(lag_years).cast(IntegerType()))


def read_nasa_bronze(spark: SparkSession, root: Path, config: dict[str, Any]) -> DataFrame:
    source = _source_path(root, config, "nasa_power_json")
    feature = (
        spark.read.schema(NASA_SCHEMA)
        .option("multiLine", "true")
        .json(source)
    )
    rows: list[DataFrame] = []
    units = {"PRECTOTCORR": "mm/day", "T2M_MAX": "C", "T2M_MIN": "C"}
    for parameter, unit in units.items():
        rows.append(
            feature.select(
                "geometry",
                F.explode(F.map_entries(F.col(f"properties.parameter.{parameter}"))).alias("entry"),
            ).select(
                F.lit(config["country_code"]).alias("country_code"),
                F.lit(config["commodity_code"]).alias("commodity_code"),
                F.col("geometry.coordinates").getItem(0).alias("longitude"),
                F.col("geometry.coordinates").getItem(1).alias("latitude"),
                F.to_date(F.col("entry.key"), "yyyyMMdd").alias("observation_date"),
                F.lit(parameter).alias("parameter"),
                F.col("entry.value").cast(DoubleType()).alias("value"),
                F.lit(unit).alias("unit"),
                F.lit("single_point_screening").alias("spatial_basis"),
                F.lit("nasa_power_json").alias("source_file_key"),
            )
        )
    return rows[0].unionByName(rows[1]).unionByName(rows[2])


def silver_weather_monthly(daily: DataFrame) -> DataFrame:
    month = F.trunc(F.col("observation_date"), "month").cast("date")
    return (
        daily.withColumn("month", month)
        .groupBy("country_code", "commodity_code", "month", "latitude", "longitude", "spatial_basis")
        .agg(
            F.avg(F.when(F.col("parameter") == "T2M_MAX", F.col("value"))).alias("t2m_max_mean_c"),
            F.avg(F.when(F.col("parameter") == "T2M_MIN", F.col("value"))).alias("t2m_min_mean_c"),
            F.sum(F.when(F.col("parameter") == "PRECTOTCORR", F.col("value"))).alias("precipitation_sum_mm"),
            F.countDistinct(F.when(F.col("parameter") == "PRECTOTCORR", F.col("observation_date"))).alias("precipitation_days_present"),
        )
    )


def gold_m0_price(monthly_prices: DataFrame) -> DataFrame:
    return monthly_prices.select(
        "country_code",
        "country_name",
        "commodity_code",
        "commodity_name",
        F.trunc(F.col("price_date"), "month").cast("date").alias("month"),
        F.col("price_nominal").alias("price_nominal"),
        F.col("price_real").alias("price_real"),
        F.col("price_usd").alias("price_usd"),
        "currency",
        "unit",
        "series_uuid",
        F.lit("FPMA calculated monthly observation; descriptive input, not event label").alias("measure_note"),
    )


def _assert_quality(
    df: DataFrame,
    dataset: str,
    keys: list[str],
    required_values: list[str] | None = None,
) -> int:
    if df.limit(1).count() == 0:
        raise ValueError(f"{dataset} is empty after filtering")
    missing_key = F.expr(" OR ".join(f"`{key}` IS NULL" for key in keys))
    if df.filter(missing_key).limit(1).count():
        raise ValueError(f"{dataset} has null values in grain key {keys}")
    if required_values:
        missing_value = F.expr(" OR ".join(f"`{name}` IS NULL" for name in required_values))
        if df.filter(missing_value).limit(1).count():
            raise ValueError(f"{dataset} has null required measures {required_values}")
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise ValueError(f"{dataset} has duplicate grain keys: {keys}")
    return df.count()


def _write(df: DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(path))


def run_week2(spark: SparkSession, root: Path, config_path: Path) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    output_root = root / config["output_root"]
    bronze_root = output_root / "bronze"
    silver_root = output_root / "silver"
    gold_root = output_root / "gold"

    price_bronze = read_fpma_bronze(spark, root, config)
    qcl_bronze = read_faostat_bronze(spark, root, config, "qcl_csv")
    tcl_bronze = read_faostat_bronze(spark, root, config, "tcl_csv")
    weather_bronze = read_nasa_bronze(spark, root, config)

    price_silver = price_bronze.withColumn("price_date", F.to_date("price_date"))
    qcl_silver = silver_annual(qcl_bronze, int(config["annual_availability_lag_years"]))
    tcl_silver = silver_annual(tcl_bronze, int(config["annual_availability_lag_years"]))
    weather_daily = weather_bronze.filter(F.col("value") != F.lit(-999.0))
    weather_monthly = silver_weather_monthly(weather_daily)
    monthly_prices = price_silver.filter(F.col("frequency") == "monthly")
    gold_price = gold_m0_price(monthly_prices)

    count_profile = {
        "bronze": {
            "prices": _assert_quality(price_bronze, "bronze.prices", ["series_uuid", "source_record_id"]),
            "production": _assert_quality(qcl_bronze, "bronze.production", ["country_code", "commodity_code", "element", "data_year"], ["value"]),
            "trade": _assert_quality(tcl_bronze, "bronze.trade", ["country_code", "commodity_code", "element", "data_year"], ["value"]),
            "weather_daily_point": _assert_quality(weather_daily, "bronze.weather_daily_point", ["country_code", "commodity_code", "observation_date", "parameter", "latitude", "longitude"]),
        },
        "silver": {
            "prices": price_silver.count(),
            "production": qcl_silver.count(),
            "trade": tcl_silver.count(),
            "weather_daily_screening": weather_daily.count(),
            "weather_monthly_screening": _assert_quality(weather_monthly, "silver.weather_monthly_screening", ["country_code", "commodity_code", "month", "latitude", "longitude"], ["t2m_max_mean_c", "t2m_min_mean_c", "precipitation_sum_mm"]),
        },
        "gold": {
            "m0_monthly_price": _assert_quality(gold_price, "gold.m0_monthly_price", ["country_code", "commodity_code", "month"], ["price_nominal"]),
        },
    }

    datasets = {
        "bronze/prices": price_bronze,
        "bronze/production": qcl_bronze,
        "bronze/trade": tcl_bronze,
        "bronze/weather_daily_point": weather_bronze,
        "silver/prices": price_silver,
        "silver/production": qcl_silver,
        "silver/trade": tcl_silver,
        "silver/weather_daily_screening": weather_daily,
        "silver/weather_monthly_screening": weather_monthly,
        "gold/m0_monthly_price": gold_price,
    }
    for name, df in datasets.items():
        _write(df, output_root / name)

    report = {
        "project": "FoodShield",
        "pipeline": "week2_local_etl",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "country_code": config["country_code"],
        "country_name": config["country_name"],
        "commodity_code": config["commodity_code"],
        "commodity_name": config["commodity_name"],
        "annual_availability_lag_years": config["annual_availability_lag_years"],
        "output_root": str(output_root.relative_to(root)),
        "counts": count_profile,
        "schemas": {name: json.loads(df.schema.json()) for name, df in datasets.items()},
        "limitations": [
            "FPMA source values are retained as local research data; do not redistribute before reuse rights are confirmed.",
            "NASA POWER weather rows are a single-point screening sample, not crop-weighted or nationally representative.",
            "The two-year annual signal lag is a provisional rule, not observed historical release timing.",
            "Gold contains the M0 price baseline only; event windows are not encoded as binary labels.",
        ],
    }
    report_path = output_root / "profile.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
