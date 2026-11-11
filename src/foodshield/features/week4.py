"""Assemble the selected FAOSTAT/SSSU price panel for Week 4 evaluation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from foodshield.etl.week2 import (
    read_faostat_bronze,
    read_nasa_bronze,
    silver_annual,
    silver_weather_monthly,
)
from foodshield.features.week3 import _asof_annual, build_week3_features


PRICE_SCHEMA = StructType([
    StructField("year", IntegerType(), False),
    StructField("month", StringType(), False),
    StructField("nominal_price_lcu_per_tonne", DoubleType(), False),
    StructField("cpi_change_previous_month_percent_index", DoubleType(), True),
    StructField("cpi_index_dec_2000_100", DoubleType(), False),
    StructField("real_price_dec_2000_lcu_per_tonne", DoubleType(), False),
    StructField("producer_price_flag", StringType(), True),
    StructField("cpi_updated", StringType(), True),
])

KEY = ["country_code", "commodity_code", "month"]

WEIGHTED_WEATHER_SCHEMA = StructType([
    StructField("month", StringType(), False),
    StructField("precipitation_sum_mm", DoubleType(), True),
    StructField("t2m_max_mean_c", DoubleType(), True),
    StructField("t2m_min_mean_c", DoubleType(), True),
    StructField("precipitation_days_present", DoubleType(), True),
    StructField("weight_covered", DoubleType(), True),
    StructField("points_present", IntegerType(), True),
])
WEIGHTED_SPATIAL_BASIS = "sssu_wheat_area_weighted_24_oblast_points"


def read_weighted_weather(spark: SparkSession, root: Path, path: str, config: dict[str, Any]) -> DataFrame:
    """Read the wheat-area-weighted monthly weather built by fetch_week4_weighted_weather.py."""
    weather = (
        spark.read.schema(WEIGHTED_WEATHER_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .csv(str(root / path))
        .select(
            F.lit(config["country_code"]).alias("country_code"),
            F.lit(config["commodity_code"]).alias("commodity_code"),
            F.to_date(F.concat_ws("-", F.col("month"), F.lit("01"))).alias("month"),
            # A weighted series has no single coordinate; spatial_basis records the method.
            F.lit(None).cast(DoubleType()).alias("latitude"),
            F.lit(None).cast(DoubleType()).alias("longitude"),
            F.lit(WEIGHTED_SPATIAL_BASIS).alias("spatial_basis"),
            "precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c",
            F.round("precipitation_days_present").cast("long").alias("precipitation_days_present"),
        )
    )
    if weather.groupBy(*KEY).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Weighted weather has duplicate months")
    return weather


def assemble_selected_price_panel(
    spark: SparkSession,
    root: Path,
    price_csv: str,
    week2_config_path: str,
    annual_lag_years: int,
    price_metadata: dict[str, Any],
    weather_monthly_csv: str | None = None,
) -> DataFrame:
    """Build a selected-source panel without reading the local FPMA price data."""
    config_path = root / week2_config_path
    config = json.loads(config_path.read_text(encoding="utf-8"))
    prices = (
        spark.read.schema(PRICE_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .csv(str(root / price_csv))
        .select(
            F.lit(config["country_code"]).alias("country_code"),
            F.lit(config["country_name"]).alias("country_name"),
            F.lit(config["commodity_code"]).alias("commodity_code"),
            F.lit(config["commodity_name"]).alias("commodity_name"),
            F.make_date(F.col("year"), F.col("month").cast(IntegerType()), F.lit(1)).alias("month"),
            F.col("nominal_price_lcu_per_tonne").alias("price_nominal"),
            F.col("real_price_dec_2000_lcu_per_tonne").alias("price_real"),
            F.lit(price_metadata["currency"]).alias("currency"),
            F.lit(price_metadata["price_unit"]).alias("price_unit"),
            F.lit(price_metadata["source"]).alias("price_source"),
            F.lit(int(price_metadata["element_code"])).alias("price_element_code"),
            F.col("producer_price_flag").alias("price_source_flag"),
            F.col("cpi_index_dec_2000_100").alias("cpi_index_dec_2000_100"),
            F.lit(price_metadata["deflator_source"]).alias("deflator_source"),
            F.col("cpi_updated").alias("cpi_source_updated"),
        )
    )
    if prices.filter(F.col("month").isNull() | F.col("price_nominal").isNull() | F.col("price_real").isNull()).limit(1).count():
        raise ValueError("Selected FAOSTAT/SSSU price panel contains null month or price values")
    if prices.groupBy(*KEY).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Selected FAOSTAT/SSSU price panel has duplicate country/commodity/month rows")

    # Reuse the profiled Week 2 readers for the shared QCL/TCL and NASA sources.
    # The configured FPMA input is intentionally never read on this path.
    production = silver_annual(
        read_faostat_bronze(spark, root, config, "qcl_csv"), annual_lag_years
    )
    trade = silver_annual(
        read_faostat_bronze(spark, root, config, "tcl_csv"), annual_lag_years
    )
    if weather_monthly_csv:
        weather = read_weighted_weather(spark, root, weather_monthly_csv, config)
    else:
        daily_weather = read_nasa_bronze(spark, root, config).filter(F.col("value") != F.lit(-999.0))
        weather = silver_weather_monthly(daily_weather).select(
            "country_code", "commodity_code", "month", "latitude", "longitude", "spatial_basis",
            "precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c", "precipitation_days_present",
        )
    panel = prices.join(weather, KEY, "left")
    panel = _asof_annual(panel, production, "production")
    panel = _asof_annual(panel, trade, "trade")
    return panel


def build_week4_selected_features(
    spark: SparkSession,
    root: Path,
    config_path: Path,
    annual_lag_override: int | None = None,
) -> tuple[DataFrame, dict[str, Any]]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if annual_lag_override is not None:  # used only for the D6 availability-lag sensitivity run
        config["annual_availability_lag_years"] = int(annual_lag_override)
    panel = assemble_selected_price_panel(
        spark,
        root,
        config["price_csv"],
        config["week2_config"],
        int(config["annual_availability_lag_years"]),
        config["price_metadata"],
        config.get("weather_monthly_csv"),
    )
    features, profile = build_week3_features(
        panel,
        annual_lag_years=int(config["annual_availability_lag_years"]),
        input_is_synthetic=False,
    )
    profile["pipeline"] = "week4_selected_faostat_sssu_m0_m4_features"
    profile["selected_price_source"] = config["price_metadata"]["source"]
    profile["selected_country"] = config.get("country_name", json.loads((root / config["week2_config"]).read_text(encoding="utf-8"))["country_name"])
    profile["selected_commodity"] = config.get("commodity_name", json.loads((root / config["week2_config"]).read_text(encoding="utf-8"))["commodity_name"])
    profile["price_csv"] = config["price_csv"]
    profile["week2_source_config"] = config["week2_config"]
    profile["source_boundary"] = "The FPMA price source is not read. Shared Week 2 QCL/TCL/NASA readers provide non-price signals."
    profile["weather_input"] = config.get("weather_monthly_csv") or "single NASA POWER screening point from the Week 2 config"
    profile["label_and_holdout_note"] = "This feature panel contains no outcome labels. Train/validation labels and split policy are handled separately; final-test labels are not generated by this run."
    return features, profile
