"""Week 5 annual-signal features: year-on-year change of the latest available annual value.

The Week 3 annual "anomaly" features compare a step series (one annual value
repeated monthly) with its trailing 12-month window. The result carries only
the sign of the year-on-year change, follows a fixed month-of-year decay
(3.175 in February to 0.289 in December), and is undefined every January.
These change features keep the magnitude instead.

For each forecast month t the panel already holds the latest annual data year
available by t (``production_feature_year`` / ``trade_feature_year``, set by
the configured availability lag). The feature compares that year's value with
the year before:

    symmetric change = (v_Y - v_(Y-1)) / ((|v_Y| + |v_(Y-1)|) / 2)

It is bounded in [-2, 2], so Ukraine's very small wheat imports cannot produce
extreme ratios, and it is null when both values are zero or either is missing.
Both years are available by t, because v_(Y-1) was released before v_Y.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from foodshield.etl.week2 import read_faostat_bronze


ANNUAL_SIGNALS = {
    "production": ("qcl_csv", {"Production": "production_t", "Yield": "yield_kg_ha",
                               "Area harvested": "area_harvested_ha"}),
    "trade": ("tcl_csv", {"Import quantity": "import_quantity_t", "Export quantity": "export_quantity_t",
                          "Import value": "import_value_1000_usd", "Export value": "export_value_1000_usd"}),
}


WEATHER_FIELDS = ["precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c"]
CLIMATOLOGY_YEARS, CLIMATOLOGY_MIN_YEARS = 10, 5


def add_weather_climatology_anomalies(spark: SparkSession, root: Path, panel: DataFrame, weather_csv: str,
                                      week2_config_path: str) -> DataFrame:
    """Same-calendar-month weather anomalies, past-only.

    The Week 3 weather anomaly compares a month with the trailing 12 months, so
    for temperature it mostly measures the season (about 95% of its variance is
    explained by calendar month). Here each month is compared with the same
    calendar month over the previous 10 years (at least 5), using the full
    2001-2022 weighted series, so the value measures unusual weather, not season.
    """
    from foodshield.features.week4 import read_weighted_weather
    from pyspark.sql import Window

    config = json.loads((root / week2_config_path).read_text(encoding="utf-8"))
    weather = read_weighted_weather(spark, root, weather_csv, config).select("month", *WEATHER_FIELDS)
    by_month = Window.partitionBy(F.month("month")).orderBy("month").rowsBetween(-CLIMATOLOGY_YEARS, -1)
    out = weather
    for name in WEATHER_FIELDS:
        n, mean, sd = F.count(name).over(by_month), F.avg(name).over(by_month), F.stddev_samp(name).over(by_month)
        out = out.withColumn(f"{name}_clim_z", F.when((n >= CLIMATOLOGY_MIN_YEARS) & (sd > 0), (F.col(name) - mean) / sd))
    out = out.select("month", *[f"{name}_clim_z" for name in WEATHER_FIELDS])
    panel = panel.join(out, "month", "left")
    angle = 2 * 3.141592653589793 * (F.month("month") - 1) / 12
    return (panel
            .withColumn("m4_price_precip_clim_interaction",
                        F.col("price_real_anomaly_z_12m") * F.col("precipitation_sum_mm_clim_z"))
            .withColumn("month_sin", F.sin(angle))
            .withColumn("month_cos", F.cos(angle)))


def change_feature_names() -> dict[str, list[str]]:
    return {signal: [f"{field}_yoy_sym_change" for field in mapping.values()]
            for signal, (_, mapping) in ANNUAL_SIGNALS.items()}


def add_annual_change_features(spark: SparkSession, root: Path, panel: DataFrame, week2_config_path: str) -> DataFrame:
    config = json.loads((root / week2_config_path).read_text(encoding="utf-8"))
    for signal, (input_key, mapping) in ANNUAL_SIGNALS.items():
        bronze = read_faostat_bronze(spark, root, config, input_key)
        wide = bronze.groupBy("data_year").agg(*[
            F.max(F.when(F.col("element") == label, F.col("value").cast("double"))).alias(field)
            for label, field in mapping.items()
        ])
        previous = wide.select((F.col("data_year") + 1).alias("data_year"),
                               *[F.col(field).alias(f"{field}__prev") for field in mapping.values()])
        changes = wide.join(previous, "data_year", "left").select(
            F.col("data_year").alias(f"{signal}_feature_year"),
            *[
                F.when(
                    F.col(field).isNotNull() & F.col(f"{field}__prev").isNotNull()
                    & ((F.abs(F.col(field)) + F.abs(F.col(f"{field}__prev"))) > 0),
                    (F.col(field) - F.col(f"{field}__prev"))
                    / ((F.abs(F.col(field)) + F.abs(F.col(f"{field}__prev"))) / 2.0),
                ).alias(f"{field}_yoy_sym_change")
                for field in mapping.values()
            ],
        )
        panel = panel.join(changes, f"{signal}_feature_year", "left")
    return panel
