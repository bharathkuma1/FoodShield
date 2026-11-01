"""Build the Week 3 monthly analytical panel and leakage-aware features."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window


KEY = ["country_code", "commodity_code", "month"]
PRICE_FIELDS = ["price_nominal", "price_real", "price_usd"]
WEATHER_FIELDS = ["precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c"]
PRODUCTION_FIELDS = ["production_t", "yield_kg_ha", "area_harvested_ha"]
TRADE_FIELDS = [
    "import_quantity_t", "export_quantity_t", "import_value_1000_usd", "export_value_1000_usd"
]
SYNTHETIC_PANEL_SCHEMA = StructType([
    StructField("country_code", StringType(), True),
    StructField("commodity_code", StringType(), True),
    StructField("month", DateType(), True),
    StructField("price_nominal", DoubleType(), True),
    StructField("price_real", DoubleType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("price_unit", StringType(), True),
    StructField("precipitation_sum_mm", DoubleType(), True),
    StructField("t2m_max_mean_c", DoubleType(), True),
    StructField("t2m_min_mean_c", DoubleType(), True),
    StructField("spatial_basis", StringType(), True),
    StructField("feature_year", IntegerType(), True),
    StructField("feature_available_from", DateType(), True),
    StructField("production_t", DoubleType(), True),
    StructField("yield_kg_ha", DoubleType(), True),
    StructField("area_harvested_ha", DoubleType(), True),
    StructField("import_quantity_t", DoubleType(), True),
    StructField("export_quantity_t", DoubleType(), True),
    StructField("import_value_1000_usd", DoubleType(), True),
    StructField("export_value_1000_usd", DoubleType(), True),
    StructField("demo_split", StringType(), True),
    StructField("synthetic_event_id", StringType(), True),
    StructField("is_synthetic", BooleanType(), True),
    StructField("synthetic_demo_future_price_stress", IntegerType(), True),
])


def _check_panel(df: DataFrame) -> int:
    if df.limit(1).count() == 0:
        raise ValueError("Week 3 input panel is empty")
    if df.filter(F.expr(" OR ".join(f"`{key}` IS NULL" for key in KEY))).limit(1).count():
        raise ValueError(f"Null monthly panel key: {KEY}")
    if df.groupBy(*KEY).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Duplicate monthly panel grain: {KEY}")
    return df.count()


def _asof_annual(monthly: DataFrame, annual: DataFrame, signal: str) -> DataFrame:
    """Attach the most recent annual record available by each forecast month."""
    if signal == "production":
        mapping = {
            "Production": "production_t",
            "Yield": "yield_kg_ha",
            "Area harvested": "area_harvested_ha",
        }
    else:
        mapping = {
            "Import quantity": "import_quantity_t",
            "Export quantity": "export_quantity_t",
            "Import value": "import_value_1000_usd",
            "Export value": "export_value_1000_usd",
        }
    aggregations = [
        F.max(F.when(F.col("element") == label, F.col("value").cast("double"))).alias(field)
        for label, field in mapping.items()
    ]
    wide = annual.groupBy("country_code", "commodity_code", "data_year", "feature_available_from").agg(
        *aggregations,
        F.collect_list(F.struct("element", "unit", "flag", "note")).alias(f"{signal}_source_metadata"),
    )
    wide = wide.withColumnRenamed("data_year", f"{signal}_feature_year").withColumnRenamed(
        "feature_available_from", f"{signal}_available_from"
    )
    m, a = monthly.alias("m"), wide.alias("a")
    condition = (
        (F.col("m.country_code") == F.col("a.country_code"))
        & (F.col("m.commodity_code") == F.col("a.commodity_code"))
        & (F.col(f"a.{signal}_available_from") <= F.col("m.month"))
    )
    joined = m.join(a, condition, "left")
    rank = Window.partitionBy(*[F.col(f"m.{key}") for key in KEY]).orderBy(
        F.col(f"a.{signal}_available_from").desc_nulls_last(),
        F.col(f"a.{signal}_feature_year").desc_nulls_last(),
    )
    joined = joined.withColumn("_annual_rank", F.row_number().over(rank)).filter(F.col("_annual_rank") == 1)
    appended = [
        F.col(f"a.{signal}_feature_year"), F.col(f"a.{signal}_available_from"),
        F.col(f"a.{signal}_source_metadata"),
    ] + [F.col(f"a.{field}") for field in mapping.values()]
    return joined.select("m.*", *appended)


def assemble_week2_panel(spark: SparkSession, root: Path, week2_output_root: str) -> DataFrame:
    """Align Week 2 Gold/Silver tables at monthly grain with as-of annual joins."""
    source_root = root / week2_output_root
    panel = spark.read.parquet(str(source_root / "gold" / "m0_monthly_price"))
    panel = panel.withColumn("month", F.trunc(F.to_date("month"), "month").cast("date"))
    weather = spark.read.parquet(str(source_root / "silver" / "weather_monthly_screening"))
    weather = weather.select(
        "country_code", "commodity_code", "month", "latitude", "longitude", "spatial_basis",
        "precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c", "precipitation_days_present",
    )
    panel = panel.join(weather, KEY, "left")
    production = spark.read.parquet(str(source_root / "silver" / "production"))
    trade = spark.read.parquet(str(source_root / "silver" / "trade"))
    panel = _asof_annual(panel, production, "production")
    panel = _asof_annual(panel, trade, "trade")
    return panel


def build_week3_features(
    panel: DataFrame, annual_lag_years: int = 2, input_is_synthetic: bool = False
) -> tuple[DataFrame, dict[str, Any]]:
    """Create M0–M4 feature columns from an already aligned monthly panel.

    Lag/rolling features use observations strictly before the current month.
    Current-month contemporaneous fields remain available for same-month signal
    descriptions. Annual features are nulled until their availability date.
    """
    observed_count = _check_panel(panel)
    panel = panel.withColumn("month", F.trunc(F.to_date("month"), "month").cast("date"))
    # Materialize missing calendar months as null-valued rows so lag_1m means
    # the immediately preceding calendar month, rather than previous record.
    bounds = panel.groupBy("country_code", "commodity_code").agg(
        F.min("month").alias("_start_month"), F.max("month").alias("_end_month")
    )
    spine = bounds.select(
        "country_code", "commodity_code",
        F.explode(F.sequence("_start_month", "_end_month", F.expr("interval 1 month"))).alias("month"),
    )
    df = spine.join(panel, KEY, "left")
    count = df.count()
    order = Window.partitionBy("country_code", "commodity_code").orderBy("month")
    generated: list[str] = []

    for name in PRICE_FIELDS:
        if name not in df.columns:
            continue
        for lag in (1, 3, 6, 12):
            out = f"{name}_lag_{lag}m"
            df = df.withColumn(out, F.lag(name, lag).over(order))
            generated.append(out)
        for width in (3, 6, 12):
            prior = order.rowsBetween(-width, -1)
            mean_name, sd_name = f"{name}_prior_{width}m_mean", f"{name}_prior_{width}m_sd"
            prior_count = F.count(name).over(prior)
            df = df.withColumn(mean_name, F.when(prior_count >= width, F.avg(name).over(prior)))
            df = df.withColumn(sd_name, F.when(prior_count >= width, F.stddev_samp(name).over(prior)))
            generated.extend([mean_name, sd_name])
        prior_year = order.rowsBetween(-12, -1)
        mean_name, sd_name = f"{name}_prior_12m_mean", f"{name}_prior_12m_sd"
        z_name = f"{name}_anomaly_z_12m"
        prior_count = F.count(name).over(prior_year)
        df = df.withColumn(z_name, F.when((prior_count >= 12) & (F.col(sd_name) > 0), (F.col(name) - F.col(mean_name)) / F.col(sd_name)))
        generated.append(z_name)
        df = df.withColumn(f"{name}_mom_change", F.col(name) / F.lag(name, 1).over(order) - F.lit(1.0))
        generated.append(f"{name}_mom_change")

    # Keep an annual observation out of the panel before its configured
    # availability date. Preserve the source data year and date for audit.
    annual_rules = [
        (PRODUCTION_FIELDS, "production_available_from"),
        (TRADE_FIELDS, "trade_available_from"),
    ]
    if "feature_available_from" in df.columns:
        df = df.withColumn("feature_available_from", F.to_date("feature_available_from"))
        annual_rules = [
            (PRODUCTION_FIELDS + TRADE_FIELDS, "feature_available_from")
        ]
    for fields, availability_col in annual_rules:
        if availability_col not in df.columns:
            if any(name in df.columns for name in fields):
                raise ValueError(f"Annual feature columns require {availability_col}")
            continue
        df = df.withColumn(availability_col, F.to_date(availability_col))
        available = F.col(availability_col) <= F.col("month")
        for field in fields:
            if field in df.columns:
                df = df.withColumn(field, F.when(available, F.col(field)))

    for name in WEATHER_FIELDS + PRODUCTION_FIELDS + TRADE_FIELDS:
        if name not in df.columns:
            continue
        for lag in (1, 3, 12):
            out = f"{name}_lag_{lag}m"
            df = df.withColumn(out, F.lag(name, lag).over(order))
            generated.append(out)
        prior = order.rowsBetween(-12, -1)
        mean_name, sd_name = f"{name}_prior_12m_mean", f"{name}_prior_12m_sd"
        prior_count = F.count(name).over(prior)
        df = df.withColumn(mean_name, F.when(prior_count >= 12, F.avg(name).over(prior))).withColumn(
            sd_name, F.when(prior_count >= 12, F.stddev_samp(name).over(prior))
        )
        z_name = f"{name}_anomaly_z_12m"
        df = df.withColumn(z_name, F.when((prior_count >= 12) & (F.col(sd_name) > 0), (F.col(name) - F.col(mean_name)) / F.col(sd_name)))
        generated.extend([mean_name, sd_name, z_name])

    # M4 interactions are explicit and use only the contemporaneous signal
    # anomaly candidates already calculated above.
    if "price_real_anomaly_z_12m" in df.columns and "precipitation_sum_mm_anomaly_z_12m" in df.columns:
        df = df.withColumn(
            "m4_price_precipitation_anomaly_interaction",
            F.col("price_real_anomaly_z_12m") * F.col("precipitation_sum_mm_anomaly_z_12m"),
        )
        generated.append("m4_price_precipitation_anomaly_interaction")

    df = df.withColumn("feature_month", F.col("month"))
    if "is_synthetic" in df.columns:
        df = df.withColumn("is_synthetic", F.coalesce(F.col("is_synthetic").cast("boolean"), F.lit(input_is_synthetic)))
    else:
        df = df.withColumn("is_synthetic", F.lit(input_is_synthetic))

    stages: dict[str, Any] = {}
    signals = {
        "M0": PRICE_FIELDS,
        "M1": PRICE_FIELDS + WEATHER_FIELDS,
        "M2": PRICE_FIELDS + WEATHER_FIELDS + PRODUCTION_FIELDS,
        "M3": PRICE_FIELDS + WEATHER_FIELDS + PRODUCTION_FIELDS + TRADE_FIELDS,
        "M4": PRICE_FIELDS + WEATHER_FIELDS + PRODUCTION_FIELDS + TRADE_FIELDS,
    }
    stage_feature_fields = {
        "M0": [
            "price_real_lag_1m", "price_real_lag_3m", "price_real_lag_6m", "price_real_lag_12m",
            "price_real_prior_3m_mean", "price_real_prior_6m_mean", "price_real_prior_12m_mean",
            "price_real_anomaly_z_12m",
        ],
        "M1": [
            "precipitation_sum_mm_anomaly_z_12m", "t2m_max_mean_c_anomaly_z_12m",
            "t2m_min_mean_c_anomaly_z_12m",
        ],
        "M2": [name + "_anomaly_z_12m" for name in PRODUCTION_FIELDS],
        "M3": [name + "_anomaly_z_12m" for name in TRADE_FIELDS],
        "M4": ["m4_price_precipitation_anomaly_interaction"],
    }
    metric_exprs = []
    present_by_stage: dict[str, list[str]] = {}
    for stage, fields in signals.items():
        present = [name for name in fields if name in df.columns]
        present_by_stage[stage] = present
        complete = F.lit(True)
        for name in present:
            complete = complete & F.col(name).isNotNull()
        metric_exprs.append(F.sum(F.when(complete, 1).otherwise(0)).alias(f"{stage}_complete"))
    all_signals = sorted({field for fields in present_by_stage.values() for field in fields})
    for field in all_signals:
        metric_exprs.append(F.sum(F.when(F.col(field).isNull(), 1).otherwise(0)).alias(f"missing__{field}"))
    features_by_stage: dict[str, list[str]] = {}
    accumulated_features: list[str] = []
    for stage in ("M0", "M1", "M2", "M3", "M4"):
        accumulated_features.extend(stage_feature_fields[stage])
        features_by_stage[stage] = [name for name in accumulated_features if name in df.columns]
        feature_complete = F.lit(True)
        for name in features_by_stage[stage]:
            feature_complete = feature_complete & F.col(name).isNotNull()
        metric_exprs.append(F.sum(F.when(feature_complete, 1).otherwise(0)).alias(f"{stage}_features_complete"))
    all_features = sorted({field for fields in features_by_stage.values() for field in fields})
    for field in all_features:
        metric_exprs.append(F.sum(F.when(F.col(field).isNull(), 1).otherwise(0)).alias(f"missing__{field}"))
    metrics = df.agg(*metric_exprs).first().asDict()
    for stage, fields in signals.items():
        present = present_by_stage[stage]
        stages[stage] = {
            "signal_fields_present": present,
            "complete_row_count": int(metrics[f"{stage}_complete"] or 0),
            "row_count": count,
            "missing_by_signal": {field: int(metrics[f"missing__{field}"] or 0) for field in present},
            "feature_fields_required": features_by_stage[stage],
            "feature_complete_row_count": int(metrics[f"{stage}_features_complete"] or 0),
            "missing_by_feature": {
                field: int(metrics[f"missing__{field}"] or 0) for field in features_by_stage[stage]
            },
        }
    profile = {
        "pipeline": "week3_monthly_features",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "grain": KEY,
        "row_count": count,
        "input_observed_row_count": observed_count,
        "annual_availability_lag_years": annual_lag_years,
        "stages": stages,
        "feature_columns": generated,
        "limitations": [
            "Input panel source status is retained in is_synthetic; synthetic rows are development-only.",
            "Annual availability dates follow the provisional configured lag and are not verified release vintages.",
            "Weather features inherit the input spatial_basis; a screening point is not nationally representative.",
            "Rolling statistics use prior rows only and require a full respective history to have non-null values.",
            "This feature build does not define an outcome, train a model, or establish performance.",
        ],
    }
    return df, profile


def run_week3(spark: SparkSession, root: Path, config_path: Path) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    output_root = root / config["output_root"]
    if config.get("input_mode", "synthetic") == "week2":
        panel = assemble_week2_panel(spark, root, config["week2_output_root"])
        input_path = config["week2_output_root"]
    else:
        source_path = root / config["input_csv"]
        panel = spark.read.schema(SYNTHETIC_PANEL_SCHEMA).option("header", "true").csv(str(source_path))
        input_path = config["input_csv"]
    features, profile = build_week3_features(
        panel,
        int(config.get("annual_availability_lag_years", 2)),
        bool(config.get("input_is_synthetic", False)),
    )
    output_root.mkdir(parents=True, exist_ok=True)
    features.write.mode("overwrite").parquet(str(output_root / "features"))
    profile["input_path"] = input_path
    profile["output_path"] = str((output_root / "features").relative_to(root)).replace("\\", "/")
    profile["input_is_synthetic"] = config.get("input_is_synthetic", False)
    (output_root / "profile.json").write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8")
    return profile
