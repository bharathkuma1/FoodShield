"""M0-M4 stage definitions and one shared Spark MLlib estimator for the ablation.

Every stage uses the same estimator and settings as M0 (binomial logistic
regression, standardization on, L2 regularization), so differences between
stages come from the added signal groups, not from model choice.
"""

from __future__ import annotations

from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession

from foodshield.features.week5 import change_feature_names
from foodshield.models.m0 import M0_FEATURES

WEATHER = ["precipitation_sum_mm_anomaly_z_12m", "t2m_max_mean_c_anomaly_z_12m", "t2m_min_mean_c_anomaly_z_12m"]
PRODUCTION_Z = ["production_t_anomaly_z_12m", "yield_kg_ha_anomaly_z_12m", "area_harvested_ha_anomaly_z_12m"]
TRADE_Z = ["import_quantity_t_anomaly_z_12m", "export_quantity_t_anomaly_z_12m",
           "import_value_1000_usd_anomaly_z_12m", "export_value_1000_usd_anomaly_z_12m"]
INTERACTION = ["m4_price_precipitation_anomaly_interaction"]
WEATHER_CLIM = ["precipitation_sum_mm_clim_z", "t2m_max_mean_c_clim_z", "t2m_min_mean_c_clim_z"]
INTERACTION_CLIM = ["m4_price_precip_clim_interaction"]
SEASON = ["month_sin", "month_cos"]
_CHANGE = change_feature_names()


def _cumulative(weather: list[str], production: list[str], trade: list[str], interaction: list[str]) -> dict[str, list[str]]:
    m0 = list(M0_FEATURES)
    m1 = m0 + weather
    m2 = m1 + production
    m3 = m2 + trade
    # "M0+season" is a control, not a model stage: price plus calendar month only.
    # Weather only adds value if M1 beats it.
    return {"M0": m0, "M0+season": m0 + SEASON, "M1": m1, "M2": m2, "M3": m3, "M4": m3 + interaction}


FEATURE_VARIANTS: dict[str, dict[str, list[str]]] = {
    # Week 5 corrected features (recommended, decision D10): weather compared with the same
    # calendar month in prior years; annual signals as bounded year-on-year change.
    "corrected": _cumulative(WEATHER_CLIM, _CHANGE["production"], _CHANGE["trade"], INTERACTION_CLIM),
    # Robustness check: the corrected features with calendar month added to every stage, asking
    # whether weather, production and trade add anything beyond price plus season.
    "corrected_plus_season": {stage: (feats if stage == "M0+season" else feats + [f for f in SEASON if f not in feats])
                              for stage, feats in _cumulative(WEATHER_CLIM, _CHANGE["production"], _CHANGE["trade"],
                                                              INTERACTION_CLIM).items()},
    # Week 3 contract as built: trailing 12-month anomalies for weather and annual signals.
    "contract_anomaly": _cumulative(WEATHER, PRODUCTION_Z, TRADE_Z, INTERACTION),
}
STAGES = ("M0", "M0+season", "M1", "M2", "M3", "M4")


def fit_and_score(spark: SparkSession, train: list[dict], score: list[dict], features: list[str],
                  model_config: dict[str, Any]) -> tuple[dict[str, float], dict[str, Any]]:
    """Fit on ``train`` rows, return P(label=1) for every row in ``score`` and the fitted coefficients."""
    columns = ["origin", "label", *features]
    to_tuple = lambda r: (r["origin"], float(r["label"]) if r["label"] is not None else None,
                          *[float(r[f]) for f in features])
    train_df = spark.createDataFrame([to_tuple(r) for r in train], columns)
    score_df = spark.createDataFrame([to_tuple(r) for r in score], columns)
    fitted = Pipeline(stages=[
        VectorAssembler(inputCols=features, outputCol="x", handleInvalid="error"),
        LogisticRegression(featuresCol="x", labelCol="label", family="binomial",
                           regParam=float(model_config["regularization_l2"]), elasticNetParam=0.0,
                           maxIter=int(model_config["max_iterations"]), standardization=True),
    ]).fit(train_df)
    scores = {r["origin"]: float(r["p"]) for r in fitted.transform(score_df)
              .select("origin", vector_to_array("probability")[1].alias("p")).collect()}
    lr = fitted.stages[-1]
    return scores, {"coefficients": dict(zip(features, map(float, lr.coefficients))), "intercept": float(lr.intercept)}
