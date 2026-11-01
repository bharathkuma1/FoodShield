"""Generate a small synthetic Ukraine wheat panel for pipeline development.

Only distribution summaries from local raw sources are used for calibration.
No source rows are copied into the output. The generated values and labels are
for development only and must never be reported as observations or results.
"""

from __future__ import annotations

import csv
import json
import math
import random
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "synthetic_week3.local.json"


def median(values: Iterable[float]) -> float:
    return float(statistics.median(list(values)))


def stdev(values: list[float], fallback: float) -> float:
    if len(values) < 2:
        return fallback
    return float(statistics.stdev(values))


def clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def monthly_price_profile(path: Path) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    series = document["results"][0]
    values: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    rows = [p for p in series["datapoints"] if p.get("periodicity") == "monthly"]
    for row in rows:
        month = int(row["date"][5:7])
        for key in ("price_value", "price_value_real", "price_value_dollar"):
            value = row.get(key)
            if value is not None and float(value) > 0:
                values[month][key].append(float(value))
    if not rows:
        raise ValueError(f"No monthly FPMA observations in {path}")

    profile = {}
    for month in range(1, 13):
        vals = values[month]
        nominal = median(vals["price_value"])
        real = median(vals["price_value_real"])
        usd = median(vals["price_value_dollar"])
        profile[month] = {
            "nominal": nominal,
            "real": real,
            "usd": usd,
            "nominal_to_real": nominal / real,
            "usd_to_nominal": usd / nominal,
            "real_log_sigma": clamp(
                stdev([math.log(v / real) for v in vals["price_value_real"]], 0.04),
                0.02,
                0.18,
            ),
        }
    real_all = [float(p["price_value_real"]) for p in rows if p.get("price_value_real") and float(p["price_value_real"]) > 0]
    nominal_all = [float(p["price_value"]) for p in rows if p.get("price_value") and float(p["price_value"]) > 0]
    usd_all = [float(p["price_value_dollar"]) for p in rows if p.get("price_value_dollar") and float(p["price_value_dollar"]) > 0]
    ratio_rows = [
        p for p in rows
        if p.get("price_value") and p.get("price_value_real") and p.get("price_value_dollar")
        and float(p["price_value"]) > 0 and float(p["price_value_real"]) > 0
    ]
    profile["global_real_median"] = median(real_all)
    profile["global_nominal_to_real"] = median(
        float(p["price_value"]) / float(p["price_value_real"]) for p in ratio_rows
    )
    profile["global_usd_to_nominal"] = median(
        float(p["price_value_dollar"]) / float(p["price_value"]) for p in ratio_rows
    )
    returns = []
    ordered = sorted(
        (p for p in rows if p.get("price_value_real") and float(p["price_value_real"]) > 0),
        key=lambda p: p["date"],
    )
    for previous, current in zip(ordered, ordered[1:]):
        a, b = float(previous["price_value_real"]), float(current["price_value_real"])
        if a > 0 and b > 0:
            returns.append(math.log(b / a))
    profile["monthly_log_return_sigma"] = clamp(stdev(returns, 0.05), 0.02, 0.12)
    profile["monthly_log_return_median"] = clamp(median(returns), -0.005, 0.005)
    profile["observation_count"] = len(rows)
    return profile


def monthly_weather_profile(path: Path) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    parameters = document["properties"]["parameter"]
    precipitation = parameters["PRECTOTCORR"]
    tmax = parameters["T2M_MAX"]
    tmin = parameters["T2M_MIN"]
    day_rows: dict[str, list[float]] = {}
    for day in sorted(set(precipitation) & set(tmax) & set(tmin)):
        p, hi, lo = float(precipitation[day]), float(tmax[day]), float(tmin[day])
        if -900 < p and -900 < hi and -900 < lo:
            day_rows.setdefault(day[:6], []).append((p, hi, lo))

    month_samples: dict[int, list[tuple[float, float, float]]] = defaultdict(list)
    for ym, rows in day_rows.items():
        month_samples[int(ym[4:6])].append(
            (sum(r[0] for r in rows), statistics.mean(r[1] for r in rows), statistics.mean(r[2] for r in rows))
        )
    profile = {}
    for month in range(1, 13):
        rows = month_samples[month]
        if not rows:
            raise ValueError(f"No NASA POWER observations for calendar month {month}")
        p_values, hi_values, lo_values = zip(*rows)
        profile[month] = {
            "precip_mean": max(0.05, statistics.mean(p_values)),
            "precip_sd": max(0.05, stdev(list(p_values), max(0.1, statistics.mean(p_values) * 0.5))),
            "tmax_mean": statistics.mean(hi_values),
            "tmax_sd": max(0.1, stdev(list(hi_values), 1.0)),
            "tmin_mean": statistics.mean(lo_values),
            "tmin_sd": max(0.1, stdev(list(lo_values), 1.0)),
        }
    profile["monthly_observation_count"] = len(day_rows)
    return profile


def read_annual_series(path: Path, elements: list[str]) -> dict[str, dict[int, float]]:
    series: dict[str, dict[int, float]] = {element: {} for element in elements}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            element = row.get("Element", "")
            if element not in series:
                continue
            try:
                year = int(row["Year"])
                value = float(row["Value"])
            except (KeyError, TypeError, ValueError):
                continue
            if value > 0:
                series[element][year] = value
    missing = [name for name, values in series.items() if not values]
    if missing:
        raise ValueError(f"No positive values for {missing} in {path}")
    return series


def log_trend_profile(values: dict[int, float]) -> dict[str, float]:
    years = sorted(values)
    x_mean = statistics.mean(years)
    y = {year: math.log(values[year]) for year in years}
    y_mean = statistics.mean(y.values())
    denominator = sum((year - x_mean) ** 2 for year in years)
    slope = sum((year - x_mean) * (y[year] - y_mean) for year in years) / denominator
    intercept = y_mean - slope * x_mean
    residuals = [y[year] - (intercept + slope * year) for year in years]
    low, high = statistics.quantiles(list(values.values()), n=20)[0], statistics.quantiles(list(values.values()), n=20)[-1]
    return {
        "intercept": intercept,
        "slope": slope,
        "sigma": clamp(stdev(residuals, 0.05), 0.02, 0.22),
        "low": max(1e-9, low * 0.8),
        "high": high * 1.2,
    }


def simulate_annual(profile: dict[str, float], year: int, rng: random.Random) -> float:
    value = math.exp(profile["intercept"] + profile["slope"] * year + rng.gauss(0, profile["sigma"]))
    return clamp(value, profile["low"], profile["high"])


def month_index(ym: str) -> int:
    year, month = map(int, ym.split("-"))
    return year * 12 + month - 1


def event_effect(ym: str, events: list[dict]) -> tuple[float, str, bool]:
    idx = month_index(ym)
    price_multiplier = 1.0
    event_id = ""
    weather_shock = False
    for event in events:
        start = month_index(event["start_month"])
        duration = int(event["duration_months"])
        k = idx - start
        lead = int(event["weather_lead_months"])
        decay = int(event["price_decay_months"])
        if -lead <= k < duration + decay:
            event_id = event["event_id"]
        if -lead <= k < 0:
            weather_shock = True
        if 0 <= k < duration:
            ramp = min(1.0, (k + 1) / 2.0)
            price_multiplier *= 1.0 + float(event["price_shock_fraction"]) * ramp
        elif duration <= k < duration + decay:
            remaining = 1.0 - (k - duration + 1) / max(1, decay)
            price_multiplier *= 1.0 + float(event["price_shock_fraction"]) * max(0.0, remaining)
    return price_multiplier, event_id, weather_shock


def month_range(start: str, end: str) -> list[str]:
    first = date.fromisoformat(f"{start}-01")
    last = date.fromisoformat(f"{end}-01")
    out = []
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        out.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return out


def build_fixture(config: dict) -> tuple[list[dict], dict]:
    price_path = ROOT / config["calibration_sources"]["fpma_monthly"]
    weather_path = ROOT / config["calibration_sources"]["nasa_daily"]
    qcl_path = ROOT / config["calibration_sources"]["qcl_annual"]
    tcl_path = ROOT / config["calibration_sources"]["tcl_annual"]
    for path in (price_path, weather_path, qcl_path, tcl_path):
        if not path.is_file():
            raise FileNotFoundError(f"Calibration input not found: {path}")

    price = monthly_price_profile(price_path)
    weather = monthly_weather_profile(weather_path)
    qcl = read_annual_series(qcl_path, ["Production", "Yield", "Area harvested"])
    tcl = read_annual_series(
        tcl_path,
        ["Import quantity", "Export quantity", "Import value", "Export value"],
    )
    qcl_profile = {name: log_trend_profile(values) for name, values in qcl.items()}
    tcl_profile = {name: log_trend_profile(values) for name, values in tcl.items()}

    rng = random.Random(int(config["seed"]))
    months = month_range(config["start_month"], config["end_month"])
    annual_years = sorted({int(month[:4]) - int(config["annual_availability_lag_years"]) for month in months})
    annual_values = {
        year: {
            "production_t": simulate_annual(qcl_profile["Production"], year, rng),
            "yield_kg_ha": simulate_annual(qcl_profile["Yield"], year, rng),
            "area_harvested_ha": simulate_annual(qcl_profile["Area harvested"], year, rng),
            "import_quantity_t": simulate_annual(tcl_profile["Import quantity"], year, rng),
            "export_quantity_t": simulate_annual(tcl_profile["Export quantity"], year, rng),
            "import_value_1000_usd": simulate_annual(tcl_profile["Import value"], year, rng),
            "export_value_1000_usd": simulate_annual(tcl_profile["Export value"], year, rng),
        }
        for year in annual_years
    }

    rows = []
    previous_log_gap = 0.0
    for ym in months:
        year, month = map(int, ym.split("-"))
        pm = price[month]
        wm = weather[month]
        shock, event_id, weather_shock = event_effect(ym, config["synthetic_events"])
        previous_log_gap = 0.7 * previous_log_gap + rng.gauss(0, price["monthly_log_return_sigma"] * 0.14)
        drift = price["monthly_log_return_median"] * (month_index(ym) - month_index(config["start_month"]))
        price_real = price["global_real_median"] * math.exp(
            drift + previous_log_gap + rng.gauss(0, price["monthly_log_return_sigma"] * 0.14)
        ) * shock
        price_nominal = price_real * price["global_nominal_to_real"]
        price_usd = price_nominal * price["global_usd_to_nominal"]

        z = rng.gauss(0, 1)
        tmax = wm["tmax_mean"] + wm["tmax_sd"] * z + (1.5 if weather_shock else 0.0)
        tmin = wm["tmin_mean"] + wm["tmin_sd"] * (0.7 * z + 0.714 * rng.gauss(0, 1)) + (0.7 if weather_shock else 0.0)
        if tmin >= tmax:
            tmin = tmax - 2.0
        precip_sigma2 = math.log(1.0 + (wm["precip_sd"] / wm["precip_mean"]) ** 2)
        precip = rng.lognormvariate(math.log(wm["precip_mean"]) - precip_sigma2 / 2, math.sqrt(precip_sigma2))
        if weather_shock:
            precip *= 0.5

        feature_year = year - int(config["annual_availability_lag_years"])
        annual = annual_values[feature_year]
        split = config["split_by_year"].get(str(year), config["default_split"])
        rows.append(
            {
                "country_code": config["country_code"],
                "commodity_code": config["commodity_code"],
                "month": f"{ym}-01",
                "price_nominal": round(price_nominal, 4),
                "price_real": round(price_real, 4),
                "price_usd": round(price_usd, 4),
                "currency": config["price_currency"],
                "price_unit": config["price_unit"],
                "precipitation_sum_mm": round(precip, 3),
                "t2m_max_mean_c": round(tmax, 3),
                "t2m_min_mean_c": round(tmin, 3),
                "spatial_basis": "single_point_screening_synthetic",
                "feature_year": feature_year,
                "feature_available_from": f"{year:04d}-01-01",
                **{name: round(value, 3) for name, value in annual.items()},
                "demo_split": split,
                "synthetic_event_id": event_id,
                "is_synthetic": True,
            }
        )

    horizon = int(config["target_horizon_months"])
    threshold = float(config["price_stress_threshold_fraction"])
    price_values = [row["price_real"] for row in rows]
    for i, row in enumerate(rows):
        future = price_values[i + 1 : i + 1 + horizon]
        row["synthetic_demo_future_price_stress"] = (
            int(max(future) >= price_values[i] * (1.0 + threshold)) if len(future) == horizon else ""
        )
    metadata = {
        "purpose": "synthetic development fixture only; never evidence, observed data, or final evaluation",
        "generated_seed": int(config["seed"]),
        "row_count": len(rows),
        "month_range": [rows[0]["month"], rows[-1]["month"]],
        "calibration_inputs": {
            "fpma_monthly": config["calibration_sources"]["fpma_monthly"],
            "nasa_daily": config["calibration_sources"]["nasa_daily"],
            "qcl_annual": config["calibration_sources"]["qcl_annual"],
            "tcl_annual": config["calibration_sources"]["tcl_annual"],
        },
        "generation_method": "Synthetic values generated from source distribution summaries (calendar-month medians/variation and annual log trends); no source rows are copied.",
        "temporal_rule": f"Annual synthetic features use data_year = forecast_year - {config['annual_availability_lag_years']} and become available January 1 of forecast_year. This mirrors a provisional project assumption, not verified release vintages.",
        "demo_split": config["split_by_year"],
        "synthetic_events": config["synthetic_events"],
        "synthetic_label_rule": {
            "name": "synthetic_demo_future_price_stress",
            "horizon_months": horizon,
            "threshold_fraction": threshold,
            "definition": "1 when the maximum synthetic real price in the next horizon months is at least (1 + threshold_fraction) times the current synthetic real price; otherwise 0; final rows without full horizon are blank.",
            "status": "illustrative only; replace with a team-approved outcome/horizon/threshold for real evaluation",
        },
        "limitations": [
            "All rows and labels are synthetic; demo_split is not a validated train/test split.",
            "Weather is calibrated from a single NASA POWER point, not a wheat-area or national signal.",
            "Annual lags are a provisional proxy; no publication vintages are simulated.",
            "The synthetic event injections are invented scenarios, not historical event reconstructions or causal claims.",
            "Generated data are local development fixtures and should not be published as observations.",
        ],
    }
    return rows, metadata


def main() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    rows, metadata = build_fixture(config)
    output_dir = ROOT / config["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "ukraine_wheat_monthly_synthetic.csv"
    metadata_path = output_dir / "ukraine_wheat_monthly_synthetic.metadata.json"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    metadata["output_file"] = str(csv_path.relative_to(ROOT)).replace("\\", "/")
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(rows)} synthetic monthly rows to {csv_path}")
    print(f"Metadata: {metadata_path}")


if __name__ == "__main__":
    main()
