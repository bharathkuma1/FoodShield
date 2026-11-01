"""Profile candidate M0–M4 calendar eligibility using versioned shared sources.

This bounded standard-library profile mirrors the Week 3 past-only feature
completeness rules. It reports eligible dates, not target labels or model scores.
"""

from __future__ import annotations

import csv
import hashlib
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PRICE_FILE = ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv"
QCL_FILE = ROOT / "data/raw/faostat_ukraine_wheat_production_2026-11.csv"
TCL_FILE = ROOT / "data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv"
WEATHER_FILE = ROOT / "data/raw/nasa_power_ukraine_central_daily_2001_2022.json"
OUTPUT = ROOT / "data/processed/week4_ukraine_m0_m4_eligibility.csv"
LAG_YEARS = 2
EVENTS = {
    "2007_2008": (2007, 1, 2008, 12),
    "2010_2011": (2010, 7, 2011, 6),
    "2022": (2022, 3, 2022, 12),
}
MONTHS = {
    name: number
    for number, name in enumerate(
        ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"),
        start=1,
    )
}


def month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def annual_wide(path: Path, mapping: dict[str, str]) -> dict[int, dict[str, float | None]]:
    by_year: dict[int, dict[str, float | None]] = {}
    for row in read_csv(path):
        year = int(row["Year"])
        target = mapping.get(row["Element"])
        if target is None:
            continue
        raw = row["Value"].strip()
        by_year.setdefault(year, {})[target] = float(raw) if raw else None
    return by_year


def latest_available(source: dict[int, dict[str, float | None]], forecast_year: int) -> dict[str, float | None]:
    eligible_years = [year for year in source if year <= forecast_year - LAG_YEARS]
    return source[max(eligible_years)] if eligible_years else {}


def z_available(values: list[float | None], index: int) -> bool:
    if index < 12 or values[index] is None:
        return False
    previous = values[index - 12:index]
    if any(value is None for value in previous):
        return False
    return statistics.stdev(previous) > 0


def monthly_weather() -> dict[str, dict[str, float]]:
    document = json.loads(WEATHER_FILE.read_text(encoding="utf-8"))
    parameters = document["properties"]["parameter"]
    daily: dict[str, dict[str, list[float]]] = {}
    for parameter, observations in parameters.items():
        for date, value in observations.items():
            if float(value) == -999.0:
                continue
            key = f"{date[:4]}-{date[4:6]}"
            daily.setdefault(key, {}).setdefault(parameter, []).append(float(value))
    result: dict[str, dict[str, float]] = {}
    for key, values in daily.items():
        result[key] = {
            "precipitation_sum_mm": sum(values["PRECTOTCORR"]),
            "t2m_max_mean_c": statistics.fmean(values["T2M_MAX"]),
            "t2m_min_mean_c": statistics.fmean(values["T2M_MIN"]),
        }
    return result


def main() -> None:
    prices = read_csv(PRICE_FILE)
    months = [month_key(int(row["year"]), int(row["month"])) for row in prices]
    if len(months) != len(set(months)) or months != sorted(months):
        raise ValueError("Candidate real-price input must have unique, ordered months")

    production = annual_wide(
        QCL_FILE,
        {"Production": "production_t", "Yield": "yield_kg_ha", "Area harvested": "area_harvested_ha"},
    )
    trade = annual_wide(
        TCL_FILE,
        {
            "Import quantity": "import_quantity_t", "Export quantity": "export_quantity_t",
            "Import value": "import_value_1000_usd", "Export value": "export_value_1000_usd",
        },
    )
    weather = monthly_weather()

    signal_series: dict[str, list[float | None]] = {
        field: [] for field in (
            "price_real", "precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c",
            "production_t", "yield_kg_ha", "area_harvested_ha", "import_quantity_t",
            "export_quantity_t", "import_value_1000_usd", "export_value_1000_usd",
        )
    }
    record_values: list[dict[str, Any]] = []
    for row, key in zip(prices, months):
        year, month = map(int, key.split("-"))
        wx = weather.get(key, {})
        prod = latest_available(production, year)
        trd = latest_available(trade, year)
        values: dict[str, float | None] = {
            "price_nominal": float(row["nominal_price_lcu_per_tonne"]),
            "price_real": float(row["real_price_dec_2000_lcu_per_tonne"]),
            **wx,
            **prod,
            **trd,
        }
        for field in signal_series:
            signal_series[field].append(values.get(field))
        record_values.append({"month": key, "year": year, "values": values})

    price_z = [z_available(signal_series["price_real"], i) for i in range(len(months))]
    weather_z = {
        field: [z_available(signal_series[field], i) for i in range(len(months))]
        for field in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")
    }
    production_z = {
        field: [z_available(signal_series[field], i) for i in range(len(months))]
        for field in ("production_t", "yield_kg_ha", "area_harvested_ha")
    }
    trade_z = {
        field: [z_available(signal_series[field], i) for i in range(len(months))]
        for field in ("import_quantity_t", "export_quantity_t", "import_value_1000_usd", "export_value_1000_usd")
    }

    output_rows = []
    for i, item in enumerate(record_values):
        v = item["values"]
        core_m0 = v.get("price_nominal") is not None and v.get("price_real") is not None
        core_m1 = core_m0 and all(v.get(f) is not None for f in weather_z)
        core_m2 = core_m1 and all(v.get(f) is not None for f in production_z)
        core_m3 = core_m2 and all(v.get(f) is not None for f in trade_z)
        feature_m0 = core_m0 and price_z[i]
        feature_m1 = feature_m0 and all(series[i] for series in weather_z.values())
        feature_m2 = feature_m1 and all(series[i] for series in production_z.values())
        feature_m3 = feature_m2 and all(series[i] for series in trade_z.values())
        feature_m4 = feature_m3 and price_z[i] and weather_z["precipitation_sum_mm"][i]
        origin_year, origin_month = map(int, item["month"].split("-"))
        horizons = {}
        for horizon in (1, 3, 6):
            future = []
            fy, fm = origin_year, origin_month
            for _ in range(horizon):
                fm += 1
                if fm == 13:
                    fy, fm = fy + 1, 1
                future.append(month_key(fy, fm))
            horizons[f"outcomes_{horizon}m_available"] = all(k in months for k in future)
        event_tags = []
        for name, (sy, sm, ey, em) in EVENTS.items():
            if (sy, sm) <= (origin_year, origin_month) <= (ey, em):
                event_tags.append(name)
        output_rows.append({
            "month": item["month"],
            "M0_core_complete": int(core_m0), "M0_features_complete": int(feature_m0),
            "M1_core_complete": int(core_m1), "M1_features_complete": int(feature_m1),
            "M2_core_complete": int(core_m2), "M2_features_complete": int(feature_m2),
            "M3_core_complete": int(core_m3), "M3_features_complete": int(feature_m3),
            "M4_core_complete": int(core_m3), "M4_features_complete": int(feature_m4),
            **horizons,
            "proposal_event_context": ";".join(event_tags),
        })

    fields = list(output_rows[0])
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(output_rows)

    stage_summary = {}
    for stage in ("M0", "M1", "M2", "M3", "M4"):
        dates = [r["month"] for r in output_rows if r[f"{stage}_features_complete"]]
        stage_summary[stage] = {"eligible_origins": len(dates), "first": dates[0] if dates else None, "last": dates[-1] if dates else None}
    horizon_summary = {
        str(h): sum(bool(row[f"outcomes_{h}m_available"]) and bool(row["M4_features_complete"]) for row in output_rows)
        for h in (1, 3, 6)
    }
    sidecar = {
        "purpose": "Source-coverage and feature-completeness profile for the agreed FAOSTAT farm-gate price candidate; no outcome labels or model performance.",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "output_file": OUTPUT.relative_to(ROOT).as_posix(),
        "output_sha256": sha256(OUTPUT),
        "records": len(output_rows),
        "candidate_price_month_range": [months[0], months[-1]],
        "feature_rules": "Mirrors src/foodshield/features/week3.py required M0–M4 predictors, trailing 12-observation past-only anomalies, and M4 price × precipitation anomaly; annual QCL/TCL values use the documented provisional 2-year availability lag.",
        "sources": {
            PRICE_FILE.relative_to(ROOT).as_posix(): sha256(PRICE_FILE),
            QCL_FILE.relative_to(ROOT).as_posix(): sha256(QCL_FILE),
            TCL_FILE.relative_to(ROOT).as_posix(): sha256(TCL_FILE),
            WEATHER_FILE.relative_to(ROOT).as_posix(): sha256(WEATHER_FILE),
        },
        "spatial_limit": "Weather is one NASA POWER point at 49 N, 32 E; its M1–M4 completeness does not establish nationally representative wheat weather.",
        "annual_availability_limit": "The configured two-year QCL/TCL lag is a proxy, not verified release-vintage evidence.",
        "stage_feature_summary": stage_summary,
        "M4_origins_with_future_price_observation_by_horizon_months": horizon_summary,
        "proposal_contexts": "The event tag is contextual only; not a stress label. Target formula, horizon and test split remain subject to approval.",
    }
    sidecar_path = OUTPUT.with_suffix(OUTPUT.suffix + ".metadata.json")
    sidecar_path.write_text(json.dumps(sidecar, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"stage_feature_summary": stage_summary, "M4_origins_with_future_price_observation_by_horizon_months": horizon_summary, "sidecar": str(sidecar_path)}, indent=2))


if __name__ == "__main__":
    main()
