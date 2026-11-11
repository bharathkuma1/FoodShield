"""Week 7 data-quality report: source coverage, feature completeness, joins, as-of checks, ablation support.

Plain Python over the committed inputs and results (no Spark), so it runs anywhere in seconds.
The as-of and streaming-equals-batch checks are the integration tests in tests/test_integration.py;
this script records their latest result from data/processed/week7_data_quality/integration_tests.json.

    python scripts/run_week7_data_quality.py

Output: data/processed/week7_data_quality/report.json (+ feature_completeness_by_year.csv)
"""

from __future__ import annotations

import collections
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.evaluation.week4 import future_rise_label, month_number  # noqa: E402
from foodshield.models.staged import FEATURE_VARIANTS  # noqa: E402
from foodshield.streaming.monitor import OnlineMonitor  # noqa: E402
from foodshield.streaming.records import annual_records  # noqa: E402

OUT = ROOT / "data" / "processed" / "week7_data_quality"
PRICE = ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv"
WEATHER = ROOT / "data/processed/week4_ukraine_weather_wheat_area_weighted_monthly.csv"
QCL = ROOT / "data/raw/faostat_ukraine_wheat_production_2026-11.csv"
TCL = ROOT / "data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv"
SSSU = ROOT / "data/raw/sssu_ukraine_wheat_harvested_area_by_oblast_2026-11.csv"
GRID_MANIFEST = ROOT / "data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.manifest.json"


def rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    price, weather, qcl, tcl, sssu = rows(PRICE), rows(WEATHER), rows(QCL), rows(TCL), rows(SSSU)
    grid = json.loads(GRID_MANIFEST.read_text(encoding="utf-8")) if GRID_MANIFEST.exists() else {}

    def months(rs, key):
        ks = sorted(key(r) for r in rs)
        return {"first": ks[0], "last": ks[-1], "count": len(ks), "unique": len(set(ks))}

    sources = {
        "price_real_monthly": {"file": str(PRICE.relative_to(ROOT)), **months(price, lambda r: f"{r['year']}-{int(r['month']):02d}"),
                               "missing_values": sum(1 for r in price if not r["real_price_dec_2000_lcu_per_tonne"]),
                               "producer_price_flags": dict(collections.Counter(r["producer_price_flag"] for r in price)),
                               "cpi_release": sorted({r["cpi_updated"] for r in price})},
        "weather_24_point_monthly": {"file": str(WEATHER.relative_to(ROOT)), **months(weather, lambda r: r["month"]),
                                     "missing_values": sum(1 for r in weather for k in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c") if r[k] == ""),
                                     "months_with_weight_covered_below_1": [r["month"] for r in weather if float(r["weight_covered"]) < 0.999]},
        "weather_grid_daily": {"rows": grid.get("rows"), "cells": grid.get("cells"), "blank_values": grid.get("blank_values")},
        "faostat_qcl_extract": {"rows": len(qcl), "years": [min(int(r["Year"]) for r in qcl), max(int(r["Year"]) for r in qcl)],
                                "flags_since_2005": dict(collections.Counter(r["Flag"] for r in qcl if int(r["Year"]) >= 2005)),
                                "missing_values": sum(1 for r in qcl if r["Value"] == "")},
        "faostat_tcl_extract": {"rows": len(tcl), "years": [min(int(r["Year"]) for r in tcl), max(int(r["Year"]) for r in tcl)],
                                "flags_since_2005": dict(collections.Counter(r["Flag"] for r in tcl if int(r["Year"]) >= 2005)),
                                "missing_values": sum(1 for r in tcl if r["Value"] == "")},
        "sssu_wheat_area_by_oblast": {"rows": len(sssu), "oblasts": len({r["region_katottg"] for r in sssu}),
                                      "years": sorted({int(r["year"]) for r in sssu}),
                                      "obs_flags": dict(collections.Counter(r["obs_flag"] or "(none)" for r in sssu))},
    }

    # Feature completeness: the streaming monitor computes the same 13 features as the Spark batch
    # pipeline (tests/test_integration.py), so it is used here without Spark.
    m = OnlineMonitor({"x": 1.0}, {}, 2)
    for r in price:
        m.prices[f"{r['year']}-{int(r['month']):02d}"] = float(r["real_price_dec_2000_lcu_per_tonne"])
    for r in weather:
        m.weather[r["month"]] = [float(r[k]) for k in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")]
    for _, _, r in annual_records(QCL, TCL, 2):
        m.add_annual(r["annual_field"], r["data_year"], r["value"])
    stages = FEATURE_VARIANTS["corrected"]
    feature_names = stages["M4"]
    panel_months = sorted(k for k in m.prices if k in m.weather)
    feats = {k: m.features(k) for k in panel_months}
    by_year = []
    for year in sorted({k[:4] for k in panel_months}):
        ks = [k for k in panel_months if k.startswith(year)]
        row = {"year": year, "months": len(ks)}
        for f in feature_names:
            row[f] = sum(feats[k][f] is not None for k in ks)
        for s in ("M0", "M1", "M2", "M3", "M4"):
            row[f"{s}_complete"] = sum(all(feats[k][f] is not None for f in stages[s]) for k in ks)
        by_year.append(row)
    with (OUT / "feature_completeness_by_year.csv").open("w", encoding="utf-8", newline="") as stream:
        w = csv.DictWriter(stream, fieldnames=list(by_year[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(by_year)
    first_missing = {f: [k for k in panel_months if feats[k][f] is None] for f in feature_names}

    # Ablation support: the common origin set and its labels (pre-test labels only).
    labels = {k: future_rise_label(m.prices, k, 3, 10, last_allowed="2020-12") for k in panel_months}
    common = [k for k in panel_months if labels[k] is not None and all(feats[k][f] is not None for f in feature_names)
              and month_number(k) >= month_number("2011-01")]
    split = {"train 2011-2017 (purged)": [k for k in common if month_number(k) + 3 <= month_number("2017-12")],
             "validation 2018-2020 (purged)": [k for k in common if month_number(k) >= month_number("2018-01")
                                               and month_number(k) + 3 <= month_number("2020-12")],
             "backtest 2014-2020": [k for k in common if "2014" <= k[:4] <= "2020"]}
    support = {"common_origins": len(common), "first": common[0], "last": common[-1],
               "splits": {n: {"origins": len(ks), "stress_months": sum(labels[k] for k in ks)} for n, ks in split.items()},
               "stress_months_by_year": dict(collections.Counter(k[:4] for k in common if labels[k] == 1))}

    # As-of rules recorded in the data (D6 and the replay rules)
    annual_rule_ok = all(r["available_at"][:4] == str(r["data_year"] + 2) for _, _, r in annual_records(QCL, TCL, 2))
    predictions = rows(ROOT / "data/processed/week5_ablation_lag2/backtest_predictions.csv")
    pretest_guard = max(r["origin"] for r in predictions) <= "2020-09"
    integration = OUT / "integration_tests.json"  # written after running tests/test_integration.py
    checks = {
        "annual_values_available_two_years_after_data_year": annual_rule_ok,
        "backtest_labels_end_by_2020_12": pretest_guard,
        "common_origin_count_matches_week5": len(common) == 117,
        "integration_tests": json.loads(integration.read_text(encoding="utf-8")) if integration.exists() else "not run",
    }
    report = {"generated_utc": datetime.now(timezone.utc).isoformat(), "sources": sources,
              "feature_missing_months": {f: {"count": len(v), "first": v[:3], "last": v[-3:]} for f, v in first_missing.items()},
              "ablation_support": support, "checks": checks,
              "feature_completeness_file": "feature_completeness_by_year.csv"}
    (OUT / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"sources": {k: {kk: vv for kk, vv in v.items() if kk in ("first", "last", "count", "missing_values", "rows")}
                                  for k, v in sources.items()}, "support": support, "checks": checks,
                      "missing": {f: v["count"] for f, v in report["feature_missing_months"].items()}}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
