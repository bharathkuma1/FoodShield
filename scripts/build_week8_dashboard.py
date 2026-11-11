"""Week 8: build the results dashboard as one self-contained HTML page (no internet, no libraries).

Reads the committed result files and embeds them as JSON. The page has five tabs:

- Overview: headline numbers, price history with alerts, M0-M4 results, episodes.
- Explorer: change the compared stages, period, stress definition, annual data delay and
  alert cutoff; results are recomputed in the browser from the saved month-by-month scores.
  The defaults reproduce the reported numbers exactly; anything else is labelled "exploring".
- What if?: nudge one month's inputs and see how the frozen one-time-test models react.
- Streaming: replay a saved laptop run month by month, or (when served by
  scripts/serve_dashboard.py) follow a running replay live, with a pop-up for every alert.
- Scale: batch scaling and the CSV-vs-Parquet comparison.

Nothing is retrained in the page. Charts are inline SVG with hover tooltips and table views.

    python scripts/build_week8_dashboard.py      ->  docs/dashboard/index.html
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.evaluation.week4 import future_rise_label, read_real_prices  # noqa: E402

P = ROOT / "data" / "processed"
OUT = ROOT / "docs" / "dashboard" / "index.html"
STAGES = ["M0", "M0+season", "M1", "M2", "M3", "M4"]
# Label sensitivity runs (backtest only, 2-year annual delay).
LABEL_RUNS = {"r5h3": ("rise5_h3", 5, 3), "r15h3": ("rise15_h3", 15, 3), "r10h1": ("rise10_h1", 10, 1), "r10h6": ("rise10_h6", 10, 6)}
REPLAY_RUNS = [("laptop_demo_1000", "Laptop demo, 1,000 events/s"), ("laptop_paced_100", "Laptop, 100 events/s"),
               ("laptop_paced_250", "Laptop, 250 events/s"), ("laptop_full_max", "Laptop, full speed"),
               ("laptop_restart", "Laptop, consumer killed and restarted")]


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def backtest_setting(path: Path, rise: float, horizon: int, lag: int) -> dict:
    stages: dict[str, list] = {s: [] for s in STAGES}
    with path.open(encoding="utf-8") as stream:
        for r in csv.DictReader(stream):
            if r["variant"] == "corrected":
                stages[r["stage"]].append([r["origin"], int(r["label"]), round(float(r["score"]), 6),
                                           round(float(r["cutoff"]), 6), int(r["alert"])])
    for rows in stages.values():
        rows.sort()
    return {"period": "backtest", "rise": rise, "horizon": horizon, "lag": lag, "stages": stages}


def test_setting(run: dict) -> dict:
    return {"period": "test", "rise": 10, "horizon": 3, "lag": run["annual_lag"],
            "stages": {s: [[m["origin"], m["label"], round(m["score"], 6), round(st["cutoff_from_validation"], 6), int(m["alert"])]
                           for m in st["monthly"]] for s, st in run["stages"].items()}}


def explorer_data(prices: dict[str, float], test: dict) -> dict:
    runs = {r["annual_lag"]: r for r in test["runs"] if r["variant"] == "corrected"}
    settings = {
        "backtest|r10h3|2": backtest_setting(P / "week5_ablation_lag2" / "backtest_predictions.csv", 10, 3, 2),
        "backtest|r10h3|3": backtest_setting(P / "week5_ablation_lag3" / "backtest_predictions.csv", 10, 3, 3),
        "test|r10h3|2": test_setting(runs[2]),
        "test|r10h3|3": test_setting(runs[3]),
    }
    for key, (folder, rise, horizon) in LABEL_RUNS.items():
        path = P / "week7_label_sensitivity" / folder / "backtest_predictions.csv"
        if path.exists():
            settings[f"backtest|{key}|2"] = backtest_setting(path, rise, horizon, 2)
    return {"prices": prices, "settings": settings}


def whatif_data(prices: dict[str, float], primary: dict) -> dict:
    path = P / "week8_dashboard" / "monthly_features.csv"
    if not path.exists():
        return None
    models = {s: {"coefficients": st["coefficients"], "intercept": st["intercept"], "cutoff": st["cutoff_from_validation"]}
              for s, st in primary["stages"].items()}
    needed = sorted({f for m in models.values() for f in m["coefficients"]})
    features = {}
    with path.open(encoding="utf-8") as stream:
        for r in csv.DictReader(stream):
            if all(r.get(f) not in (None, "") for f in needed):
                features[r["origin"]] = {f: float(r[f]) for f in needed}
    outcome = {m: future_rise_label(prices, m, 3, 10) for m in features}
    return {"models": models, "features": features, "outcome": outcome}


def replay_data() -> list[dict]:
    runs = []
    for name, label in REPLAY_RUNS:
        folder = P / "week6_streaming" / name
        report = load_json(folder / "week6_report.json")
        if not report or not (folder / "alerts.csv").exists():
            continue
        with (folder / "alerts.csv").open(encoding="utf-8") as stream:
            alerts = [[r["observation_month"], r["model_stage"], round(float(r["score"]), 4), round(float(r["cutoff"]), 4),
                       int(r["alert"]), None if r["latency_s"] == "" else round(float(r["latency_s"]), 2), r["reason_codes"]]
                      for r in csv.DictReader(stream)]
        s = report["settings"]
        runs.append({"id": name, "label": label, "machine": f"{report['machine']['os'].split('-')[0]}, {report['machine']['cpu_count']} cores",
                     "rate": s["rate_events_per_s"], "measure_from": (s.get("measure_from") or "")[:7] or None,
                     "p50": report["alert_latency"].get("p50_s"), "p95": report["alert_latency"].get("p95_s"),
                     "checks": report["all_checks_passed"], "restarts": len(report["consumer"]["restarts"]), "alerts": alerts})
    return runs


def build_data() -> dict:
    prices = read_real_prices(P / "week4_ukraine_wheat_farmgate_realprice_candidate.csv")
    test = load_json(P / "final_test" / "final_test_report.json")
    primary = next(r for r in test["runs"] if r["variant"] == "corrected" and r["annual_lag"] == 2)
    labels = {m: future_rise_label(prices, m, 3, 10, last_allowed="2020-12") for m in prices}
    for m in primary["stages"]["M0"]["monthly"]:
        labels[m["origin"]] = m["label"]
    alerts = {"M0": {}, "M4": {}}
    with (P / "week5_ablation_lag2" / "backtest_predictions.csv").open(encoding="utf-8") as stream:
        for r in csv.DictReader(stream):
            if r["variant"] == "corrected" and r["stage"] in alerts:
                alerts[r["stage"]][r["origin"]] = {"alert": int(r["alert"]), "score": float(r["score"]), "window": "backtest"}
    for s in alerts:
        for m in primary["stages"][s]["monthly"]:
            alerts[s][m["origin"]] = {"alert": m["alert"], "score": m["score"], "window": "test"}
    timeline = [{"month": m, "price": round(prices[m], 1), "label": labels.get(m),
                 "m0": alerts["M0"].get(m), "m4": alerts["M4"].get(m)} for m in sorted(prices) if m <= "2022-12"]

    evaluation = load_json(P / "week7_model_evaluation" / "summary.json")
    episodes = []
    path = P / "week7_model_evaluation" / "episodes.csv"
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            episodes = [r for r in csv.DictReader(stream) if r["stage"] in ("M0", "M4")]
    streaming = []
    order = ["full_max", "restart", "paced_250", "paced_100", "demo_1000"]

    def sort_key(n: str):
        base = n.removeprefix("laptop_")
        group = ["s7_rate", "s7_part", "s7_trig"]
        g = next((i for i, x in enumerate(group) if base.startswith(x)), -1)
        num = float(re.sub(r"[^0-9.]", "", base[len(group[g]) + 1:].replace("_", ".")) or 0) if g >= 0 else 0
        return (not n.startswith("laptop"), order.index(base) if base in order else 10 + g, num)
    names = sorted((d.name for d in (P / "week6_streaming").iterdir() if (d / "week6_report.json").exists()), key=sort_key)
    for name in names:
        r = load_json(P / "week6_streaming" / name / "week6_report.json")
        if r:
            w = r["producer"].get("measured_window") or {}
            streaming.append({"run": name, "machine": f"{r['machine']['os'].split('-')[0]}, {r['machine']['cpu_count']} cores",
                              "rate": r["settings"]["rate_events_per_s"], "messages": r["producer"]["messages_sent"],
                              "achieved": w.get("achieved_events_per_s"), "consumer": w.get("consumer_events_per_s"),
                              "p50": r["alert_latency"].get("p50_s"), "p95": r["alert_latency"].get("p95_s"),
                              "checks": r["all_checks_passed"], "restarts": len(r["consumer"]["restarts"]),
                              "partitions": r["settings"]["topics"]["observations"]["partitions"],
                              "trigger": r["settings"]["trigger_seconds"]})
    scaling = load_json(P / "week7_batch_scaling" / "summary.json")
    if scaling:
        scaling.pop("generated_utc", None)  # the page shows results, not when they were produced
    bulk = load_json(P / "d9a_faostat_bulk" / "bulk_profile.json")
    grid = load_json(P / "d9b_grid_weather" / "grid_weather_profile.json")
    quality = load_json(P / "week7_data_quality" / "report.json")
    d9 = None
    if bulk:
        d9 = {"domains": {k: {"rows": v["csv_rows"], "csv_mb": round(v.get("csv_bytes", 0) / 1e6, 1) if v.get("csv_bytes") else None,
                              "parquet_mb": round(v.get("parquet_bytes", 0) / 1e6, 1) if v.get("parquet_bytes") else None,
                              "query_csv_s": v.get("query_csv_seconds"), "query_parquet_s": v.get("query_parquet_seconds")}
                          for k, v in bulk["domains"].items()},
              "grid_rows": grid["daily_rows"] if grid else None, "basis": "single D9-A run, query only"}
        fao = [c for c in (scaling or {}).get("configs", []) if c["series"] == "faostat_query"]
        if fao:
            for c in fao:
                d9["domains"][c["domain"]]["query_" + c["format"] + "_s"] = c["median_s"]
            d9["basis"] = "Week 7: median of 3 timed runs after a warm-up, including opening the dataset"
    return {"timeline": timeline,
            "headline": evaluation["headline"] if evaluation else [], "episodes": episodes, "streaming": streaming,
            "scaling": scaling, "d9": d9, "quality": quality and quality["ablation_support"],
            "test_window": primary["test_window"], "explorer": explorer_data(prices, test),
            "whatif": whatif_data(prices, primary), "replay": replay_data()}


TEMPLATE = (Path(__file__).resolve().parent / "dashboard_template.html").read_text(encoding="utf-8")


def main() -> int:
    data = build_data()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    OUT.write_text(TEMPLATE.replace("__DATA__", payload), encoding="utf-8")
    print(f"Wrote {OUT} ({OUT.stat().st_size / 1e3:.0f} kB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
