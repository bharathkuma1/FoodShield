"""Generate a synthetic 2021-2022 test window and score it with the frozen M0 model.

Purpose: show end to end how the Week 4 M0 baseline behaves on a test set
(features -> score -> alert -> metrics -> warning lead time) without opening
the real held-out 2021-2022 labels, which stay untouched until the team locks
the protocol.

The synthetic set mirrors the real test window's structure: same calendar
months, same eligible M4 origins, same feature definitions, same >=10% / 3-month
label rule and the same frozen model and alert cutoff. Prices are simulated by
a calendar-aligned half-year block bootstrap of 2011-2020 real-price returns,
starting from the real December 2020 price. No 2021-2023 price is read.

Every generated row is marked is_synthetic=true. Results are a demonstration,
not FoodShield performance evidence (see docs/SYNTHETIC_DATA.md).

    python scripts/generate_week4_synthetic_showcase.py
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from foodshield.evaluation.week4 import (  # noqa: E402
    confusion, episode_lead_times, future_rise_label, month_key, month_number, price_features,
    read_eligible, read_real_prices, roc_auc, summarise,
)
from foodshield.models.m0 import M0_FEATURES  # noqa: E402
from run_week4_rolling_origin import make_spark  # noqa: E402
from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "synthetic_week4_showcase.local.json")
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    week4 = json.loads((ROOT / cfg["week4_config"]).read_text(encoding="utf-8"))
    target, split = week4["target"], week4["candidate_split"]
    horizon, rise = int(target["forecast_horizon_months"]), float(target["minimum_real_price_rise_pct"])
    profile = json.loads((ROOT / cfg["frozen_model"]["profile"]).read_text(encoding="utf-8"))
    cutoff = float(profile["validation_alert_threshold_selection"]["threshold"])

    real = read_real_prices(ROOT / week4["price_csv"])
    history_end = cfg["anchor"]["real_history_end"]
    if month_number(history_end) >= month_number(split["test_start"]):
        raise RuntimeError("Holdout guard: anchor history must end before the test period")
    # Only pre-test real prices are ever used below.
    pre_test = {k: v for k, v in real.items() if month_number(k) <= month_number(history_end)}
    history = {k: v for k, v in pre_test.items()
               if month_number(cfg["anchor"]["real_history_start"]) <= month_number(k)}

    pool_h1, pool_h2 = [], []
    for year in cfg["bootstrap"]["pool_years"]:
        returns = []
        for m in range(1, 13):
            key, prev = f"{year}-{m:02d}", month_key(month_number(f"{year}-{m:02d}") - 1)
            returns.append(math.log(pre_test[key] / pre_test[prev]))
        pool_h1.append((year, returns[:6]))
        pool_h2.append((year, returns[6:]))

    first_syn, last_syn = cfg["synthetic_months"]
    syn_months = [month_key(n) for n in range(month_number(first_syn), month_number(last_syn) + 1)]
    last_origin = month_key(month_number(last_syn) - horizon)
    eligible = [o for o in read_eligible(ROOT / week4["eligibility_csv"])
                if month_number(split["test_start"]) <= month_number(o) <= month_number(last_origin)]

    rng = random.Random(int(cfg["seed"]))
    scenarios = []
    for sid in range(int(cfg["scenarios"])):
        path = dict(history)
        price, drawn, i = history[history_end], [], 0
        while i < len(syn_months):
            month = int(syn_months[i][5:7])
            year, block = rng.choice(pool_h1 if month <= 6 else pool_h2)
            drawn.append(f"{'H1' if month <= 6 else 'H2'}-{year}")
            start = (month - 1) % 6
            for r in block[start:]:
                if i >= len(syn_months):
                    break
                price *= math.exp(r)
                path[syn_months[i]] = price
                i += 1
        rows = []
        for o in eligible:
            mom, z = price_features(path, o)
            rows.append({"origin": o, "label": future_rise_label(path, o, horizon, rise),
                         "price_real_mom_change": mom, "price_real_anomaly_z_12m": z})
        scenarios.append({"scenario_id": sid, "blocks": drawn, "path": path, "rows": rows})

    spark = make_spark()
    try:
        model = PipelineModel.load(str(ROOT / cfg["frozen_model"]["model_path"]))
        records = [(s["scenario_id"], r["origin"], float(r["price_real_mom_change"]), float(r["price_real_anomaly_z_12m"]))
                   for s in scenarios for r in s["rows"]]
        frame = spark.createDataFrame(records, ["scenario_id", "origin", *M0_FEATURES])
        scored = {(r["scenario_id"], r["origin"]): float(r["score"]) for r in model.transform(frame)
                  .select("scenario_id", "origin", vector_to_array("m0_probability_vector")[1].alias("score")).collect()}
    finally:
        spark.stop()

    metrics = []
    for s in scenarios:
        origins = [r["origin"] for r in s["rows"]]
        y = [r["label"] for r in s["rows"]]
        sc = [scored[(s["scenario_id"], o)] for o in origins]
        alerts = [int(v >= cutoff) for v in sc]
        for r, v, a in zip(s["rows"], sc, alerts):
            r["m0_score"], r["m0_alert"] = v, a
        c = confusion(sc, y, cutoff)
        eps = episode_lead_times(s["path"], origins, y, alerts, horizon, rise)
        mom = confusion([float(r["price_real_mom_change"] > 0) for r in s["rows"]], y, 0.5)
        metrics.append({
            "scenario_id": s["scenario_id"], "blocks": "|".join(s["blocks"]), "origins": len(y),
            "positives": sum(y), "alerts": c["alerts"], "true_positive": c["true_positive"],
            "false_positive": c["false_positive"], "false_negative": c["false_negative"],
            "precision": c["precision"], "recall": c["recall"], "f1": c["f1"],
            "false_positive_rate": c["false_positive_rate"], "auc": roc_auc(sc, y),
            "episodes": len(eps), "episodes_detected": sum(e["detected"] for e in eps),
            "median_lead_time_months": (sorted(e["lead_time_months"] for e in eps if e["lead_time_months"] is not None) or [None])[
                len([e for e in eps if e["lead_time_months"] is not None]) // 2] if eps else None,
            "momentum_rule_f1": mom["f1"],
        })

    # Typical (not best) showcase: median positive count, at least two stress episodes,
    # and F1 closest to the median F1 across all scenarios.
    median_pos = sorted(m["positives"] for m in metrics)[len(metrics) // 2]
    median_f1 = summarise([m["f1"] for m in metrics])["median"]
    showcase = min((m for m in metrics if m["episodes"] >= 2 and m["f1"] is not None),
                   key=lambda m: (abs(m["positives"] - median_pos), abs(m["f1"] - median_f1), m["scenario_id"]))
    chosen = scenarios[showcase["scenario_id"]]

    out = ROOT / cfg["output_dir"]
    out.mkdir(parents=True, exist_ok=True)
    showcase_csv = out / "ukraine_wheat_test_window_synthetic_showcase.csv"
    by_origin = {r["origin"]: r for r in chosen["rows"]}
    fields = ["month", "row_role", "is_synthetic", "price_real_dec_2000_lcu_per_tonne", "eligible_test_origin",
              "price_real_mom_change", "price_real_anomaly_z_12m", "future_stress_label", "m0_score", "m0_alert",
              "alert_cutoff", "scenario_id"]
    with showcase_csv.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for month in sorted(chosen["path"], key=month_number):
            r = by_origin.get(month)
            synthetic = month_number(month) > month_number(history_end)
            writer.writerow({
                "month": month, "row_role": "synthetic_test_window" if synthetic else "real_pre_test_history",
                "is_synthetic": str(synthetic).lower(),
                "price_real_dec_2000_lcu_per_tonne": f"{chosen['path'][month]:.4f}",
                "eligible_test_origin": int(r is not None),
                "price_real_mom_change": f"{r['price_real_mom_change']:.6f}" if r else "",
                "price_real_anomaly_z_12m": f"{r['price_real_anomaly_z_12m']:.6f}" if r else "",
                "future_stress_label": r["label"] if r else "",
                "m0_score": f"{r['m0_score']:.6f}" if r else "",
                "m0_alert": r["m0_alert"] if r else "",
                "alert_cutoff": f"{cutoff:.6f}" if r else "",
                "scenario_id": chosen["scenario_id"],
            })
    metrics_csv = out / "scenario_metrics.csv"
    with metrics_csv.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(metrics[0]), lineterminator="\n")
        writer.writeheader()
        for m in metrics:
            writer.writerow({k: (f"{v:.6f}" if isinstance(v, float) else v) for k, v in m.items()})

    eps = episode_lead_times(chosen["path"], [r["origin"] for r in chosen["rows"]], [r["label"] for r in chosen["rows"]],
                             [r["m0_alert"] for r in chosen["rows"]], horizon, rise)
    metadata = {
        "is_synthetic": True,
        "purpose": cfg["purpose"],
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "recipe": {"config": str(args.config.resolve().relative_to(ROOT)).replace("\\", "/"), "seed": cfg["seed"],
                   "scenarios": cfg["scenarios"], "bootstrap": cfg["bootstrap"], "anchor": cfg["anchor"]},
        "mirrors_real_test_window": {
            "test_origins": eligible, "origin_count": len(eligible),
            "label_rule": target["label_rule_id"], "features": M0_FEATURES,
            "frozen_model": cfg["frozen_model"]["model_path"], "alert_cutoff": cutoff,
        },
        "holdout_guard": "No real price after 2020-12 is read; the real 2021-2022 test labels remain unopened.",
        "inputs": {week4["price_csv"]: sha256(ROOT / week4["price_csv"]),
                   week4["eligibility_csv"]: sha256(ROOT / week4["eligibility_csv"])},
        "showcase_scenario": {**showcase, "episodes_detail": eps,
                              "selection_rule": "Typical, not best: positive-label count closest to the median, at least two stress episodes, then F1 closest to the median F1 across all scenarios; lowest scenario id breaks ties."},
        "distribution_across_scenarios": {
            name: summarise([m[name] for m in metrics])
            for name in ("positives", "precision", "recall", "f1", "false_positive_rate", "auc", "momentum_rule_f1")
        },
        "episode_detection_across_scenarios": {
            "episodes": sum(m["episodes"] for m in metrics),
            "detected": sum(m["episodes_detected"] for m in metrics),
        },
        "outputs": {p.name: sha256(p) for p in (showcase_csv, metrics_csv)},
        "do_not_use_for": ["reported model performance", "event labels", "source coverage", "final evaluation"],
        "caveat": "Bootstrap returns come from 2011-2020, which overlaps the model's 2011-2017 training years, so scores on these paths are optimistic. The real 2021-2022 test, with the Black Sea war shock, may look very different.",
    }
    (out / "ukraine_wheat_test_window_synthetic_showcase.metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"showcase": showcase, "distribution": metadata["distribution_across_scenarios"],
                      "episodes": metadata["episode_detection_across_scenarios"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
