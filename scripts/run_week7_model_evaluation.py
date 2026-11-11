"""Week 7 model evaluation: one consolidated view of M0-M4 evidence (no new test-period labels).

Reads results already produced and adds analyses that use only pre-test data:

- headline: backtest 2014-2020 (Week 5) and the one-time 2021-2022 test, every stage,
  with 90% bootstrap intervals (test intervals recomputed from the saved monthly alerts
  with the published method and checked against FINAL_TEST_RESULTS.md);
- episodes: every stress episode, whether each stage caught it, and the lead time;
- threshold trade-off: precision / recall / false-alarm rate across alert cutoffs on the
  pooled out-of-sample backtest scores (2014-2020 only);
- probability quality: Brier score on the backtest;
- robustness: 3-year lag, season in every stage, Week 3 features (backtest and test);
- label sensitivity (D1/D2): backtest at 5% and 15% rises and 1- and 6-month horizons,
  if data/processed/week7_label_sensitivity/<case>/ablation_report.json exist.

    python scripts/run_week7_model_evaluation.py

Outputs: data/processed/week7_model_evaluation/ and docs/figures/week7_model_*.png
"""

from __future__ import annotations

import csv
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.evaluation.week4 import confusion, episode_lead_times, read_real_prices, roc_auc  # noqa: E402

PROCESSED = ROOT / "data" / "processed"
OUT = PROCESSED / "week7_model_evaluation"
FIGURES = ROOT / "docs" / "figures"
STAGES = ["M0", "M0+season", "M1", "M2", "M3", "M4"]
LABELS = {"M0": "M0 price", "M0+season": "Price + season (control)", "M1": "M1 + weather", "M2": "M2 + production",
          "M3": "M3 + trade", "M4": "M4 + interaction"}
SEED, RESAMPLES = 20261107, 2000
SENSITIVITY_CASES = {"rise5_h3": (5, 3), "rise15_h3": (15, 3), "rise10_h1": (10, 1), "rise10_h6": (10, 6)}


def interval(xs: list[float]) -> list[float]:
    xs = sorted(xs)
    return [round(xs[int(0.05 * len(xs))], 3), round(xs[int(0.95 * len(xs)) - 1], 3)]


def bootstrap(by_stage: dict[str, dict[str, tuple[int, int]]]) -> dict[str, dict]:
    """Same method as the Week 5 ablation and the final test: resample months, same draw for every stage."""
    rng = random.Random(SEED)
    months = sorted(by_stage["M0"])
    draws = {s: [] for s in by_stage}
    diffs = {s: [] for s in by_stage}
    for _ in range(RESAMPLES):
        sample = [rng.choice(months) for _ in months]
        f1 = {}
        for s, preds in by_stage.items():
            f1[s] = confusion([float(preds[m][1]) for m in sample], [preds[m][0] for m in sample], 0.5)["f1"] or 0.0
            draws[s].append(f1[s])
        for s in by_stage:
            diffs[s].append(f1[s] - f1["M0"])
    return {s: {"f1_90pct_interval": interval(draws[s]), "f1_minus_M0_90pct_interval": interval(diffs[s])} for s in by_stage}


def brier(scores: list[float], labels: list[int]) -> float:
    return sum((s - y) ** 2 for s, y in zip(scores, labels)) / len(scores)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    prices = read_real_prices(PROCESSED / "week4_ukraine_wheat_farmgate_realprice_candidate.csv")
    lag2 = json.loads((PROCESSED / "week5_ablation_lag2" / "ablation_report.json").read_text(encoding="utf-8"))
    lag3 = json.loads((PROCESSED / "week5_ablation_lag3" / "ablation_report.json").read_text(encoding="utf-8"))
    test = json.loads((PROCESSED / "final_test" / "final_test_report.json").read_text(encoding="utf-8"))
    horizon = int(lag2["target"]["forecast_horizon_months"])
    rise = float(lag2["target"]["minimum_real_price_rise_pct"])

    preds: dict[str, dict[str, list[dict]]] = {}
    with (PROCESSED / "week5_ablation_lag2" / "backtest_predictions.csv").open(encoding="utf-8") as stream:
        for r in csv.DictReader(stream):
            preds.setdefault(r["variant"], {}).setdefault(r["stage"], []).append(
                {"origin": r["origin"], "label": int(r["label"]), "score": float(r["score"]), "alert": int(r["alert"])})

    def run(report, variant, lag):
        return next(r for r in report["runs"] if r["variant"] == variant and r["annual_lag"] == lag)

    # ---- headline
    primary_bt = next(v for v in lag2["variants"] if v["variant"] == "corrected")
    primary_test = run(test, "corrected", 2)
    test_by_stage = {s: {m["origin"]: (m["label"], m["alert"]) for m in primary_test["stages"][s]["monthly"]} for s in STAGES}
    test_ci = bootstrap(test_by_stage)
    published = {"M4": [0.08, 0.31], "M0+season": [-0.62, -0.19], "M1": [-0.14, 0.18], "M2": [-0.14, 0.19], "M3": [-0.12, 0.18]}
    # FINAL_TEST_RESULTS.md intervals were computed with the same method but outside a script; the
    # resampling order differs, so compare within a tolerance and record the largest difference.
    test_ci_max_diff = max(abs(a - b) for s, v in published.items() for a, b in zip(test_ci[s]["f1_minus_M0_90pct_interval"], v))
    test_ci_check = test_ci_max_diff <= 0.025
    headline = []
    for s in STAGES:
        b = primary_bt["stages"][s]["backtest"]
        t = primary_test["stages"][s]
        bt_ci = lag2["uncertainty"]["by_variant"]["corrected"][s]
        bt_preds = preds["corrected"][s]
        headline.append({
            "stage": s, "label": LABELS[s], "features": len(primary_bt["stages"][s]["features"]),
            "backtest_precision": b["precision"], "backtest_recall": b["recall"], "backtest_f1": b["f1"],
            "backtest_f1_ci": bt_ci["f1_90pct_interval"], "backtest_f1_minus_m0_ci": bt_ci["f1_minus_M0_90pct_interval"],
            "backtest_false_alarm_rate": b["false_positive_rate"], "backtest_mean_fold_auc": b["mean_fold_auc"],
            "backtest_pooled_auc": roc_auc([p["score"] for p in bt_preds], [p["label"] for p in bt_preds]),
            "backtest_brier": brier([p["score"] for p in bt_preds], [p["label"] for p in bt_preds]),
            "backtest_episodes": f"{b['episodes_detected']}/{b['episodes']}",
            "test_alerts": t["alerts"], "test_precision": t["precision"], "test_recall": t["recall"], "test_f1": t["f1"],
            "test_f1_ci": test_ci[s]["f1_90pct_interval"], "test_f1_minus_m0_ci": test_ci[s]["f1_minus_M0_90pct_interval"],
            "test_false_alarm_rate": t["false_positive_rate"], "test_auc": t["auc"],
            "test_episodes": f"{sum(e['detected'] for e in t['episodes'])}/{len(t['episodes'])}",
        })
    baselines = {"backtest": primary_bt["baselines_on_backtest_origins"], "test": primary_test["baselines"]}
    base_rate = {"backtest": sum(p["label"] for p in preds["corrected"]["M0"]) / len(preds["corrected"]["M0"]),
                 "test": primary_test["test_stress_months"] / primary_test["test_origins"]}
    for h in headline:  # Brier of always predicting the base rate, for reference
        h["backtest_brier_base_rate"] = base_rate["backtest"] * (1 - base_rate["backtest"])

    # ---- episodes and lead times (backtest recomputed from saved alerts; test from the report)
    episodes = []
    for s in STAGES:
        rows = sorted(preds["corrected"][s], key=lambda p: p["origin"])
        for e in episode_lead_times(prices, [p["origin"] for p in rows], [p["label"] for p in rows],
                                    [p["alert"] for p in rows], horizon, rise):
            episodes.append({"window": "backtest 2014-2020", "stage": s, **e})
        for e in primary_test["stages"][s]["episodes"]:
            episodes.append({"window": "test 2021-2022", "stage": s, **e})
    lead = {}
    for s in STAGES:
        for w in ("backtest 2014-2020", "test 2021-2022"):
            es = [e for e in episodes if e["stage"] == s and e["window"] == w]
            times = [e["lead_time_months"] for e in es if e["lead_time_months"] is not None]
            lead[f"{s}|{w}"] = {"episodes": len(es), "caught": sum(e["detected"] for e in es),
                                "lead_times": times, "mean_lead": round(sum(times) / len(times), 2) if times else None}

    # ---- threshold trade-off on pooled backtest scores (pre-test only)
    tradeoff = {}
    for s in ("M0", "M0+season", "M4"):
        rows = preds["corrected"][s]
        y = [p["label"] for p in rows]
        sc = [p["score"] for p in rows]
        curve = []
        for cut in sorted(set(sc), reverse=True):
            c = confusion(sc, y, cut)
            curve.append({"cutoff": cut, "alert_rate": c["alerts"] / len(y), "precision": c["precision"],
                          "recall": c["recall"], "false_alarm_rate": c["false_positive_rate"], "f1": c["f1"]})
        tradeoff[s] = curve

    # ---- robustness (backtest and test)
    robustness = []
    for variant, title in (("corrected", "Primary: corrected features, 2-year lag"),
                           ("corrected_plus_season", "Season in every stage (D11)"),
                           ("contract_anomaly", "Week 3 features as built")):
        for lag, report in ((2, lag2), (3, lag3)):
            if variant != "corrected" and lag == 3:
                continue
            bt = next(v for v in report["variants"] if v["variant"] == variant)
            tt = run(test, variant, lag)
            name = title if lag == 2 else "3-year annual lag (D6)"
            for s in STAGES:
                robustness.append({"check": name, "stage": s, "backtest_f1": bt["stages"][s]["backtest"]["f1"],
                                   "backtest_auc": bt["stages"][s]["backtest"]["mean_fold_auc"],
                                   "test_f1": tt["stages"][s]["f1"], "test_auc": tt["stages"][s]["auc"]})

    # ---- label sensitivity (D1/D2), backtest only
    sensitivity = []
    sens_root = PROCESSED / "week7_label_sensitivity"
    cases = {"rise10_h3 (primary)": PROCESSED / "week5_ablation_lag2"}
    cases.update({k: sens_root / k for k in SENSITIVITY_CASES})
    for case, folder in cases.items():
        path = folder / "ablation_report.json"
        if not path.exists():
            continue
        rep = json.loads(path.read_text(encoding="utf-8"))
        v = next(x for x in rep["variants"] if x["variant"] == "corrected")
        ci = rep["uncertainty"]["by_variant"]["corrected"]
        for s in STAGES:
            b = v["stages"][s]["backtest"]
            sensitivity.append({"case": case, "rise_pct": rep["target"]["minimum_real_price_rise_pct"],
                                "horizon_months": rep["target"]["forecast_horizon_months"], "stage": s,
                                "origins": b["n"], "stress_months": b["positives"], "episodes": b["episodes"],
                                "f1": b["f1"], "f1_minus_m0_ci": ci[s]["f1_minus_M0_90pct_interval"],
                                "false_alarm_rate": b["false_positive_rate"], "mean_fold_auc": b["mean_fold_auc"],
                                "episodes_caught": b["episodes_detected"]})

    summary = {"inputs": ["data/processed/week5_ablation_lag2", "data/processed/week5_ablation_lag3",
                          "data/processed/final_test/final_test_report.json"],
               "test_intervals_recomputed_match_published_within_0_025": test_ci_check,
               "test_intervals_max_abs_difference_vs_published": round(test_ci_max_diff, 3), "base_rate": base_rate,
               "headline": headline, "baselines": baselines, "lead_times": lead, "robustness": robustness,
               "label_sensitivity": sensitivity, "note": "No test-period label is computed here; test numbers come from the one-time run."}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    for name, rows in (("headline.csv", headline), ("episodes.csv", episodes), ("robustness.csv", robustness),
                       ("label_sensitivity.csv", sensitivity)):
        if rows:
            with (OUT / name).open("w", encoding="utf-8", newline="") as stream:
                w = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
                w.writeheader()
                w.writerows(rows)
    (OUT / "threshold_tradeoff_backtest.json").write_text(json.dumps(tradeoff) + "\n", encoding="utf-8")
    plots(headline, episodes, tradeoff, sensitivity)
    print(json.dumps({"test_intervals_match_published": test_ci_check, "max_diff": round(test_ci_max_diff, 3), "base_rate": base_rate,
                      "lead": {k: v for k, v in lead.items() if k.split("|")[0] in ("M0", "M4")},
                      "sensitivity_cases": sorted({r["case"] for r in sensitivity})}, indent=1))
    return 0


def plots(headline, episodes, tradeoff, sensitivity) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, muted, grid, accent, accent2, surface = "#1f2933", "#7b8794", "#e4e7eb", "#2563eb", "#d97706", "#ffffff"
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": muted, "axes.labelcolor": ink, "xtick.color": ink,
                         "ytick.color": ink, "axes.spines.top": False, "axes.spines.right": False})
    FIGURES.mkdir(parents=True, exist_ok=True)

    # 1. F1 with 90% intervals, backtest vs test
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    ys = list(range(len(headline)))[::-1]
    for y, h in zip(ys, headline):
        for off, key, color, name in ((0.15, "backtest", accent, "Backtest 2014–2020 (81 months, 8 episodes)"),
                                      (-0.15, "test", accent2, "Test 2021–2022 (22 months, 3 episodes)")):
            lo, hi = h[f"{key}_f1_ci"]
            ax.plot([lo, hi], [y + off, y + off], color=color, linewidth=2, alpha=0.5)
            ax.scatter([h[f"{key}_f1"]], [y + off], color=color, s=30, zorder=3, label=name if y == ys[0] else None)
    ax.set_yticks(ys, [h["label"] for h in headline]); ax.set_xlim(0, 1); ax.set_xlabel("F1 (dot) with 90% bootstrap interval")
    ax.axvline(headline[0]["backtest_f1"], color=accent, linestyle=":", linewidth=1)
    ax.axvline(headline[0]["test_f1"], color=accent2, linestyle=":", linewidth=1)
    ax.legend(frameon=False, fontsize=8, loc="lower right"); ax.set_title("M0–M4: backtest and test disagree", color=ink, loc="left")
    fig.tight_layout(); fig.savefig(FIGURES / "week7_model_f1_intervals.png", dpi=150, facecolor=surface); plt.close(fig)

    # 2. Episodes and lead times for M0 and M4
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.4), gridspec_kw={"width_ratios": [8, 3]})
    for ax, window in zip(axes, ("backtest 2014-2020", "test 2021-2022")):
        eps = sorted({e["first_stress_month"] or e["episode_start"] for e in episodes if e["window"] == window})
        for j, s in enumerate(("M0", "M4")):
            for e in (e for e in episodes if e["window"] == window and e["stage"] == s):
                x = eps.index(e["first_stress_month"] or e["episode_start"]) + (j - 0.5) * 0.36
                lt = e["lead_time_months"]
                ax.bar(x, lt if e["detected"] else 0, width=0.34, color=[accent, accent2][j],
                       label=LABELS[s] if e is next(q for q in episodes if q["window"] == window and q["stage"] == s) else None)
                if not e["detected"]:
                    ax.text(x, 0.08, "missed", rotation=90, ha="center", va="bottom", fontsize=7, color=muted)
        ax.set_xticks(range(len(eps)), eps, rotation=45, ha="right", fontsize=8)
        ax.set_ylim(0, 3.6); ax.set_title(window.capitalize(), color=ink, loc="left", fontsize=10)
    axes[0].set_ylabel("lead time (months before first stress month)")
    axes[0].legend(frameon=False, fontsize=8, loc="upper left")
    fig.tight_layout(); fig.savefig(FIGURES / "week7_model_lead_times.png", dpi=150, facecolor=surface); plt.close(fig)

    # 3. Threshold trade-off (backtest pooled scores)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    for s, color in (("M0", accent), ("M0+season", muted), ("M4", accent2)):
        c = tradeoff[s]
        a1.plot([p["recall"] for p in c], [p["precision"] or 0 for p in c], color=color, label=LABELS[s])
        a2.plot([p["alert_rate"] for p in c], [p["recall"] for p in c], color=color, label=LABELS[s])
    base = headline[0]["backtest_brier_base_rate"]
    a1.set_xlabel("recall (stress months warned)"); a1.set_ylabel("precision (alerts that were right)")
    a1.set_title("Precision–recall, backtest scores", color=ink, loc="left")
    a2.plot([0, 1], [0, 1], color=grid, linestyle="--", label="random alerts")
    a2.set_xlabel("share of months alerted"); a2.set_ylabel("recall"); a2.set_title("Alerts needed for recall", color=ink, loc="left")
    a2.legend(frameon=False, fontsize=8); del base
    fig.tight_layout(); fig.savefig(FIGURES / "week7_model_threshold_tradeoff.png", dpi=150, facecolor=surface); plt.close(fig)

    # 4. Label sensitivity
    if sensitivity:
        cases = list(dict.fromkeys(r["case"] for r in sensitivity))
        fig, ax = plt.subplots(figsize=(9, 3.4))
        width = 0.8 / len(STAGES)
        palette = [accent, muted, "#7c3aed", "#059669", "#db2777", accent2]
        for j, s in enumerate(STAGES):
            vals = [next((r["f1"] or 0) for r in sensitivity if r["case"] == c and r["stage"] == s) for c in cases]
            ax.bar([i + (j - 2.5) * width for i in range(len(cases))], vals, width=width, color=palette[j], label=LABELS[s])
        ax.set_xticks(range(len(cases)), [f"{c}\n({next(r['stress_months'] for r in sensitivity if r['case'] == c)} stress months)" for c in cases], fontsize=8)
        ax.set_ylabel("backtest F1"); ax.set_ylim(0, 1); ax.legend(frameon=False, fontsize=7, ncol=3)
        ax.set_title("Label sensitivity (D1/D2): backtest 2014–2020 only", color=ink, loc="left")
        fig.tight_layout(); fig.savefig(FIGURES / "week7_model_label_sensitivity.png", dpi=150, facecolor=surface); plt.close(fig)


if __name__ == "__main__":
    raise SystemExit(main())
