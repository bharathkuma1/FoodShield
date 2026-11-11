"""Draw the review figures: Week 4 backtest timeline, synthetic showcase, Week 5 ablation.

    python scripts/plot_week4_review_figures.py
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from foodshield.evaluation.week4 import read_real_prices  # noqa: E402

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SERIES_1, SERIES_2 = "#2a78d6", "#eb6834"  # categorical slots 1 (blue) and 2 (orange)
STRESS_FILL = "#e9e8e4"


def d(key: str) -> date:
    return date(int(key[:4]), int(key[5:7]), 1)


def style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.set_ylabel("Real farm-gate price (UAH/t, Dec 2000 prices)", color=INK_2, fontsize=9)
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="bold", pad=36)
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, color=INK_2, fontsize=9, va="bottom", linespacing=1.4)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))


def shade(ax, months: list[str]) -> None:
    for m in months:
        start = d(m)
        end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
        ax.axvspan(start, end, color=STRESS_FILL, linewidth=0, zorder=0)


def backtest_figure(out: Path) -> None:
    prices = read_real_prices(ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv")
    rows = list(csv.DictReader((ROOT / "data/processed/week4_rolling_origin/rolling_origin_predictions.csv").open()))
    report = json.loads((ROOT / "data/processed/week4_rolling_origin/rolling_origin_report.json").read_text())
    months = [k for k in sorted(prices) if "2013-06" <= k <= "2020-12"]
    fig, ax = plt.subplots(figsize=(11, 5.2), dpi=200, facecolor=SURFACE)
    shade(ax, [r["origin"] for r in rows if r["label"] == "1"])
    ax.plot([d(m) for m in months], [prices[m] for m in months], color=SERIES_1, linewidth=2, zorder=2)
    alerts = [r["origin"] for r in rows if r["m0_alert"] == "1"]
    ax.scatter([d(m) for m in alerts], [prices[m] for m in alerts], s=46, color=SERIES_2,
               edgecolor=SURFACE, linewidth=1.5, zorder=3)
    p = report["pooled_out_of_sample"]
    style(ax, "M0 rolling-origin backtest on real data, 2014-2020 (2016 has no common-origin months)",
          f"Shaded = month followed by a >=10% real price rise within 3 months.\nOut of sample: {p['n']} months, "
          f"precision {p['precision']:.2f}, recall {p['recall']:.2f}, F1 {p['f1']:.2f}, "
          f"mean per-year AUC {p['mean_fold_auc']:.2f}; {report['episodes_detected']} episodes caught")
    handles = [plt.Line2D([], [], color=SERIES_1, linewidth=2),
               plt.Line2D([], [], marker="o", linestyle="", color=SERIES_2, markersize=7),
               plt.Rectangle((0, 0), 1, 1, color=STRESS_FILL)]
    ax.legend(handles, ["Real price (FAOSTAT / SSSU CPI)", "M0 alert", "Stress ahead (label = 1)"],
              loc="upper left", bbox_to_anchor=(0, -0.09), frameon=False, fontsize=9, labelcolor=INK_2, ncol=3)
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def showcase_figure(out: Path) -> None:
    path = ROOT / "data/synthetic/week4_showcase/ukraine_wheat_test_window_synthetic_showcase.csv"
    rows = list(csv.DictReader(path.open()))
    meta = json.loads(path.with_name(path.stem + ".metadata.json").read_text())
    s = meta["showcase_scenario"]
    dist = meta["distribution_across_scenarios"]
    real = [r for r in rows if r["is_synthetic"] == "false"]
    syn = [real[-1]] + [r for r in rows if r["is_synthetic"] == "true"]
    fig, ax = plt.subplots(figsize=(11, 5.2), dpi=200, facecolor=SURFACE)
    shade(ax, [r["month"] for r in rows if r["future_stress_label"] == "1"])
    ax.plot([d(r["month"]) for r in real], [float(r["price_real_dec_2000_lcu_per_tonne"]) for r in real],
            color=INK_2, linewidth=2, zorder=2)
    ax.plot([d(r["month"]) for r in syn], [float(r["price_real_dec_2000_lcu_per_tonne"]) for r in syn],
            color=SERIES_1, linewidth=2, linestyle=(0, (4, 2)), zorder=2)
    alerts = [r for r in rows if r["m0_alert"] == "1"]
    ax.scatter([d(r["month"]) for r in alerts], [float(r["price_real_dec_2000_lcu_per_tonne"]) for r in alerts],
               s=46, color=SERIES_2, edgecolor=SURFACE, linewidth=1.5, zorder=3)
    ax.axvline(date(2021, 1, 1), color=GRID, linewidth=1)
    ax.text(date(2021, 1, 20), ax.get_ylim()[1], "synthetic from here", color=INK_2, fontsize=8, va="top")
    style(ax, f"SYNTHETIC showcase - frozen M0 on a simulated 2021-2022 test window (scenario {s['scenario_id']} of 500)",
          f"This path: {s['origins']} test months, {s['episodes_detected']} of {s['episodes']} stress episodes caught, "
          f"{s['false_positive']} false alarms, F1 {s['f1']:.2f}.\nAcross 500 paths: median F1 {dist['f1']['median']:.2f}, "
          f"median AUC {dist['auc']['median']:.2f}. Not real data.")
    handles = [plt.Line2D([], [], color=INK_2, linewidth=2),
               plt.Line2D([], [], color=SERIES_1, linewidth=2, linestyle=(0, (4, 2))),
               plt.Line2D([], [], marker="o", linestyle="", color=SERIES_2, markersize=7),
               plt.Rectangle((0, 0), 1, 1, color=STRESS_FILL)]
    ax.legend(handles, ["Real 2020 history", "Synthetic price path", "M0 alert", "Stress ahead (label = 1)"],
              loc="upper left", bbox_to_anchor=(0, -0.09), frameon=False, fontsize=9, labelcolor=INK_2, ncol=4)
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)




def ablation_figure(out: Path, report_path: Path = ROOT / "data/processed/week5_ablation_lag2/ablation_report.json",
                    variant: str = "corrected") -> None:
    """Week 5: backtest F1 (with 90% bootstrap interval) and mean per-year AUC by stage."""
    report = json.loads(report_path.read_text())
    v = next(x for x in report["variants"] if x["variant"] == variant)
    unc = report["uncertainty"]["by_variant"][variant]
    stages = ["M0", "M0+season", "M1", "M2", "M3", "M4"]
    names = {"M0": "M0 price", "M0+season": "M0 + season (control)", "M1": "M1 + weather",
             "M2": "M2 + production", "M3": "M3 + trade", "M4": "M4 + interaction"}
    always = v["baselines_on_backtest_origins"]["always_alert"]["f1"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.4), dpi=200, facecolor=SURFACE, sharey=True)
    ys = list(range(len(stages)))[::-1]
    for ax in (a1, a2):
        ax.set_facecolor(SURFACE)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.spines["bottom"].set_color(GRID)
        ax.grid(axis="x", color=GRID, linewidth=0.8)
        ax.tick_params(colors=INK_2, labelsize=9, length=0)
    for y, s in zip(ys, stages):
        b = v["stages"][s]["backtest"]
        lo, hi = unc[s]["f1_90pct_interval"]
        color = SERIES_2 if s == "M0+season" else SERIES_1
        a1.plot([lo, hi], [y, y], color=color, linewidth=2, alpha=0.45, solid_capstyle="round")
        a1.scatter([b["f1"]], [y], s=60, color=color, edgecolor=SURFACE, linewidth=1.5, zorder=3)
        a1.text(hi + 0.012, y, f"{b['f1']:.2f}", va="center", fontsize=9, color=INK_2)
        a2.scatter([b["mean_fold_auc"]], [y], s=60, color=color, edgecolor=SURFACE, linewidth=1.5, zorder=3)
        a2.text(b["mean_fold_auc"] + 0.012, y, f"{b['mean_fold_auc']:.2f}", va="center", fontsize=9, color=INK_2)
    a1.axvline(always, color=INK_2, linewidth=1, linestyle=(0, (3, 3)))
    a1.text(always, len(stages) - 0.45, f"always-alert {always:.2f}", fontsize=8, color=INK_2, ha="center")
    a2.axvline(0.5, color=INK_2, linewidth=1, linestyle=(0, (3, 3)))
    a2.text(0.5, len(stages) - 0.45, "chance 0.50", fontsize=8, color=INK_2, ha="center")
    a1.set_yticks(ys, [names[s] for s in stages], color=INK)
    a1.set_xlim(0.2, 0.8)
    a2.set_xlim(0.45, 1.0)
    a1.set_xlabel("F1 on 2014-2020 backtest (line = 90% bootstrap interval)", color=INK_2, fontsize=9)
    a2.set_xlabel("Mean per-year AUC (ranking quality)", color=INK_2, fontsize=9)
    n = v["stages"]["M0"]["backtest"]
    fig.suptitle("Week 5 ablation: adding weather, production and trade does not beat price-only on 2014-2020",
                 x=0.01, ha="left", color=INK, fontsize=12, fontweight="bold")
    fig.text(0.01, 0.885, f"Real data, corrected features, {n['n']} out-of-sample months ({n['positives']} with stress ahead), "
             "same estimator and months for every stage. Orange = calendar-season control, not a model stage.",
             color=INK_2, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def final_test_figure(out: Path, report_path: Path = ROOT / "data/processed/final_test/final_test_report.json") -> None:
    """Real 2021-2022 test (opened once): price, stress months, and M0 vs M4 alerts."""
    report = json.loads(report_path.read_text())
    run = next(r for r in report["runs"] if r["annual_lag"] == 2 and r["variant"] == "corrected")
    prices = read_real_prices(ROOT / "data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv")
    months = [k for k in sorted(prices) if "2020-07" <= k <= "2023-01"]
    m0, m4 = run["stages"]["M0"], run["stages"]["M4"]
    stress = [x["origin"] for x in m0["monthly"] if x["label"] == 1]
    fig, (ax, strip) = plt.subplots(2, 1, figsize=(11, 5.6), dpi=200, facecolor=SURFACE, sharex=True,
                                    gridspec_kw={"height_ratios": [3.2, 1]})
    shade(ax, stress)
    shade(strip, stress)
    ax.plot([d(m) for m in months], [prices[m] for m in months], color=SERIES_1, linewidth=2, zorder=2)
    ax.axvline(date(2022, 2, 24), color=INK_2, linewidth=1, linestyle=(0, (3, 3)))
    ax.text(date(2022, 3, 5), ax.get_ylim()[1], "24 Feb 2022 invasion", color=INK_2, fontsize=8, va="top")
    style(ax, "Real 2021-2022 test, opened once: M4 cut false alarms and caught all 3 stress episodes",
          f"{run['test_origins']} test months, {run['test_stress_months']} with stress ahead (shaded). "
          f"M0 price-only: F1 {m0['f1']:.2f}, false alarms {m0['false_positive_rate']:.0%}. "
          f"M4 all signals: F1 {m4['f1']:.2f}, false alarms {m4['false_positive_rate']:.0%}.\n"
          "Small sample: the 2014-2020 backtest pointed the other way. Treat as one data point, not proof.")
    for row, (name, stage, color) in enumerate((("M4 all signals", m4, SERIES_1), ("M0 price only", m0, INK_2))):
        xs = [d(x["origin"]) for x in stage["monthly"] if x["alert"] == 1]
        strip.scatter(xs, [row] * len(xs), s=42, color=color, edgecolor=SURFACE, linewidth=1.2, zorder=3)
    strip.set_yticks([0, 1], ["M4 alerts", "M0 alerts"], color=INK)
    strip.set_ylim(-0.7, 1.7)
    strip.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        strip.spines[side].set_visible(False)
    strip.spines["bottom"].set_color(GRID)
    strip.tick_params(colors=INK_2, labelsize=9, length=0)
    strip.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1, 7)))
    strip.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


if __name__ == "__main__":
    figures = ROOT / "docs" / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    backtest_figure(figures / "week4_backtest_timeline.png")
    showcase_figure(figures / "week4_synthetic_showcase.png")
    ablation_figure(figures / "week5_ablation.png")
    if (ROOT / "data/processed/final_test/final_test_report.json").exists():
        final_test_figure(figures / "final_test_2021_2022.png")
    print("Wrote the Week 4 and Week 5 figures to docs/figures/")
