"""Week 7 streaming series: plot and tabulate the s7_* replay runs (scripts/run_week6_replay.py).

    python scripts/plot_week7_streaming.py   ->  docs/figures/week7_streaming_series.png,
                                                 data/processed/week6_streaming/week7_series_summary.csv
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / "data" / "processed" / "week6_streaming"


def main() -> int:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = []
    for d in sorted(RUNS.glob("s7_*")):
        r = json.loads((d / "week6_report.json").read_text(encoding="utf-8"))
        w = r["producer"]["measured_window"]
        rows.append({"run": d.name, "rate": r["settings"]["rate_events_per_s"],
                     "partitions": r["settings"]["topics"]["observations"]["partitions"],
                     "trigger_s": r["settings"]["trigger_seconds"], "window_messages": w["messages"],
                     "window_seconds": w["seconds"], "achieved_per_s": w["achieved_events_per_s"],
                     "consumer_per_s": w["consumer_events_per_s"], "micro_batches": r["consumer"]["batches"],
                     "latency_p50_s": r["alert_latency"]["p50_s"], "latency_p95_s": r["alert_latency"]["p95_s"],
                     "latency_max_s": r["alert_latency"]["max_s"], "all_checks_passed": r["all_checks_passed"]})
    with (RUNS / "week7_series_summary.csv").open("w", encoding="utf-8", newline="") as stream:
        w = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    ink, muted, accent, accent2, surface = "#1f2933", "#7b8794", "#2563eb", "#d97706", "#ffffff"
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": muted, "axes.labelcolor": ink, "xtick.color": ink,
                         "ytick.color": ink, "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.4), gridspec_kw={"width_ratios": [4, 3, 3]})
    groups = [
        ("Offered rate (events/s)\n3 partitions, 1 s trigger", [r for r in rows if r["run"].startswith("s7_rate")], "rate", "{:.0f}"),
        ("Partitions\n2,000 events/s, 1 s trigger", [r for r in rows if r["run"].startswith("s7_part")] +
         [r for r in rows if r["run"] == "s7_rate_2000"], "partitions", "{}"),
        ("Trigger interval (s)\n1,000 events/s, 3 partitions", [r for r in rows if r["run"].startswith("s7_trig")] +
         [r for r in rows if r["run"] == "s7_rate_1000"], "trigger_s", "{:g}"),
    ]
    for ax, (title, rs, key, fmt) in zip(axes, groups):
        rs = sorted(rs, key=lambda r: float(r[key]))
        xs = range(len(rs))
        ax.bar([x - 0.2 for x in xs], [r["latency_p50_s"] for r in rs], width=0.38, color=accent, label="p50")
        ax.bar([x + 0.2 for x in xs], [r["latency_p95_s"] for r in rs], width=0.38, color=accent2, label="p95")
        ax.axhline(10, color=muted, linestyle="--", linewidth=1)
        ax.set_xticks(list(xs), [fmt.format(float(r[key]) if key != "partitions" else r[key]) for r in rs])
        ax.set_title(title, color=ink, loc="left", fontsize=9)
        ax.set_ylim(0, 11)
    axes[0].set_ylabel("alert latency (s)")
    axes[0].text(0.02, 10.15, "target p95 ≤ 10 s", color=muted, fontsize=8, transform=axes[0].get_yaxis_transform())
    axes[0].legend(frameon=False, fontsize=8, loc="upper left", bbox_to_anchor=(0, 0.92))
    fig.tight_layout()
    fig.savefig(ROOT / "docs" / "figures" / "week7_streaming_series.png", dpi=150, facecolor=surface)
    for r in rows:
        print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
