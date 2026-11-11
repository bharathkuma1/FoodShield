"""Fold the laptop runs into the shared results in one step (Week 8).

Run this after ``scripts/week6_laptop.ps1`` (and, optionally, the laptop batch
series ``run_week7_batch_scaling.py --phase all --output-suffix _laptop``):

    python scripts/apply_laptop_results.py

It reads ``data/processed/week6_streaming/laptop_*/week6_report.json`` and writes:

- ``data/processed/week6_streaming/laptop_summary.csv`` and ``laptop_summary.json``;
- ``docs/WEEK6_LAPTOP_RESULTS.md``: laptop and workspace runs side by side, the
  laptop batch series if present, and the ready-to-paste text for the four
  "TBD (laptop)" cells in the report's Section 5.2 and the slide-10 note;
- ``docs/dashboard/index.html``, rebuilt (the dashboard already shows laptop runs).

Exit code: 0 when all four laptop runs are present and passed every check,
1 when a run is missing, 2 when a run failed a check. Nothing else is changed.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STREAMING = ROOT / "data" / "processed" / "week6_streaming"
BATCH_LAPTOP = ROOT / "data" / "processed" / "week7_batch_scaling_laptop"
BATCH_WORKSPACE = ROOT / "data" / "processed" / "week7_batch_scaling"
DOC = ROOT / "docs" / "WEEK6_LAPTOP_RESULTS.md"

# Report row label, laptop run name, workspace run name, what the run tests.
RUNS = [
    ("Full speed, 1% re-sent", "laptop_full_max", "full_max", "Capacity, completeness, de-duplication"),
    ("Hard restart", "laptop_restart", "restart", "Recovery from a killed consumer"),
    ("250 events/s", "laptop_paced_250", "paced_250", "Burst target (>=250/s, p95 <=10 s)"),
    ("100 events/s", "laptop_paced_100", "paced_100", "Sustained target (>=100/s, p95 <=10 s)"),
]


def load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def get(d: dict | None, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def summarise(report: dict) -> dict:
    """One flat row per run, from the fields run_week6_replay.py writes."""
    window = get(report, "producer", "measured_window") or get(report, "measured_window") or {}
    checks = get(report, "checks", default={}) or {}
    sent = get(report, "producer", "messages_sent")
    processed = get(report, "consumer", "processed_total")
    kafka_rows = get(report, "consumer", "kafka_input_rows")
    return {
        "run": get(report, "run"),
        "generated_utc": get(report, "generated_utc"),
        "machine_os": get(report, "machine", "os"),
        "machine_cores": get(report, "machine", "cpu_count"),
        "java": (get(report, "machine", "java") or "").split("\n")[0],
        "offered_rate": get(report, "settings", "rate_events_per_s"),
        "replay_from": get(report, "settings", "replay_from"),
        "messages_sent": sent,
        "unique_records": get(report, "producer", "unique_total"),
        "duplicates_sent": get(report, "producer", "duplicates_sent"),
        "duplicates_removed": (kafka_rows - processed) if isinstance(kafka_rows, int) and isinstance(processed, int) else None,
        "processed_total": processed,
        "micro_batches": get(report, "consumer", "batches"),
        "restarts": len(get(report, "consumer", "restarts", default=[]) or []),
        "window_messages": window.get("messages"),
        "window_seconds": window.get("seconds"),
        "achieved_per_s": window.get("achieved_events_per_s"),
        "consumer_per_s": window.get("consumer_events_per_s"),
        "max_backlog_records": window.get("max_consumer_lag_records"),
        "latency_months": get(report, "alert_latency", "months"),
        "latency_p50_s": get(report, "alert_latency", "p50_s"),
        "latency_p95_s": get(report, "alert_latency", "p95_s"),
        "latency_max_s": get(report, "alert_latency", "max_s"),
        "alerts": get(report, "alerts", "unique"),
        "m0_alerts_agree": get(report, "frozen_test_cross_check_2021_01_to_2022_10", "M0", "alerts_agree"),
        "m4_alerts_agree": get(report, "frozen_test_cross_check_2021_01_to_2022_10", "M4", "alerts_agree"),
        "all_checks_passed": get(report, "all_checks_passed"),
        "failed_checks": ";".join(k for k, v in checks.items() if v is False),
    }


def num(v, digits=1, thousands=True) -> str:
    if v is None:
        return "n/a"
    if isinstance(v, int) or (isinstance(v, float) and digits == 0):
        return f"{v:,.0f}" if thousands else f"{v:.0f}"
    return f"{v:,.{digits}f}" if thousands else f"{v:.{digits}f}"


def report_cell(label: str, s: dict) -> str:
    """Text for one 'Laptop' cell of the report's Section 5.2 table, in the workspace column's style."""
    ok = "all checks passed" if s["all_checks_passed"] else f"FAILED: {s['failed_checks'] or 'see report'}"
    p95 = f"p95 {num(s['latency_p95_s'])} s"
    if label.startswith("Full speed"):
        return (f"{num(s['achieved_per_s'], 0)} events/s sent, {num(s['consumer_per_s'], 0)}/s processed; "
                f"{num(s['duplicates_removed'], 0)} of {num(s['duplicates_sent'], 0)} duplicates removed; {p95}; {ok}")
    if label.startswith("Hard restart"):
        return (f"{num(s['max_backlog_records'], 0)}-record backlog cleared from the checkpoint; "
                f"{'nothing lost or double-counted' if s['all_checks_passed'] else 'see checks'}; {p95}; {ok}")
    met = "met" if s["all_checks_passed"] else "NOT met"
    return (f"{num(s['achieved_per_s'])}/s for {num(s['window_seconds'], 0)} s; "
            f"p50 {num(s['latency_p50_s'])} s, {p95}; {met}")


def run_table(rows: list[tuple[str, dict | None]]) -> list[str]:
    out = ["| Run | Achieved rate | Consumer rate | Largest backlog | Alert latency p50 / p95 / max | Micro-batches | Checks |",
           "| --- | --- | --- | --- | --- | --- | --- |"]
    for label, s in rows:
        if s is None:
            out.append(f"| {label} | not run | | | | | |")
            continue
        out.append(f"| {label} | {num(s['achieved_per_s'])}/s | {num(s['consumer_per_s'])}/s | "
                   f"{num(s['max_backlog_records'], 0)} records | {num(s['latency_p50_s'])} / {num(s['latency_p95_s'])} / "
                   f"{num(s['latency_max_s'])} s ({s['latency_months']} months) | {s['micro_batches']} | "
                   f"{'all passed' if s['all_checks_passed'] else 'FAILED: ' + (s['failed_checks'] or 'see report')} |")
    return out


def batch_section() -> list[str]:
    laptop = load(BATCH_LAPTOP / "summary.json")
    if not laptop:
        return ["## Batch series on the laptop", "",
                "Not run. Optional: `python scripts\\run_week7_batch_scaling.py --phase all --output-suffix _laptop` "
                "(about an hour), then run this script again.", ""]
    workspace = load(BATCH_WORKSPACE / "summary.json") or {}

    def rows_of(summary: dict) -> dict:
        rows = summary.get("configs") or []
        return {r.get("tag"): r for r in rows if isinstance(r, dict)}

    lap, ws = rows_of(laptop), rows_of(workspace)
    out = ["## Batch series on the laptop", "",
           f"Machine: {get(laptop, 'machine', 'os') or 'see summary.json'}, "
           f"{get(laptop, 'machine', 'cpu_count') or '?'} cores. Median of the timed runs after a warm-up.", "",
           "| Configuration | Laptop median | Workspace median (2 cores) |", "| --- | --- | --- |"]
    for tag in sorted(lap, key=lambda t: (lap[t].get("series") or "", lap[t].get("factor") or 0, t)):
        out.append(f"| `{tag}` | {num(lap[tag].get('median_s'), 2)} s | "
                   f"{num(ws.get(tag, {}).get('median_s'), 2) + ' s' if tag in ws else 'n/a'} |")
    return out + [""]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--streaming-dir", type=Path, default=STREAMING, help="where the run folders are")
    parser.add_argument("--doc", type=Path, default=DOC, help="markdown file to write")
    parser.add_argument("--no-dashboard", action="store_true", help="skip rebuilding docs/dashboard/index.html")
    args = parser.parse_args()

    laptop, workspace, missing = [], [], []
    for label, lap_name, ws_name, _ in RUNS:
        lap = load(args.streaming_dir / lap_name / "week6_report.json")
        ws = load(args.streaming_dir / ws_name / "week6_report.json")
        if lap is None:
            missing.append(lap_name)
        laptop.append((label, summarise(lap) if lap else None))
        workspace.append((label, summarise(ws) if ws else None))

    found = [s for _, s in laptop if s]
    if found:
        with (args.streaming_dir / "laptop_summary.csv").open("w", encoding="utf-8", newline="") as stream:
            w = csv.DictWriter(stream, fieldnames=list(found[0]), lineterminator="\n")
            w.writeheader()
            w.writerows(found)
        (args.streaming_dir / "laptop_summary.json").write_text(
            json.dumps({"generated_utc": datetime.now(timezone.utc).isoformat(), "runs": found}, indent=2) + "\n",
            encoding="utf-8")

    failed = [s["run"] for s in found if not s["all_checks_passed"]]
    machine = found[0] if found else {}
    lines = [
        "# Week 6: laptop runs (generated)",
        "",
        "**Generated by** `scripts/apply_laptop_results.py`. Do not edit by hand; run the script again instead.",
        "",
        f"**Laptop:** {machine.get('machine_os') or 'not run yet'}, {machine.get('machine_cores') or '?'} cores, "
        f"{machine.get('java') or 'Java unknown'}.",
        "",
        f"**Status:** {len(found)} of {len(RUNS)} laptop runs found"
        + (f"; missing: {', '.join(missing)}" if missing else "")
        + (f"; failed a check: {', '.join(failed)}" if failed else "; every check passed" if found else "") + ".",
        "",
        "## Laptop runs",
        "",
        *run_table(laptop),
        "",
        "## Workspace development runs (for comparison)",
        "",
        *run_table(workspace),
        "",
        "## Text for the report and slides",
        "",
        "Report Section 5.2, \"Laptop\" column of the target-runs table:",
        "",
        "| Run | Laptop |",
        "| --- | --- |",
    ]
    for label, s in laptop:
        lines.append(f"| {label} | {report_cell(label, s) if s else 'TBD (laptop)'} |")
    paced = [s for label, s in laptop if s and label[0].isdigit()]
    if paced:
        p95 = max(s["latency_p95_s"] for s in paced if s["latency_p95_s"] is not None)
        note = (f"Slide 10 note: laptop runs met both targets with alert p95 at or below {num(p95)} s."
                if all(s["all_checks_passed"] for s in paced) else
                "Slide 10 note: a laptop paced run did not meet its target; see the table above.")
        lines += ["", note]
    lines += ["", *batch_section()]
    args.doc.parent.mkdir(parents=True, exist_ok=True)
    args.doc.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {args.doc}")

    if not args.no_dashboard:
        result = subprocess.run([sys.executable, str(ROOT / "scripts" / "build_week8_dashboard.py")])
        print("Dashboard rebuilt" if result.returncode == 0 else "Dashboard rebuild failed; run build_week8_dashboard.py")

    for label, s in laptop:
        print(f"  {label:24s} {report_cell(label, s) if s else 'missing'}")
    if missing:
        print(f"Missing laptop runs: {', '.join(missing)}")
        return 1
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
