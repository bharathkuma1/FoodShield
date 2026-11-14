"""Week 6: run the local Kafka replay end to end and check it (decision D12).

One command does everything:

1. checks the Kafka archive's SHA-512, unpacks it once and starts one KRaft
   broker on 127.0.0.1 (fresh data folder, so every run starts from empty topics);
2. starts the Spark Structured Streaming consumer (scripts/run_week6_consumer.py);
3. replays 1,864,486 records (daily grid weather, monthly real price, annual
   production/trade) in availability order: fast up to --measure-from, then at
   --rate events per second;
4. optionally kills the consumer after N batches and restarts it (restart test)
   and re-sends a share of records (duplicate test);
5. stops everything and checks the run:
   - every unique record was processed exactly once, and duplicates were removed;
   - streamed national weather equals the D9-B Spark batch series;
   - streamed alerts equal an offline replay of the same records (no Kafka/Spark);
   - streamed M0 scores equal the frozen 2021-2022 test scores;
   - throughput and alert latency against the D12 targets (paced runs).

Examples (PowerShell or bash, from the repository folder):

    python scripts/run_week6_replay.py --name full_max --duplicate-fraction 0.01
    python scripts/run_week6_replay.py --name paced_100 --replay-from 2011-01-01 --measure-from 2022-01-01 --rate 100
    python scripts/run_week6_replay.py --name paced_250 --replay-from 2011-01-01 --measure-from 2022-01-01 --rate 250
    python scripts/run_week6_replay.py --name restart --kill-consumer-after-batches 20

Outputs: data/processed/week6_streaming/<name>/week6_report.json and alerts.csv
(small, shared); checkpoints, state and logs under data/interim/week6/runs/<name>/.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.streaming.kafka_local import LocalKafka, create_topics, java_executable  # noqa: E402
from foodshield.streaming.monitor import OnlineMonitor, load_cell_weights, load_frozen_models, replay_offline  # noqa: E402
from foodshield.streaming.producer import ReplayProducer  # noqa: E402
from foodshield.streaming.winsafe import kill_tree, remove_tree  # noqa: E402
from foodshield.streaming.records import GridWeather, replay_records  # noqa: E402


def sha512(path: Path) -> str:
    h = hashlib.sha512()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def start_consumer(config: Path, run_dir: Path) -> subprocess.Popen:
    log = open(run_dir / "consumer.log", "ab")
    return subprocess.Popen([sys.executable, str(ROOT / "scripts" / "run_week6_consumer.py"), "--config", str(config),
                             "--run-dir", str(run_dir)], stdout=log, stderr=subprocess.STDOUT)


def consumer_status(run_dir: Path) -> dict:
    try:
        return json.loads((run_dir / "consumer_status.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):  # missing, or briefly locked while the consumer replaces it (Windows)
        return {}


def wait_for(predicate, timeout: float, what: str, process: subprocess.Popen | None = None) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        if process is not None and process.poll() is not None:
            sys.exit(f"Consumer exited while waiting for {what}; see its consumer.log")
        time.sleep(0.5)
    sys.exit(f"Timed out waiting for {what}")


def kafka_alert_messages(bootstrap: str, topic: str) -> dict:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, consumer_timeout_ms=5000)
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    ids = [m.key.decode() for m in consumer]
    consumer.close()
    return {"messages": len(ids), "unique_alert_ids": len(set(ids))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week6_streaming.local.json")
    parser.add_argument("--name", required=True, help="Run name; outputs go to <output_root>/<name>.")
    parser.add_argument("--rate", type=float, default=0.0, help="Events per second from --measure-from on (0 = max).")
    parser.add_argument("--measure-from", default=None, help="available_at date (YYYY-MM-DD) where the measured window starts.")
    parser.add_argument("--until", default=None, help="Last available_at date to replay (default: all records).")
    parser.add_argument("--replay-from", default=None,
                        help="First available_at date to replay (default: all). Paced runs can start in 2011: the "
                             "2022 window then still has 10+ years of weather history and 12 months of prices.")
    parser.add_argument("--duplicate-fraction", type=float, default=0.0)
    parser.add_argument("--kill-consumer-after-batches", type=int, default=0)
    parser.add_argument("--work-root", type=Path, default=None,
                        help="Folder for Kafka data, checkpoints and Spark temp files. On Windows use a short path "
                             "without spaces (the Windows scripts use FOODSHIELD_WORK_ROOT, default fs6 on the system drive).")
    parser.add_argument("--partitions", type=int, default=None, help="Override observation-topic partitions.")
    parser.add_argument("--trigger-seconds", type=float, default=None, help="Override the Structured Streaming trigger.")
    parser.add_argument("--max-offsets", type=int, default=None, help="Override maxOffsetsPerTrigger.")
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    if args.partitions is not None:
        cfg["topics"]["observations"]["partitions"] = args.partitions
    if args.trigger_seconds is not None:
        cfg["streaming"]["trigger_seconds"] = args.trigger_seconds
    if args.max_offsets is not None:
        cfg["streaming"]["max_offsets_per_trigger"] = args.max_offsets
    if args.work_root is not None:
        cfg["work_root"] = str(args.work_root.resolve())
        cfg["kafka"]["data_dir"] = str(args.work_root.resolve() / "kafka-data")
    # One Kafka folder per run, so a file still locked by an earlier run (Windows) cannot block this one.
    cfg["kafka"]["data_dir"] = str(Path(cfg["kafka"]["data_dir"]) / args.name)
    run_dir = ROOT / cfg["work_root"] / args.name
    out_dir = ROOT / cfg["output_root"] / args.name
    # Clear the previous run of this name. The dashboard server may be reading its alert files, so retry.
    if not remove_tree(run_dir):
        sys.exit(f"Could not clear the previous run folder {run_dir}: a program still has a file in it open. "
                 "Close it (or delete the folder) and run again.")
    run_dir.mkdir(parents=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    effective_config = run_dir / "effective_config.json"
    effective_config.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    archive = ROOT / cfg["kafka"]["archive"]
    if not (ROOT / cfg["kafka"]["home"] / "libs").is_dir() and sha512(archive) != cfg["kafka"]["archive_sha512"]:
        sys.exit(f"SHA-512 of {archive} does not match the Apache release value in the config")

    inputs = {k: ROOT / v for k, v in cfg["inputs"].items()}
    print("Loading replay inputs ...", flush=True)
    grid = GridWeather(inputs["grid_daily_csv"])
    lag = int(cfg["availability"]["annual_lag_years"])
    start_at = f"{args.replay_from}T00:00:00Z" if args.replay_from else ""
    records = lambda: (r for r in replay_records(grid, inputs["price_csv"], inputs["qcl_csv"], inputs["tcl_csv"], lag, args.until)
                       if r["available_at"] >= start_at)

    kafka = LocalKafka(ROOT, cfg["kafka"])
    kafka.reset()
    print("Starting Kafka ...", flush=True)
    kafka.start()
    consumer = None
    restarts = []
    samples = []
    try:
        create_topics(cfg["kafka"]["bootstrap"], cfg["topics"])
        print("Starting Spark Structured Streaming consumer ...", flush=True)
        consumer = start_consumer(effective_config, run_dir)
        wait_for(lambda: (run_dir / "READY").exists(), 300, "the streaming query to start", consumer)

        producer = ReplayProducer(cfg["kafka"]["bootstrap"], cfg["topics"]["observations"]["name"])
        result: dict = {}
        thread = threading.Thread(target=lambda: result.update(producer.run(
            records(), args.measure_from, args.rate, args.duplicate_fraction)), daemon=True)
        print(f"Replaying (rate {args.rate or 'max'} from {args.measure_from or 'start'}) ...", flush=True)
        thread.start()
        killed = False
        last_print = 0.0
        while thread.is_alive():
            status = consumer_status(run_dir)
            samples.append({"t": time.time(), "produced_unique": sum(producer.unique.values()),
                            "processed": status.get("rows", 0), "last_month": status.get("last_month")})
            if args.kill_consumer_after_batches and not killed and status.get("last_batch_id", -1) >= args.kill_consumer_after_batches:
                kill_tree(consumer)
                restarts.append({"killed_after_batch": status.get("last_batch_id"), "epoch": time.time()})
                print(f"  killed consumer after batch {status.get('last_batch_id')}; restarting", flush=True)
                time.sleep(5)  # let the old Spark JVM exit before the checkpoint is reopened
                consumer = start_consumer(effective_config, run_dir)
                killed = True
            if consumer.poll() is not None:
                producer.stop_event.set()
                thread.join(30)
                sys.exit("Consumer stopped unexpectedly; see consumer.log")
            if time.time() - last_print > 15:
                print(f"  sent {producer.sent:,}  processed {status.get('rows', 0):,}  last month {status.get('last_month')}", flush=True)
                last_print = time.time()
            time.sleep(1)
        if "messages_sent" not in result:
            sys.exit("Producer failed")

        print("Computing the offline reference ...", flush=True)
        models = load_frozen_models(ROOT / cfg["models"]["frozen_report"], cfg["models"]["variant"],
                                    int(cfg["models"]["annual_lag"]), cfg["models"]["stages"])
        reference_monitor = OnlineMonitor(load_cell_weights(inputs["grid_cells"]), models, int(cfg["models"]["annual_lag"]))
        reference = {a["record_id"]: a for a in replay_offline(records(), reference_monitor)}
        expected_months = len(reference_monitor.scored)
        unique_total = result["unique_total"]
        wait_for(lambda: consumer_status(run_dir).get("rows", 0) >= unique_total
                 and consumer_status(run_dir).get("months_evaluated", 0) >= expected_months, 900,
                 "the consumer to process every record", consumer)
        time.sleep(3)
        (run_dir / "STOP").write_text("stop", encoding="utf-8")
        try:
            consumer.wait(180)
        except subprocess.TimeoutExpired:
            kill_tree(consumer)
        alert_topic = kafka_alert_messages(cfg["kafka"]["bootstrap"], cfg["topics"]["alerts"]["name"])
    finally:
        if consumer is not None and consumer.poll() is None:
            kill_tree(consumer)
        kafka.stop()

    # ---------------- checks and report ----------------
    status = consumer_status(run_dir)
    batches, progress = read_jsonl(run_dir / "batches.jsonl"), read_jsonl(run_dir / "progress.jsonl")
    processed = {}
    for b in batches:
        for k, v in b["rows"].items():
            processed[k] = processed.get(k, 0) + v
    kafka_input_rows = sum(p["num_input_rows"] for p in progress)
    streamed: dict[str, dict] = {}
    for path in sorted((run_dir / "alerts").glob("batch-*.jsonl")):
        for a in read_jsonl(path):
            streamed.setdefault(a["record_id"], a)  # first emission (a restart may re-emit)
    score_diffs = [abs(streamed[k]["score"] - reference[k]["score"]) for k in reference if k in streamed]
    flags_equal = all(streamed[k]["alert"] == reference[k]["alert"] for k in reference if k in streamed)

    monitor_state = json.loads((run_dir / "monitor_state.json").read_text(encoding="utf-8"))
    batch_weather = {r["month"]: r for r in csv.DictReader(inputs["grid_batch_monthly_csv"].open(encoding="utf-8"))}
    weather_diff = max(abs(monitor_state["weather"][m][i] - float(batch_weather[m][f]))
                       for m in batch_weather if m in monitor_state["weather"]
                       for i, f in enumerate(("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")))

    frozen = json.loads((ROOT / cfg["models"]["frozen_report"]).read_text(encoding="utf-8"))
    run = next(r for r in frozen["runs"] if r["variant"] == cfg["models"]["variant"] and r["annual_lag"] == int(cfg["models"]["annual_lag"]))
    cross = {}
    for stage in cfg["models"]["stages"]:
        official = {m["origin"]: m for m in run["stages"][stage]["monthly"]}
        mine = {a["observation_month"]: a for a in streamed.values() if a["model_stage"] == stage}
        common = [o for o in official if o in mine]
        cross[stage] = {"test_months": len(official), "compared": len(common),
                        "max_abs_score_difference": max(abs(mine[o]["score"] - official[o]["score"]) for o in common) if common else None,
                        "alerts_agree": sum(int(mine[o]["alert"]) == official[o]["alert"] for o in common)}

    window = result.get("measured_window")
    latency = {}
    if window:
        # Months whose last input was sent inside the measured window; one value per month (first stage).
        window_alerts = [a for a in streamed.values() if a.get("latency_s") is not None
                         and (a.get("inputs_complete_kafka_ts") or 0) >= window["first_send_epoch"]
                         and a["model_stage"] == cfg["models"]["stages"][0]]
        values = [a["latency_s"] for a in window_alerts]
        latency = {"months": len(values), "p50_s": percentile(values, 0.5), "p95_s": percentile(values, 0.95),
                   "max_s": max(values) if values else None}
        rows_in_window = [b for b in batches if b["end_epoch"] >= window["first_send_epoch"]]
        processed_window = sum(sum(b["rows"].values()) for b in rows_in_window)
        span = (max(b["end_epoch"] for b in rows_in_window) - window["first_send_epoch"]) if rows_in_window else None
        window["consumer_processed_rows"] = processed_window
        window["consumer_events_per_s"] = round(processed_window / span, 1) if span else None
        lags = [s["produced_unique"] - s["processed"] for s in samples if s["t"] >= window["first_send_epoch"]]
        window["max_consumer_lag_records"] = max(lags) if lags else None
        if args.rate:
            window["max_consumer_lag_seconds_at_rate"] = round(max(lags) / args.rate, 1) if lags else None

    targets = cfg["targets"]
    checks = {
        "every_unique_record_processed_once": processed == result["unique_records"],
        "duplicates_removed": (kafka_input_rows - sum(processed.values()) == result["duplicates_sent"]) if not restarts else None,
        "streamed_alerts_equal_offline_reference": set(streamed) == set(reference) and flags_equal
                                                    and (max(score_diffs) if score_diffs else 0) < 1e-9,
        "streamed_weather_equals_d9b_batch_within_csv_rounding": weather_diff < 1e-3,
        "m0_scores_equal_frozen_test": None if not cross.get("M0", {}).get("compared") else
                                       (cross["M0"]["max_abs_score_difference"] < 1e-9
                                        and cross["M0"]["alerts_agree"] == cross["M0"]["test_months"]),
        "kafka_alert_topic_has_every_alert": alert_topic["unique_alert_ids"] == len(reference),
    }
    if args.rate and window:
        checks["offered_rate_achieved"] = window["achieved_events_per_s"] >= 0.98 * args.rate
        checks["alert_latency_p95_within_target"] = latency.get("p95_s") is not None and latency["p95_s"] <= targets["alert_latency_p95_s"]

    try:
        java_version = subprocess.run([java_executable(), "-version"], capture_output=True, text=True).stderr.strip().splitlines()
        java_version = [l for l in java_version if not l.startswith("Picked up")][0]
    except Exception:  # noqa: BLE001
        java_version = None
    import pyspark
    report = {
        "pipeline": "week6_kafka_spark_streaming_replay", "run": args.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "settings": {"rate_events_per_s": args.rate or "max", "measure_from": args.measure_from, "until": args.until,
                     "replay_from": args.replay_from,
                     "duplicate_fraction": args.duplicate_fraction, "kill_consumer_after_batches": args.kill_consumer_after_batches,
                     "trigger_seconds": cfg["streaming"]["trigger_seconds"],
                     "max_offsets_per_trigger": cfg["streaming"]["max_offsets_per_trigger"],
                     "dedupe_watermark": cfg["streaming"]["dedupe_watermark"], "topics": cfg["topics"], "spark": cfg["spark"]},
        "machine": {"os": platform.platform(), "cpu_count": os.cpu_count(), "python": platform.python_version(),
                    "pyspark": pyspark.__version__, "java": java_version, "kafka": Path(cfg["kafka"]["home"]).name},
        "producer": result,
        "consumer": {"batches": len(batches), "kafka_input_rows": kafka_input_rows, "processed_rows": processed,
                     "processed_total": sum(processed.values()), "restarts": restarts,
                     "redelivered_batches_skipped": status.get("skipped_redelivered_batches"),
                     "months_evaluated": status.get("months_evaluated"), "monitor_counters": status.get("counters")},
        "alerts": {"unique": len(streamed), "reference": len(reference), "alerts_raised": sum(a["alert"] for a in streamed.values()),
                   "max_abs_score_difference_vs_reference": max(score_diffs) if score_diffs else None,
                   "kafka_alert_topic": alert_topic},
        "weather_max_abs_difference_vs_d9b_batch": weather_diff,
        "frozen_test_cross_check_2021_01_to_2022_10": cross,
        "measured_window": window, "alert_latency": latency, "targets": targets, "checks": checks,
        "all_checks_passed": all(v for v in checks.values() if v is not None),
        "note": "Engineering demonstration (D12). Alerts for 2011-2020 are in-sample for the frozen models; the replay adds no test evidence.",
    }
    (out_dir / "week6_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    with (out_dir / "alerts.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["observation_month", "model_stage", "score", "cutoff", "alert", "latency_s", "reason_codes"])
        for a in sorted(streamed.values(), key=lambda a: (a["observation_month"], a["model_stage"])):
            writer.writerow([a["observation_month"], a["model_stage"], f"{a['score']:.6f}", f"{a['cutoff']:.6f}", int(a["alert"]),
                             "" if a.get("latency_s") is None else f"{a['latency_s']:.3f}", " ".join(a["reason_codes"])])
    print(json.dumps({k: report[k] for k in ("producer", "consumer", "alert_latency", "checks", "all_checks_passed")}, indent=2))
    return 0 if report["all_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
