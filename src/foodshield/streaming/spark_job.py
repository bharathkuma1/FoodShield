"""Spark Structured Streaming consumer for the Week 6 replay (decision D12).

Kafka topic -> parse JSON with an explicit schema -> drop malformed records ->
drop duplicate ``record_id`` values within an event-time watermark (Spark state)
-> ``foreachBatch``:

1. Spark aggregates the batch's daily weather to cell-month partial sums
   (sums, counts, a day bitmask and the latest Kafka timestamp);
2. the driver-side OnlineMonitor merges them, closes complete months and scores
   the frozen M0 and M4 models;
3. alerts are written to a per-batch JSON-lines file (atomic, overwritten if
   the batch is re-run) and to the Kafka alert topic;
4. the monitor state is saved with the batch id, so a batch that Spark
   re-delivers after a restart is skipped.

Delivery is at-least-once end to end: Spark's checkpoint replays an
unfinished batch, de-duplication removes repeated records, and alert messages
can repeat after a crash (consumers de-duplicate by alert ``record_id``).
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from foodshield.streaming.monitor import OnlineMonitor, load_cell_weights, load_frozen_models
from foodshield.streaming.winsafe import write_text_atomic

RECORD_SCHEMA = StructType([
    StructField("schema_version", IntegerType()), StructField("record_id", StringType()),
    StructField("record_type", StringType()), StructField("country_code", StringType()),
    StructField("commodity_code", StringType()), StructField("source", StringType()),
    StructField("series_id", StringType()), StructField("observation_date", StringType()),
    StructField("available_at", StringType()), StructField("feature_set_version", StringType()),
    StructField("prectotcorr_mm", DoubleType()), StructField("t2m_max_c", DoubleType()),
    StructField("t2m_min_c", DoubleType()), StructField("price_real_lcu_per_tonne", DoubleType()),
    StructField("annual_field", StringType()), StructField("data_year", IntegerType()),
    StructField("value", DoubleType()), StructField("unit", StringType()),
])
RECORD_TYPES = ("weather_daily", "price_monthly", "annual")


def make_streaming_spark(root: Path, cfg: dict[str, Any]) -> SparkSession:
    hadoop_home = os.environ.get("HADOOP_HOME")
    if os.name == "nt":
        if not hadoop_home:
            sys.exit("On Windows, set HADOOP_HOME to the Hadoop 3.5.0 native files (winutils.exe, hadoop.dll); "
                     "Spark needs them for streaming checkpoints. See docs/WEEK6_STREAMING.md.")
        os.environ["PATH"] = f"{Path(hadoop_home, 'bin')}{os.pathsep}{os.environ.get('PATH', '')}"
    temp_root = (root / cfg["work_root"]) / ".spark-tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    os.environ["TEMP"] = os.environ["TMP"] = str(temp_root)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    jars_dir = root / cfg["spark"]["kafka_jars_dir"]
    jars = sorted(str(p) for p in jars_dir.glob("*.jar"))
    needed = ("spark-sql-kafka-0-10", "spark-token-provider-kafka-0-10", "kafka-clients", "commons-pool2")
    missing = [n for n in needed if not any(n in Path(j).name for j in jars)]
    if missing:
        sys.exit(f"Missing Spark Kafka connector jars in {jars_dir}: {missing}")
    spark = (
        SparkSession.builder.appName("FoodShield-Week6-Streaming")
        .master(cfg["spark"]["master"])
        .config("spark.driver.memory", cfg["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(cfg["spark"]["shuffle_partitions"]))
        .config("spark.jars", ",".join(jars))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(temp_root))
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class _ProgressLog(StreamingQueryListener):
    def __init__(self, path: Path):
        self.path = path

    def onQueryStarted(self, event):  # noqa: N802
        pass

    def onQueryProgress(self, event):  # noqa: N802
        p = event.progress
        row = {"batch_id": p.batchId, "timestamp": p.timestamp, "num_input_rows": p.numInputRows,
               "input_rows_per_second": p.inputRowsPerSecond, "processed_rows_per_second": p.processedRowsPerSecond,
               "duration_ms": dict(p.durationMs)}
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")

    def onQueryIdle(self, event):  # noqa: N802
        pass

    def onQueryTerminated(self, event):  # noqa: N802
        pass


def run_consumer(root: Path, cfg: dict[str, Any], run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    stream_cfg, topics = cfg["streaming"], cfg["topics"]
    bootstrap = cfg["kafka"]["bootstrap"]
    state_path, alerts_dir = run_dir / "monitor_state.json", run_dir / "alerts"
    batches_log, status_path, stop_path = run_dir / "batches.jsonl", run_dir / "consumer_status.json", run_dir / "STOP"
    alerts_dir.mkdir(exist_ok=True)

    models = load_frozen_models(root / cfg["models"]["frozen_report"], cfg["models"]["variant"],
                                int(cfg["models"]["annual_lag"]), cfg["models"]["stages"])
    monitor = OnlineMonitor(load_cell_weights(root / cfg["inputs"]["grid_cells"]), models, int(cfg["models"]["annual_lag"]))
    resumed = monitor.load(state_path)
    totals = {"rows": 0, "skipped_redelivered_batches": 0, "alerts": 0, "restarts": int(resumed)}
    if status_path.exists():
        previous = json.loads(status_path.read_text(encoding="utf-8"))
        totals.update({k: previous.get(k, v) for k, v in totals.items()})
        totals["restarts"] = previous.get("restarts", 0) + 1

    from kafka import KafkaProducer
    alert_producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=0)

    spark = make_streaming_spark(root, cfg)
    spark.streams.addListener(_ProgressLog(run_dir / "progress.jsonl"))
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topics["observations"]["name"])
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", int(stream_cfg["max_offsets_per_trigger"]))
           .option("failOnDataLoss", "true")
           .load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), RECORD_SCHEMA).alias("r"),
                         F.col("timestamp").alias("kafka_ts"))
              .select("r.*", "kafka_ts")
              .withColumn("available_ts", F.to_timestamp("available_at"))
              .filter(F.coalesce(F.col("record_id").isNotNull() & (F.col("schema_version") == 1)
                                 & F.col("record_type").isin(*RECORD_TYPES) & F.col("available_ts").isNotNull(),
                                 F.lit(False))))
    # Malformed messages are dropped before de-duplication; the report reconciles
    # Kafka input rows (progress log) with processed rows: the gap must equal the
    # duplicates the producer sent.
    deduped = parsed.withWatermark("available_ts", stream_cfg["dedupe_watermark"]).dropDuplicatesWithinWatermark(["record_id"])

    def process(df, batch_id: int) -> None:
        started = time.time()
        if batch_id <= monitor.last_batch_id:
            totals["skipped_redelivered_batches"] += 1
            return
        df = df.persist()
        counts = {r["record_type"]: r["count"] for r in df.groupBy("record_type").count().collect()}
        valid = df
        weather = (valid.filter(F.col("record_type") == "weather_daily")
                   .groupBy("series_id", F.substring("observation_date", 1, 7).alias("month"))
                   .agg(F.sum("prectotcorr_mm"), F.sum("t2m_max_c"), F.sum("t2m_min_c"),
                        F.count("prectotcorr_mm"), F.count("t2m_max_c"), F.count("t2m_min_c"), F.count(F.lit(1)),
                        F.expr("bit_or(shiftleft(1L, dayofmonth(to_date(observation_date))))"),
                        (F.max(F.unix_micros("kafka_ts")) / 1e6))
                   .collect())
        others = (valid.filter(F.col("record_type") != "weather_daily")
                  .select("record_type", "observation_date", "price_real_lcu_per_tonne", "annual_field", "data_year",
                          "value", (F.unix_micros("kafka_ts") / 1e6).alias("ts"))
                  .orderBy("observation_date").collect())
        df.unpersist()
        monitor.add_weather_partials(sorted((tuple(r) for r in weather), key=lambda t: (t[1], t[0])))
        for r in others:
            if r["record_type"] == "price_monthly":
                monitor.add_price(r["observation_date"][:7], r["price_real_lcu_per_tonne"], r["ts"])
            else:
                monitor.add_annual(r["annual_field"], int(r["data_year"]), r["value"])
        alerts = monitor.score_ready()
        if alerts:
            for a in alerts:
                alert_producer.send(cfg["topics"]["alerts"]["name"], key=a["record_id"].encode(),
                                    value=json.dumps(a).encode())
            alert_producer.flush()
            emitted = time.time()
            for a in alerts:
                a["emitted_epoch"] = emitted
                a["latency_s"] = emitted - a["inputs_complete_kafka_ts"] if a["inputs_complete_kafka_ts"] else None
                a["batch_id"] = batch_id
            write_text_atomic(alerts_dir / f"batch-{batch_id:06d}.jsonl", "".join(json.dumps(a) + "\n" for a in alerts))
        monitor.last_batch_id = batch_id
        monitor.save(state_path)
        totals["rows"] += sum(counts.values())
        totals["alerts"] += len(alerts)
        with batches_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"batch_id": batch_id, "rows": counts, "alerts": len(alerts),
                                     "months_closed": len(alerts) // max(len(models), 1),
                                     "process_seconds": round(time.time() - started, 3), "end_epoch": time.time()}) + "\n")
        status = {**totals, "last_batch_id": batch_id, "months_evaluated": len(monitor.scored),
                  "last_month": monitor.scored[-1] if monitor.scored else None, "counters": monitor.counters,
                  "updated_epoch": time.time()}
        # Progress only: if the runner happens to hold the file, the next batch updates it.
        write_text_atomic(status_path, json.dumps(status), required=False)

    query = (deduped.writeStream.foreachBatch(process)
             .option("checkpointLocation", str(run_dir / "checkpoint"))
             .trigger(processingTime=f"{float(stream_cfg['trigger_seconds'])} seconds")
             .start())
    (run_dir / "READY").write_text(str(time.time()), encoding="utf-8")
    try:
        while query.isActive and not stop_path.exists():
            query.awaitTermination(1)
        if query.exception():
            raise RuntimeError(str(query.exception()))
    finally:
        if query.isActive:
            query.stop()
        alert_producer.close()
        spark.stop()
