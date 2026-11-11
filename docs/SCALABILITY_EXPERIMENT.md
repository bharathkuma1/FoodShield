# FoodShield scalability experiment design

**Status:** Measurement protocol. Following it, the Week 7 batch series and the streaming series were run in Week 7; results are in [`WEEK7_SCALABILITY.md`](WEEK7_SCALABILITY.md). The single reference runs below came first.

**Scope:** Spark batch processing and the TA-required local Kafka/Spark Structured Streaming replay. Wheat remains the only research commodity.

## Why the experiment is part of this project

The course rubric calls for scalability/performance goals and an evaluation with experimental design, metrics, plots, and analysis. FoodShield's research data integration and alerting workflow supplies a relevant workload, while controlled scale tests will show how its processing cost changes as input volume and available Spark resources change. The current pilot's modest monthly coverage is not, by itself, evidence of scale.

## Separate analytical evidence from engineering benchmarks

- Run the analytical workflow on the profiled source inputs and report data-quality, feature-coverage, and model results only from eligible real observations.
- Use a clearly labeled benchmark-only synthetic workload when the observed source data are too small to expose scaling behavior. Preserve the project schema, add an explicit benchmark-series identifier to maintain unique keys, and do not use these expanded rows for model training, event validation, reported source coverage, or food-market conclusions.
- Keep the source-data run and synthetic scale run in separate output locations and profiles. Record the workload-generation method and seed so another teammate can recreate it.
- Describe local Spark results as single-machine scaling unless the experiment actually uses and measures a multi-node cluster.

## Batch experiment

1. Fix a reproducible pipeline slice, input schema, transformations, output format, and correctness checks.
2. Record a source-based reference run. Then run a sequence of documented benchmark workload sizes, increasing one workload dimension at a time while keeping the transformation logic fixed.
3. Compare Spark resource configurations one factor at a time (for example, local worker parallelism, shuffle partitioning, and driver/executor memory where applicable). Stay within the host's available resources and record the machine/runtime configuration.
4. Repeat runs consistently, include a warm-up policy, and record the measurement boundary so file-reading, transformation, and Parquet-writing costs are clear.

Report elapsed time, processed-record throughput, speedup relative to a declared reference, scaling efficiency where meaningful, and join/shuffle or spill indicators available from Spark. Plot runtime and throughput against workload size and resource configuration. Preserve failures and resource-limit effects in the analysis; do not extrapolate past measured conditions.

## Streaming experiment

Use the same documented replay event schema and a fixed local Kafka/Spark configuration. Measure valid-record accounting, sustained and burst throughput, and end-to-end alert latency (including p50 and p95) under clearly specified replay pacing. Keep the professor/TA-inspired values in `PLAN.md` labeled as proposed targets until observed. A replay benchmark demonstrates the local architecture; it is not a live-feed service or evidence of predictive performance.

## Reporting boundary

The professor-facing manuscript may omit source extract row counts. It should still state the benchmark workload sizes, input/output sizes, machine/runtime settings, and observed metrics needed to reproduce and interpret the scalability experiment. Clearly label benchmark-only synthetic inputs and keep their results out of the food-supply research conclusions.

## Measured so far (reference runs, not the scaling series)

These are single runs on one machine: Claude cloud workspace, Linux, 2 cores, 7 GB RAM, Spark 4.3.0 `local[2]`. They have no repeats and no warm-up policy, so they are reference points for the Week 7 series, not scaling results. Details: [`D9_SCALE.md`](D9_SCALE.md).

| Workload | Input | Steps measured | Result |
|---|---|---|---|
| D9-A: full FAOSTAT QCL + TCL + PP archives | 22.7 million rows, 3.2 GB of CSV | Unzip, CSV count, Parquet write (year-partitioned), Parquet count, the same filter query on CSV and Parquet | 248 MB of Parquet; whole run 2 min 40 s; query 11–44× faster on Parquet |
| D9-B: gridded daily weather | 1,864,120 rows, 68 MB of CSV | Read and count; daily → monthly per cell → wheat-weighted national | 2.8 s and 8.3 s |
| Week 6: Kafka → Structured Streaming replay | 1,864,486 records (+1% duplicates) | Producer and consumer rates, backlog, alert latency (p50/p95/max) | About 5,000 events/s at full speed, alert p95 2.6 s; at a paced 250 or 100 events/s the p95 was 1.9 or 1.7 s (`WEEK6_STREAMING.md`) |

## Next dependency

The Week 4 outcome, horizon, threshold method and split are now fixed (D1–D4). The Week 7 batch series can start from the D9-A Parquet slice:

- repeat each run with a warm-up;
- vary workload size with the labelled synthetic expansion (D9-E);
- vary Spark parallelism, shuffle partitions and driver memory one factor at a time.

The streaming series can start from the Week 6 replay (`scripts/run_week6_replay.py`), varying the offered rate, partitions, trigger interval and `maxOffsetsPerTrigger`.
