# FoodShield scalability experiment design

**Status:** Proposed measurement protocol; no scalability result is claimed here.

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

## Next dependency

Week 4 must first define an eligible future-stress outcome, forecast horizon, threshold-selection method, and chronological evaluation periods. The scalability benchmark can proceed separately once its fixed pipeline slice and workload schema are stable; it does not need to wait for model-performance results.
