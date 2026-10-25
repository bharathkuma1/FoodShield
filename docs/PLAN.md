# FoodShield 8-week roadmap

The roadmap follows the project's eight-week schedule and incorporates the TA's local replay requirement. It maps the submitted proposal's milestones to the agreed implementation sequence while preserving the proposal's scope.

| Week | Focus and deliverables | Dependencies | Acceptance criteria |
|---|---|---|---|
| 1 | Data discovery and ingestion spike; source inventory, sample downloads, quality/coverage profile; select candidate countries and event definitions. | None | Feasibility table records source/version, frequency, coverage, identifiers, units, missingness, access method, and known limitations. Selection rationale documented; no source presumed available. |
| 2 | Bronze/Silver/Gold ETL with Spark; schemas, normalization, Parquet storage, data-quality checks. | W1 source/coverage decisions | A small reproducible run produces layered datasets at documented paths; schemas and row/key quality checks are inspectable. |
| 3 | Spark analytics and feature engineering; country–commodity–month alignment, price/weather/production/trade features, explicit temporal availability and lags. | W2 normalized datasets | Feature definitions and timing rules are documented; joins have completeness summaries and no known future leakage in a reviewed sample. |
| 4 | Price-only M0 baseline; define thresholding and historical evaluation protocol. | W1 event definitions; W3 price features | M0 can be run reproducibly on selected history and reports detection timestamps and agreed quality metrics, including false positives. |
| 5 | Multi-signal progression M1–M4 and ablation. | W3 features; W4 baseline | Each feasible stage has a reproducible run; unavailable signals are explicitly reported; comparison uses consistent event/holdout definitions. |
| 6 | Local Kafka replay and Spark Structured Streaming; produce replayed observations and stress alerts. | Stable event schema and at least one processed historical slice | One documented local run replays records end-to-end and emits traceable alerts; measured throughput/latency is reported against proposed targets, with configuration and limitations. |
| 7 | Evaluation and scalability: lead time, precision, recall, F1, false-positive rate; streaming latency/throughput; batch runtime, throughput, and scaling behavior. | W4–W6 outputs | Results tables distinguish targets from observations; methods, data volume, machine/runtime settings, and limitations are recorded. |
| 8 | Dashboard, final report, presentation, reproducibility package, and demo. | W1–W7 evidence | Demo can be repeated from written steps; report and slides state scope, methods, results, limitations, and no unsupported claims. |

## Proposed local streaming benchmark targets

These are engineering targets proposed for this project, not values prescribed by the TA and not achieved results. The TA note gives 1,000 events/s sustained, 2,000 events/s peak, and p95 latency ≤10 s as examples, while cautioning that the numbers should be chosen deliberately. For an 8 GB Windows laptop, start with a lighter, measurable target:

| Measure | Proposed target | Measurement definition |
|---|---:|---|
| Sustained input throughput | ≥100 events/s for 10 minutes | Successfully processed input events divided by steady-state elapsed time; use a fixed, documented event schema and replay workload. |
| Short burst throughput | ≥250 events/s for 60 seconds | Processed events/s during a fixed burst with the same local configuration. |
| End-to-end alert latency | p95 ≤10 seconds | Event publication timestamp to corresponding alert output timestamp; report p50 and p95, excluding intentional replay pacing. |
| Replay completeness | 100% of valid input records accounted for | Compare input and processed/output accounting under a clean run; define invalid records separately. This is a target to verify, not exactly-once delivery semantics. |

Run in local mode with bounded batches and a fixed small payload; report Windows/laptop configuration, Kafka/Spark versions, trigger interval, partitions, batch size, warm-up, duration, and measurement method. If the target is not met, record the measured result and bottleneck instead of presenting it as achieved. Start with a small sample before increasing volume. The throughput values are pragmatic proposed thresholds; confirm them against an initial measurement before treating them as a project commitment.

## Cross-week dependencies and scope gates

- Data feasibility gates the signal set and geographic scope; retain a clear M0 and evaluation protocol even if a candidate source fails.
- Streaming consumes a stable processed-record contract. It is a separate replay demonstration and does not replace historical M0–M4 evaluation.
- Keep the laptop workload bounded. If full data or all signal stages exceed local resources, document the subset and preserve reproducibility rather than silently claiming full coverage.
- Reserve W8 for communication and repeatability, not first-time data discovery or unmeasured performance claims.
