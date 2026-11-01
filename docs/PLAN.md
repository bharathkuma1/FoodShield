# FoodShield 8-week roadmap

The roadmap follows the project's eight-week schedule and incorporates the TA's local replay requirement. It maps the submitted proposal's milestones to the agreed implementation sequence while preserving the proposal's scope.

| Week | Focus and deliverables | Dependencies | Acceptance criteria |
|---|---|---|---|
| 1 | Data discovery and ingestion spike; source inventory, sample downloads, quality/coverage profile; compare candidate countries and draft event definitions. Workstream owners: Bharath—price/baseline; Abhijith—production; Jyoti—weather; Naman—trade/event evidence. | None | Shared profiles and reproducible download references are documented; working country/source/event windows, availability lag, weather coverage limits, and unresolved rights are recorded with rationale. All four Week 1 feasibility handoffs are complete; implementation/validation gates remain. See `docs/WEEK1_TASK_ASSIGNMENTS.md` and `docs/DECISIONS.md`. |
| 2 | Bronze/Silver/Gold ETL with Spark; schemas, normalization, Parquet storage, data-quality checks. Agreed owners carried forward from W1: Bharath—price/M0; Abhijith—production; Jyoti—weather; Naman—trade. | W1 source/coverage decisions | A small reproducible run produces layered datasets at documented paths; schemas and row/key quality checks are inspectable. See `docs/WEEK2_TASK_ASSIGNMENTS.md` and `docs/WEEK2_ETL.md`. |
| 3 | Spark analytics and feature engineering; country–commodity–month alignment, price/weather/production/trade features, explicit temporal availability and lags. Proposed owners: Bharath—price/M0 features; Abhijith—production availability/features; Jyoti—weather aggregation/features; Naman—trade features and integration-quality reporting. | W2 normalized datasets | Feature definitions and timing rules are documented; joins have completeness summaries and no known future leakage in a reviewed sample. See `docs/WEEK3_TASK_ASSIGNMENTS.md`; ownership is proposed for team confirmation. |
| 4 | Price-only M0 baseline; define the observed future-stress outcome, forecast horizon, thresholds, and chronological train/validation/test split. Start with 3–5 pre-event training years, extending or using rolling-origin folds when the verified history supports it. | W1 source/outcome feasibility; W3 price features and availability rules | M0 can be run reproducibly on pre-event data and evaluated on untouched later periods/events against observed outcomes; threshold selection uses no test data, and missed events/false alarms are reported. |
| 5 | Multi-signal progression M1–M4 and ablation. | W3 features; W4 baseline | Each feasible stage has a reproducible run; unavailable signals are explicitly reported; comparison uses consistent event/holdout definitions. |
| 6 | Local Kafka replay and Spark Structured Streaming; produce replayed observations and stress alerts. | Stable event schema and at least one processed historical slice | One documented local run replays records end-to-end and emits traceable alerts; measured throughput/latency is reported against proposed targets, with configuration and limitations. |
| 7 | Evaluation and scalability: lead time, precision, recall, F1, false-positive rate; streaming latency/throughput; batch runtime, throughput, and scaling behavior. | W4–W6 outputs | Results tables distinguish targets from observations; methods, data volume, machine/runtime settings, and limitations are recorded. |
| 8 | Dashboard, final report, presentation, reproducibility package, and demo. | W1–W7 evidence | Demo can be repeated from written steps; report and slides state scope, methods, results, limitations, and no unsupported claims. |

## Current roadmap and teammate status

**Status snapshot:** Week 4 handoff. Percentages are completion of the documented workstream deliverable in this workspace, not personal effort, attendance, or teammate approval.

The end-to-end per-teammate allocation and detailed progress basis for all eight weeks are in [`docs/TEAM_RESPONSIBILITIES.md`](TEAM_RESPONSIBILITIES.md). Week 1 and Week 2 assignments are agreed; Weeks 3–8 are proposed/provisional allocations to revise as project visibility improves.

| Week | Bharath Kumar | Abhijith C R | Jyoti Prakash Das | Naman Singh Shekhawat | Roadmap status |
|---|---:|---:|---:|---:|---|
| 1 — source feasibility | 100% | 100% | 100% | 100% | Complete as a documented feasibility handoff. |
| 2 — Spark ETL | 100% | 100% | 100% | 100% | Complete as a reproducible local implementation/run; ownership carried forward from Week 1 was agreed. |
| 3 — analytics/features | 0% | 0% | 0% | 0% | Not started; proposed ownership is documented in `docs/WEEK3_TASK_ASSIGNMENTS.md`, pending teammate confirmation. |
| 4 — M0 baseline/evaluation protocol | 0% | 0% | 0% | 0% | Not started; depends on Week 3 features and target/split decisions. |
| 5 — M1–M4 and ablation | 0% | 0% | 0% | 0% | Not started. |
| 6 — Kafka/Spark streaming replay | 0% | 0% | 0% | 0% | Not started. |
| 7 — evaluation/scalability | 0% | 0% | 0% | 0% | Not started. |
| 8 — dashboard/submission/demo | 0% | 0% | 0% | 0% | Not started. |

By equal weekly milestone count, 2 of 8 roadmap weeks are complete (25%); this is a schedule snapshot, not an effort-weighted project-completion estimate. Week 1 and Week 2 tracker details and their acceptance boundaries are in `docs/WEEK1_TASK_ASSIGNMENTS.md` and `docs/WEEK2_TASK_ASSIGNMENTS.md`. Week 1 is feasibility only; Week 2 completion does not clear data rights, representative weather, target definition, or evaluation gates.

## Proposed local streaming benchmark targets

These are engineering targets proposed for this project, not values prescribed by the TA and not achieved results. The TA note gives 1,000 events/s sustained, 2,000 events/s peak, and p95 latency ≤10 s as examples, while cautioning that the numbers should be chosen deliberately. These initial lower targets were drafted for the current maintainer's 8 GB Windows host; that hardware is not a team-wide requirement. Tune each local run to available resources and record host details, while keeping a documented reference configuration for comparisons:

| Measure | Proposed target | Measurement definition |
|---|---:|---|
| Sustained input throughput | ≥100 events/s for 10 minutes | Successfully processed input events divided by steady-state elapsed time; use a fixed, documented event schema and replay workload. |
| Short burst throughput | ≥250 events/s for 60 seconds | Processed events/s during a fixed burst with the same local configuration. |
| End-to-end alert latency | p95 ≤10 seconds | Event publication timestamp to corresponding alert output timestamp; report p50 and p95, excluding intentional replay pacing. |
| Replay completeness | 100% of valid input records accounted for | Compare input and processed/output accounting under a clean run; define invalid records separately. This is a target to verify, not exactly-once delivery semantics. |

Run in local mode with bounded batches and a fixed small payload; report Windows/laptop configuration, Kafka/Spark versions, trigger interval, partitions, batch size, warm-up, duration, and measurement method. If the target is not met, record the measured result and bottleneck instead of presenting it as achieved. Start with a small sample before increasing volume. The throughput values are pragmatic proposed thresholds; confirm them against an initial measurement before treating them as a project commitment.

## Cross-week dependencies and scope gates

- Workstreams proceed in parallel when their input contracts are clear; do not require every teammate to finish a weekly task before others continue. A milestone is complete only when its shared dependency artifacts and acceptance evidence are reviewed. See `docs/TEAM_RESPONSIBILITIES.md` for the collaboration rule.
- Data feasibility gates the signal set and geographic scope; retain a clear M0 and evaluation protocol even if a candidate source fails.
- Week 4 M0 can proceed from the price feature contract while weather, production, or trade subtasks continue; M1–M4 comparisons require the applicable features and common-period coverage evidence. Target and holdout rules still require a shared decision before evaluation claims.
- Streaming consumes a reviewed stable processed-record contract and at least one processed slice. Its producer, consumer, alert, and replay-configuration tasks can be developed in parallel against that contract; the end-to-end replay acceptance waits for their integration.
- Keep the laptop workload bounded. If full data or all signal stages exceed local resources, document the subset and preserve reproducibility rather than silently claiming full coverage.
- Reserve W8 for communication and repeatability, not first-time data discovery or unmeasured performance claims.
