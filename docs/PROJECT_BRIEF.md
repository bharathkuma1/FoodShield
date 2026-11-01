# FoodShield project brief

## Identity and objective

**Project:** FoodShield: A Scalable Multi-Signal Early-Warning Platform for Food Supply Shocks  
**Course/team:** IISc DA231o Data Engineering at Scale, Data-Flux  
**Research question:** Can multi-source temporal signals detect emerging food-supply stress earlier than price-only monitoring?

FoodShield will integrate heterogeneous public data and study historical wheat-market stress. It aims to detect observable supply-stress signals earlier than a price-only approach. It does not aim to predict geopolitical events or their causes, and results must not imply causality.

## Authoritative scope

The submitted proposal defines the core scope: wheat as the initial commodity; historical analysis and documented stress-event validation; a country–commodity–month analytical grain; a price-only baseline compared with progressively richer signals; temporal feature engineering; and Spark processing/scalability experiments. Candidate countries and exact datasets are to be selected after profiling. Additional commodities, logistics signals, and global real-time feeds remain extensions unless separately agreed.

The TA feedback is a required addition that extends the execution architecture without replacing the proposal's batch research: replay historical/processed records locally through Kafka, consume them with Spark Structured Streaming, calculate stress, and emit alerts. No live production feed is required.

## Model progression

| Stage | Signals and role |
|---|---|
| M0 | Price-only baseline |
| M1 | Price + weather |
| M2 | Price + weather + production |
| M3 | Price + weather + production + trade |
| M4 | All signals, temporal features, lagged relationships, and cross-signal interactions |

Compare stages through chronological validation and ablation. Train/calibrate on years before the test event, use a separate validation slice to select thresholds, and reserve later event periods as untouched tests. A 3–5 year training span is a reasonable initial minimum, not a required cap; at monthly grain it is only 36–60 rows, so use simple detectors and extend the training history or use rolling-origin evaluation when source coverage permits. At each prediction month, features must be limited to information available by that date, and the outcome must be measured only afterward. Annual structural signals require explicit availability lags to prevent future-information leakage.

## Architecture and candidate data

**Confirmed technology direction:** Python, Apache Spark/PySpark, Spark DataFrames and Spark SQL, Parquet, Kafka, and Spark Structured Streaming. Spark is the primary distributed processing platform. Spark MLlib is optional where appropriate.

**Batch path:** public source files → profiling and normalization → layered Parquet datasets → country/commodity/month alignment and feature engineering → M0–M4 experiments → historical-event evaluation.

**Streaming demonstration:** historical/processed observations → local replay producer → Kafka → Spark Structured Streaming → stress calculation/score → alert output. This demonstrates replay behavior, not a live-feed service.

**Candidate sources (not yet confirmed feasible):** FAO food-price data; FAOSTAT crop production and agrifood trade; NASA POWER daily meteorology. Profile coverage, geographic/commodity mappings, frequency, units, revisions, access constraints, and join quality before selecting them. The proposal also mentions supply-related indicators; food-balance data is a possible candidate from the discussion deck, subject to feasibility.

## Evaluation

Measure detection lead time, precision, recall, F1, and false-positive rate against an explicit observed outcome; ablation contribution; data quality (missingness, join completeness, temporal alignment, reproducibility); and batch processing runtime, throughput, speedup, scaling efficiency, and join/shuffle cost. Add streaming throughput and end-to-end alert latency under a documented local configuration. Record targets separately from observed results. A future price-stress outcome measures wheat-market price stress, not physical shortage; any export-flow outcome needs its own coverage and reporting-gap profile.

## Team

- Bharath Kumar
- Abhijith C R (proposal files shorten the name to “Abhijith”)
- Jyoti Prakash Das
- Naman Singh Shekhawat

### Agreed Week 1 ownership

- **Bharath Kumar — price and baseline:** profile FPMA wheat/grain and flour series for shortlisted countries; capture source, market, product, unit, frequency, coverage, and missing months.
- **Abhijith C R — production:** profile FAOSTAT wheat production, yield, and harvested area; preserve flags/units and document publication/availability lags.
- **Jyoti Prakash Das — weather:** profile NASA POWER parameters and historical coverage for wheat-producing areas; recommend a defensible spatial representation, time standard, and monthly aggregation.
- **Naman Singh Shekhawat — trade and event evidence:** profile FAOSTAT wheat trade measures and flags; compile cited GIEWS/FAO event evidence and draft candidate event definitions.

These are Week 1 workstream owners. Current feasibility evidence makes Ukraine the strongest starting geography for the downloaded input sources and keeps India as an alternative/comparator. The three historical cases in proposal development are examples, not fixed evaluation requirements: select final instances after establishing the target definition and quality-checked common coverage. The current weather extracts are point samples and do not yet establish representative national exposure. Operational prediction labels, publication rights, temporal availability, and feature coverage remain gates. These are not teammate vote records. See `docs/DECISIONS.md`, `docs/DATA_SOURCE_REGISTER.md`, `docs/WEEK1_EVENT_CASES.md`, and `docs/WEEK1_TASK_ASSIGNMENTS.md`.

### Agreed Week 2 ownership

The team agreed to carry the Week 1 signal owners into Week 2 Spark ETL: Bharath—price/M0; Abhijith—production; Jyoti—weather; Naman—trade. Week 2 implementation evidence and completion criteria are in [`docs/WEEK2_TASK_ASSIGNMENTS.md`](WEEK2_TASK_ASSIGNMENTS.md) and [`docs/WEEK2_ETL.md`](WEEK2_ETL.md).

### Proposed Week 3 ownership

For the analytics and feature-engineering milestone, the proposed workstream mapping follows the four Week 1/2 signal owners: Bharath—price/M0 features; Abhijith—production features and as-of availability; Jyoti—weather aggregation and features; Naman—trade features plus cross-signal join/completeness integration. This is a proposed assignment, not recorded teammate sign-off. Shared acceptance criteria and boundaries are in [`docs/WEEK3_TASK_ASSIGNMENTS.md`](WEEK3_TASK_ASSIGNMENTS.md).

The current working responsibility allocation for all eight weeks, including provisional owners and per-week status, is maintained in [`docs/TEAM_RESPONSIBILITIES.md`](TEAM_RESPONSIBILITIES.md). Weeks 4–8 remain adjustable planning assignments, not frozen commitments.

## Recommended project structure

Recommended layout for source code, configuration, data, and generated outputs. Keep submitted proposal and presentation artifacts in their existing locations.

```text
AGENTS.md
docs/
  PROJECT_BRIEF.md
  PLAN.md
  DECISIONS.md
src/foodshield/
  ingestion/       # source readers and profiling
  transforms/      # bronze/silver/gold and joins
  features/        # temporal and cross-signal features
  models/          # M0–M4 scoring/detection
  streaming/       # replay producer, stream consumer, alerts
  evaluation/      # event labels and metrics
configs/           # small, explicit run configurations
scripts/           # bounded local entry points
tests/             # targeted unit and data-contract checks
data/              # local raw/bronze/silver/gold; keep on D:, do not commit
  synthetic/       # clearly marked development fixtures; never final evaluation evidence
artifacts/         # generated metrics, charts, demo outputs
```

Keep notebooks optional and exploratory; promote repeatable pipeline logic into `src/`. Avoid duplicating submitted proposal materials under `docs/`.

The current architecture baseline and implemented Week 2 paths are documented in [`docs/ARCHITECTURE.md`](ARCHITECTURE.md); the local Spark runner, schema/grain contracts, outputs, and setup steps are in [`docs/WEEK2_ETL.md`](WEEK2_ETL.md). Treat the tree above as the target organization and those implementation notes as the current state; model, feature, evaluation, and streaming packages remain planned work.

Synthetic development fixtures are generated only to unblock pipeline and model-interface development. Their generation recipe, seed, split, target rule, and limitations are in [`docs/SYNTHETIC_DATA.md`](SYNTHETIC_DATA.md). They must remain identifiable and must not be used to claim observed coverage or final evaluation performance.

The continuing professor-facing project manuscript, including the wheat-only scope, methods, evidence, software prerequisites, and current reproducibility instructions, is [`docs/PROJECT_SUBMISSION.md`](PROJECT_SUBMISSION.md).
