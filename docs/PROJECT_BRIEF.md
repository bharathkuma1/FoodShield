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

Compare stages through historical validation and ablation. Use holdout periods where feasible. Annual structural signals must only become available at valid dates, with explicit lags to prevent future-information leakage.

## Architecture and candidate data

**Confirmed technology direction:** Python, Apache Spark/PySpark, Spark DataFrames and Spark SQL, Parquet, Kafka, and Spark Structured Streaming. Spark is the primary distributed processing platform. Spark MLlib is optional where appropriate.

**Batch path:** public source files → profiling and normalization → layered Parquet datasets → country/commodity/month alignment and feature engineering → M0–M4 experiments → historical-event evaluation.

**Streaming demonstration:** historical/processed observations → local replay producer → Kafka → Spark Structured Streaming → stress calculation/score → alert output. This demonstrates replay behavior, not a live-feed service.

**Candidate sources (not yet confirmed feasible):** FAO food-price data; FAOSTAT crop production and agrifood trade; NASA POWER daily meteorology. Profile coverage, geographic/commodity mappings, frequency, units, revisions, access constraints, and join quality before selecting them. The proposal also mentions supply-related indicators; food-balance data is a possible candidate from the discussion deck, subject to feasibility.

## Evaluation

Measure detection lead time, precision, recall, F1, and false-positive rate; ablation contribution; data quality (missingness, join completeness, temporal alignment, reproducibility); and batch processing runtime, throughput, speedup, scaling efficiency, and join/shuffle cost. Add streaming throughput and end-to-end alert latency under a documented local configuration. Record targets separately from observed results.

## Team

- Bharath Kumar
- Abhijith C R (proposal files shorten the name to “Abhijith”)
- Jyoti Prakash Das
- Naman Singh Shekhawat

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
artifacts/         # generated metrics, charts, demo outputs
```

Keep notebooks optional and exploratory; promote repeatable pipeline logic into `src/`. Avoid duplicating submitted proposal materials under `docs/`.
