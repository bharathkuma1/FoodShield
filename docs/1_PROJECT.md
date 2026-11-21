# 1. FoodShield: project, plan and team

**Project:** FoodShield: A Scalable Multi-Signal Early-Warning Platform for Food Supply Shocks
**Course/team:** IISc DA231o Data Engineering at Scale, Data-Flux
**Research question:** Can multi-source temporal signals warn of future Ukraine wheat farm-gate producer-price stress earlier or more reliably than price-only monitoring?

Related documents: data sources in [`2_DATA.md`](2_DATA.md), pipeline design and setup in [`3_ARCHITECTURE.md`](3_ARCHITECTURE.md), models and results in [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md), streaming in [`5_STREAMING.md`](5_STREAMING.md), scalability in [`6_SCALABILITY.md`](6_SCALABILITY.md), the decision log in [`7_DECISIONS.md`](7_DECISIONS.md), and every processing step with code in [`8_PIPELINE_WALKTHROUGH.md`](8_PIPELINE_WALKTHROUGH.md).

## 1.1 Objective and scope

FoodShield integrates heterogeneous public data and studies historical wheat-market stress. For the Ukraine pilot, the outcome is future wheat farm-gate producer-price stress from the public FAOSTAT monthly producer-price series; CPI-adjusted analyses use the attributed SSSU all-items CPI chain. It aims to warn of that future price stress earlier or more reliably than a price-only approach. It does not aim to predict geopolitical events or physical shortages, and results must not imply causality.

The submitted proposal defines the research scope: wheat as the commodity, with Ukraine as the selected pilot; historical analysis and documented stress-event validation; a country–commodity–month analytical grain; a price-only baseline compared with progressively richer signals; temporal feature engineering; and Spark processing and scalability experiments. The pipeline is structured so country and commodity identifiers, source paths and metadata, and target and split choices can be replaced through configuration for a separately profiled run. Reuse of that framework does not expand the current wheat study or imply that every country or commodity has equivalent public data. Additional commodities, logistics signals, global real-time feeds and a multi-country panel are extension work (D13).

The TA feedback is a required addition that extends the execution architecture without replacing the proposal's batch research: replay historical and processed records locally through Kafka, consume them with Spark Structured Streaming, calculate stress, and emit alerts. No live production feed is required.

**Technology:** Python, Apache Spark/PySpark (DataFrames, Spark SQL, MLlib), Parquet, Apache Kafka and Spark Structured Streaming.

- **Batch path:** public source files → profiling and normalisation → layered Parquet datasets → country/commodity/month alignment and feature engineering → M0–M4 experiments → historical evaluation.
- **Streaming demonstration:** historical observations → local replay producer → Kafka → Spark Structured Streaming → stress score → alert output. This demonstrates replay behaviour, not a live-feed service.

## 1.2 Model progression

| Stage | Signals and role |
|---|---|
| M0 | Price-only baseline |
| M1 | Price + weather |
| M2 | Price + weather + production |
| M3 | Price + weather + production + trade |
| M4 | All signals plus a cross-signal (price × rainfall) interaction |

Stages are compared through chronological validation and ablation on identical months with one estimator. At each prediction month, features use only information available by that date and the outcome is measured only afterwards; annual signals carry explicit availability lags. The evaluation measures detection lead time, precision, recall, F1 and false-alarm rate against the explicit future outcome, plus ablation contribution, data quality, and batch and streaming performance. Warning lead time (how early a model crosses its alert threshold before future price stress) is kept distinct from processing and alert latency (runtime from input to output). Targets are recorded separately from measured results. Definitions and results: [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md).

### Streaming benchmark targets

These engineering targets were proposed for this project; they are not values prescribed by the TA. The TA note gave 1,000 events/s sustained, 2,000 events/s peak and p95 latency ≤10 s as examples, while cautioning that the numbers should be chosen deliberately. The lower targets below were drafted for an 8 GB Windows host and kept as D12c.

| Measure | Target | Measurement definition | Measured |
|---|---:|---|---|
| Sustained input throughput | ≥100 events/s for 10 minutes | Processed input events divided by steady-state elapsed time, with a fixed, documented event schema and replay workload | Laptop: 100.0/s for 849 s; met |
| Short burst throughput | ≥250 events/s for 60 seconds | Processed events/s during a fixed burst with the same local configuration | Laptop: 250.0/s for 340 s; met |
| End-to-end alert latency | p95 ≤10 seconds | Event publication timestamp to the corresponding alert output timestamp; p50 and p95, excluding intentional replay pacing | Laptop: p95 2.9 s at 100/s and 3.1 s at 250/s; met |
| Replay completeness | 100% of valid input records accounted for | Input versus processed/output accounting under a clean run; not exactly-once delivery semantics | Every unique record processed once in every run |

Details: [`5_STREAMING.md`](5_STREAMING.md) §5.4 and [`6_SCALABILITY.md`](6_SCALABILITY.md) §6.3.

## 1.3 Eight-week plan and where each part is documented

The roadmap follows the project's eight-week schedule and incorporates the TA's local replay requirement.

| Week | Focus and deliverables | Acceptance criteria | Delivered | Documented in |
|---|---|---|---|---|
| 1 | Data discovery: source inventory, sample downloads, quality and coverage profile; candidate countries and event definitions | Shared profiles and reproducible download references; working country, source and event windows, availability lag, weather limits and unresolved rights recorded with rationale | All four feasibility handoffs complete | [`2_DATA.md`](2_DATA.md) §2.2–2.5 |
| 2 | Bronze/Silver/Gold ETL with Spark: schemas, normalisation, Parquet, data-quality checks | A small reproducible run produces layered datasets at documented paths with inspectable schema and key checks | Reproducible local run | [`3_ARCHITECTURE.md`](3_ARCHITECTURE.md) §3.3 |
| 3 | Spark analytics and features: country–commodity–month alignment, price/weather/production/trade features, explicit availability and lags | Feature definitions and timing rules documented; joins have completeness summaries and no known future leakage in a reviewed sample | Feature panel, coverage profile, as-of review | [`3_ARCHITECTURE.md`](3_ARCHITECTURE.md) §3.4 |
| 4 | Stress target, horizon, threshold method and chronological split; M0 baseline and rolling backtest | All stages on identical origins, thresholds chosen without test data, missed events and false alarms reported | D1–D9; M0 and rolling backtest | [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.1–4.3 |
| 5 | M1–M4 and ablation; frozen protocol and one-time test | Each stage reproducible; unavailable signals reported; consistent event and holdout definitions | D10, D11; test opened once | [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.4–4.6 |
| 6 | Local Kafka replay into Spark Structured Streaming with alerts | One documented local run replays records end to end and emits traceable alerts; throughput and latency reported against targets | Built, checked, laptop runs passed every check (D12) | [`5_STREAMING.md`](5_STREAMING.md) |
| 7 | Evaluation and scalability: lead time, precision, recall, F1, false alarms; streaming latency and throughput; batch scaling | Targets distinguished from measurements; workloads, settings, plots and limitations recorded; synthetic benchmarks not presented as market evidence | Done | [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.7, [`2_DATA.md`](2_DATA.md) §2.7, [`6_SCALABILITY.md`](6_SCALABILITY.md) |
| 8 | Dashboard, final report, presentation, reproducibility package and demo | Demo repeatable from written steps; report and slides state scope, methods, results and limitations without unsupported claims | Interactive dashboard with live alerts (D15) and every stress definition to explore (D16); report and slides drafted; joint review pending | `README.md`, [`5_STREAMING.md`](5_STREAMING.md) §5.7, §1.4 below |
| D9 | Data at real scale: full FAOSTAT archives and 232-cell gridded weather | Measured timings; Ukraine reconciles with the model inputs | Done | [`6_SCALABILITY.md`](6_SCALABILITY.md) §6.4 |

**Working rules across weeks:**

- Workstreams proceed in parallel when their input contracts are clear; a milestone is complete only when its shared artifacts and acceptance evidence are reviewed.
- Data feasibility gates the signal set and geographic scope; a clear M0 and evaluation protocol are kept even if a candidate source fails.
- Keep laptop workloads bounded. If full data or all stages exceed local resources, document the subset rather than silently claiming full coverage.
- Week 8 is for communication and repeatability, not first-time data discovery or unmeasured performance claims.

## 1.4 Course submission requirements

**Source:** the professor's final-project instruction slide shared by the team. Internal checklist for one consistent team submission.

| Requirement | Expected content or format | Status |
|---|---|---|
| One report per team | Avoid duplicate submissions from team members | Pending final submission |
| Length and format | 6–8 A4 pages, 10-point font, 2.5 cm margins, PDF | The report draft is a shared Claude doc built from [`PROJECT_SUBMISSION.md`](PROJECT_SUBMISSION.md) and the results; final page layout and PDF export pending |
| Heading | Project title, authors and email address | Title and author names present; team email addresses to be supplied |
| Problem and goals | Problem definition, motivation, design goals, required features, scalability and performance goals | In the report draft; confirm at the joint review |
| Approach and methods | High-level design, architecture and data model, big-data platforms, ML methods | Architecture and Spark/Kafka design documented; data model in [`3_ARCHITECTURE.md`](3_ARCHITECTURE.md) §3.9; model methods fixed (D1–D11) |
| Evaluation | Experiment design, scalability and performance targets, metrics, plots and analysis | Model evaluation, data-quality checks, batch and streaming scaling series done with plots; laptop streaming runs passed every check. Only measured results are presented as achieved |
| Summary | Progress toward the proposal's goals and future extensions | The report draft's conclusion states the model result, the streaming and scaling assessment and future extensions; confirm at the joint review |
| Presentation slides | One deck per team | Drafted as a shared Claude slide deck (12 slides, speaker notes, demo script; exports to PowerPoint or PDF); joint review pending |
| Public GitHub repository link | One link per team; verify public access | Push `main` and confirm public access before submission |
| Contribution summary | Each member submits the MS Form with tasks performed and estimated effort | The repository records workstream ownership (§1.5); each member provides their own form response |
| Demo/presentation session | Session-selection MS Form; all members attend the full session | Session details not yet available |
| Peer evaluation | Each student completes the evaluation form for presentations in the session | Pending session and form |

The final evaluation includes only the benchmark workload sizes and measured performance details needed to make scalability results reproducible; those are experimental settings, not claims about the amount or completeness of the source data.

**Assessment dimensions:** novelty, technical effort, technical correctness, scalability and impact.

| Dimension | Current fit |
|---|---|
| Novelty | **Moderate and applied.** Leakage-aware price monitoring, heterogeneous wheat/weather/production/trade signals, historical event evaluation and a local replay path; not a new ML algorithm. |
| Technical effort | Spark M0–M4 pipeline on the selected farm-gate source with weighted weather, a frozen protocol and a one-time test; full FAOSTAT archives and gridded weather at real size; a Kafka/Spark Structured Streaming replay of 1.86 million records that passes every check and resumes after a hard restart; an interactive dashboard. |
| Technical correctness | **Strong.** Target, horizon, split and purge confirmed (D1–D3); two feature definitions corrected before the test (D10); the test pre-registered and opened once; Ukraine wheat from the full archives matches the model inputs exactly. Annual release timing is a proxy, so a 3-year sensitivity run is reported. |
| Scalability | 22.7 million FAOSTAT rows (3.2 GB) in 2 min 40 s; 186 million rows in 43 s (4.3 million rows/s) in the batch series; streaming kept up to 4,000 events/s with alert p95 ≤ 2.7 s in development runs, and met both targets on the team laptop (p95 ≤ 3.1 s). |
| Impact | **Relevant problem; mixed evidence.** Multi-signal models were worse than price-only on 2014–2020 and better on the 2021–2022 test (M4 F1 0.62 vs 0.43). Too few events to settle it. The target is farm-gate pricing, not consumer affordability or physical shortage. |

The main risk is claiming predictive value from too few independent events: 8 backtest and 3 test episodes, pointing in different directions. Report both, state the small-event limitation, and do not treat months as independent events. A multi-country panel (31 feasible countries) is the main way to add events and is extension work (D13).

## 1.5 Team and contributions

**Team:** Bharath Kumar, Abhijith C R (the proposal shortens the name to "Abhijith"), Jyoti Prakash Das, Naman Singh Shekhawat.

**Working mode from Week 6 (D14):** Claude does the work of all four workstreams in one shared place, and the whole team reviews it together. The owners below remain responsible for reviewing and accepting their workstream.

### Responsibility matrix

| Week | Bharath Kumar | Abhijith C R | Jyoti Prakash Das | Naman Singh Shekhawat | Shared outcome |
|---|---|---|---|---|---|
| **1 — source feasibility** | Price and M0 source profile | Production source profile and availability-lag proposal | Weather source profile, time and spatial method | Trade profile and event evidence | Source coverage, country feasibility, event contexts and open gates compared. **Agreed.** |
| **2 — Spark ETL** | Price Bronze/Silver and M0 Gold | Production Bronze/Silver and lag metadata | Weather Bronze/Silver and monthly screening aggregate | Trade Bronze/Silver and retained metadata | Reproducible local Spark layers, schema and grain checks, source limitations. **Agreed.** |
| **3 — alignment and features** | Price/M0 lags, rolling summaries, anomaly candidates | Annual production alignment, as-of dates, availability-aware features | Weather monthly aggregation, anomalies and lags, spatial-basis caveat | Trade alignment and features, cross-signal join completeness | Country–wheat–month feature panel with reviewed temporal and join evidence. **Agreed default.** |
| **4 — M0 and evaluation protocol** | Price-only M0 detector and scoring baseline | Observable outcome and label data, publication timing | Chronological split and event-period eligibility from verified coverage | Event evidence, evaluation protocol and metric definitions | Team review of outcome, horizon, threshold method and split before metrics are claimed. **Agreed default.** |
| **5 — M1–M4 and ablation** | M0 reference and score interface | M2 production addition and its availability sensitivity | M1 weather addition and representative-weather sensitivity | M3 trade and M4 cross-signal additions; staged ablation comparison | Feasible M0–M4 stages compared on consistent records. **Agreed default.** |
| **6 — Kafka replay and Structured Streaming** | Processed records in the agreed replay envelope | Structured Streaming input, state path and bounded run | Alert output fields and reason traceability | Kafka topics, replay orchestration and input/output accounting | One repeatable local end-to-end replay with documented configuration. **Agreed default.** |
| **7 — evaluation and scalability** | M0–M4 event metrics, lead time and price-only comparison | Feature quality, join completeness, as-of checks | Batch Spark scaling measurements | Streaming throughput, latency, replay completeness | Measured values distinguished from targets. **Agreed default.** |
| **8 — dashboard, report, presentation** | Research question, method, model progression and interpretation | Data lineage, normalisation, quality, availability and reproducibility | Dashboard and result visuals and their coverage | Demo and runbook, limitations, presentation flow, artifact checklist | Review report and demo together; reproduce a clean run. **Agreed default.** |

### Weeks 1–3 deliverables by owner

| Owner | Week 1: feasibility | Week 2: Spark ETL | Week 3: features |
|---|---|---|---|
| **Bharath Kumar** | Compared price candidates; selected the Ukraine FPMA national wheat-grain series as provisional M0, kept India as comparator, documented metadata mismatch, provenance and a no-redistribution restriction | FPMA price Bronze/Silver and Gold M0 monthly price output (804 Bronze, 804 Silver, 310 Gold rows) | Monthly key, price lags, rolling summaries and anomaly candidates; 309/310 real M0 rows complete; lag and anomaly dates hand-reviewed |
| **Abhijith C R** | Profiled India and Ukraine FAOSTAT production extracts (years, units, counts, flags); chose the conservative `data_year <= forecast_year - 2` availability rule | Production Bronze/Silver with units, flags, notes and the two-year availability lag (99 rows per layer) | Annual production, yield and area joined as-of with availability dates; 2001/2015/2022 examples reviewed |
| **Jyoti Prakash Das** | Profiled NASA POWER parameters, units, time standard and sentinels; documented the official crop-statistics archive; chose the wheat-area-weighted spatial plan | Daily weather Bronze/Silver, sentinel filtering and monthly screening aggregate (24,105 daily, 264 monthly rows) | Monthly weather summaries and anomaly candidates covering 264/310 months; January 2015 daily-to-monthly recomputation matches |
| **Naman Singh Shekhawat** | Profiled Ukraine and India trade quantity and value extracts and flags; documented bilateral mismatch risks and a cited event chronology; chose aggregate trade | Trade Bronze/Silver with units, flags and notes (132 rows per layer) | Trade joined as-of with metadata; M0–M4 row completeness and per-signal missingness profiled |

### Completion by documented workstream

Percentages indicate workstream evidence present in the repository. They are not measures of personal contribution, attendance or model success.

| Week | Bharath | Abhijith | Jyoti | Naman | Basis |
|---|---:|---:|---:|---:|---|
| 1 | 100% | 100% | 100% | 100% | Feasibility handoffs documented |
| 2 | 100% | 100% | 100% | 100% | Local ETL outputs and owner deliverables documented |
| 3 | 100% | 100% | 100% | 100% | Spark feature build, real-input as-of integration, coverage profile and hand-reviewed examples |
| 4 | 80% | 95% | 55% | 65% | Week 4 snapshot (mean 74%). Since then the target, horizon, split, M0 baseline, weighted weather, annual lag and test rule were confirmed as D1–D9 and the rolling backtest added; owners to confirm the percentages |
| 5 | — | — | — | — | Implemented; owners review their stage (M1 Jyoti, M2 Abhijith, M3/M4 Naman, M0 reference Bharath) |
| 6 | — | — | — | — | Built by Claude for all four workstreams (D14): replay records (Bharath), Structured Streaming input and state (Abhijith), alert fields and reason codes (Jyoti), Kafka topics and replay run (Naman); checked on the 2-core development machine and on the team laptop |
| 7 | — | — | — | — | Built by Claude for all four workstreams (D14): M0–M4 metrics and lead time (Bharath), data quality and as-of checks (Abhijith), batch scaling (Jyoti), streaming throughput and latency (Naman) |
| 8 | — | — | — | — | Dashboard, report draft, slides and live demo drafted (D14, D15); set at the joint review |

**Week 4 percentage basis:**

| Owner | Progress | Evidence present | Remaining |
|---|---:|---|---|
| Bharath | 80% | Implemented and ran the Spark selected-source feature panel and the price-only M0 MLlib logistic regression, trained on training origins with validation-only cutoff calibration | Review model settings, target, split and validation calibration with the team |
| Abhijith | 95% | Selected the CC BY FAOSTAT farm-gate price source, documented the SSSU reuse basis, produced the CPI-adjusted price with source hashes, generated target and horizon sensitivity, integrated the selected source into Spark | Confirm annual availability assumptions; keep provenance current |
| Jyoti | 55% | Profiled shared-source M0–M4 feature-eligible dates, common horizon coverage and a candidate chronological split | Review the split and purge; representative weather construction (since added as D5) |
| Naman | 65% | Mapped proposal event contexts to selected-source coverage; counted candidate positive origins and episodes for the validation periods | Align labels to event definitions, finalise metric definitions, document small-event uncertainty |

**Assignment boundaries:**

- Parallel work is the default: an owner proceeds as soon as their input contract, scope and acceptance check are clear.
- Use dependency gates, not person-completion gates; record blocked subtasks and continue independent work.
- Bridge unavailable inputs explicitly with the documented schema or a labelled synthetic fixture, and rerun on real inputs before making claims.
- Review week-level acceptance evidence together before calling a milestone complete; integration, interpretation and final sign-off are team responsibilities.
- Do not use synthetic fixtures for historical event labels, reported coverage, model performance or final evaluation.
- Change this allocation when the project lead explicitly calls out a change.
