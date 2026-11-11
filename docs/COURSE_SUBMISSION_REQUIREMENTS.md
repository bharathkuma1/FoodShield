# DA231o final submission requirements

**Source:** professor's final-project instruction slide shared by the team.

**Purpose:** internal checklist for preparing one consistent team submission. This file tracks course logistics; it is not part of the professor-facing research manuscript.

## Team report

| Requirement | Expected content or format | Current status |
|---|---|---|
| One report per team | Avoid duplicate submissions from team members. | Pending final submission. |
| Length and format | 6–8 A4 pages, 10-point font, 2.5 cm margins, PDF. | The final report draft is a shared Claude doc built from `PROJECT_SUBMISSION.md` and the Week 7 results; final page layout and PDF export remain pending. |
| Heading | Project title, authors, and email address. | Title and author names are present; team email address(es) need to be supplied/confirmed. |
| Problem and goals | Problem definition, motivation, design goals, required features, scalability/performance goals. | Core problem and wheat-only M0–M4 scope are in the manuscript; batch scaling protocol and proposed local replay goals are now explicit. Confirm goals after baseline measurement and review against the proposal before PDF export. |
| Approach and methods | High-level design, architecture/data model, big-data platforms, and ML methods. | Architecture and Spark/Kafka direction are documented. Model methods are fixed (D1–D11) and written up in manuscript §5.4. |
| Evaluation | Experiment design, scalability/performance targets, evaluation metrics, plots, and analysis. | **Model evaluation done** (backtest and one-time test, with figures in `docs/figures/`). First batch reference timings are measured (D9). The Week 7 evaluation is done: lead times, threshold trade-off, probability quality, label sensitivity, data quality and as-of tests, batch scaling series and streaming series, with plots (`WEEK7_*.md`). The laptop streaming runs passed every check (`WEEK6_LAPTOP_RESULTS.md`). Only measured results are presented as achieved. |
| Summary | Assess progress toward the proposal's design/performance goals and describe future extensions. | The report draft's conclusion states the model result, the streaming and scaling assessment (laptop numbers included) and future extensions; confirm in the joint review. |

The manuscript keeps local source-extract counts out of its data description. The final evaluation should include only the benchmark workload sizes and measured performance details needed to make scalability results reproducible; those are experimental settings, not claims about the amount or completeness of the source data.

## Other team submissions and participation

| Requirement | Expected content or format | Current status |
|---|---|---|
| Presentation slides | One slide deck per team; avoid duplicate submissions. | Drafted as a shared Claude slide deck (12 slides, speaker notes, demo script; exports to PowerPoint or PDF). Laptop streaming numbers added; the joint review remains. |
| Public GitHub repository link | One link per team; verify public accessibility before submission. | `feature/workinprogress_v1` is on GitHub. `feature/workinprogress_v2` (Weeks 4–5 and D9) exists only in the local repository folder and the git bundle so far, and still needs to be pushed. Public accessibility has not been confirmed. |
| Contribution summary | Each team member submits the MS Form entry describing tasks performed and estimated effort percentage. | The repository records assigned workstream ownership and artifact status; this is not an individual-contribution declaration. Each member must provide their own form response. |
| Demo/presentation session | Use the session-selection MS Form when posted. All team members attend the full session and all presentations in that session. | Session details not yet available. |
| Peer evaluation | Each student completes the circulated evaluation form for presentations in the session. | Pending session and form. |

## Assessment dimensions

Use these dimensions for the final internal review: **novelty, technical effort, technical correctness, scalability, and impact**. The manuscript and demo should provide evidence for each dimension without claiming results that have not been measured.

### Current project fit against the professor's expectations

| Assessment dimension | Current fit | Evidence needed for a strong final submission |
|---|---|---|
| Novelty | **Moderate and applied.** The project combines leakage-aware price monitoring, heterogeneous wheat/weather/production/trade signals, historical event evaluation, and a local replay path. It is not proposing a new ML algorithm. | Demonstrate a clear M0–M4 ablation and explain what the integrated early-warning experiment reveals beyond portal charts or single-source summaries. |
| Technical effort | **Batch implementation complete through Week 5.** The Spark M0–M4 pipeline runs on the selected farm-gate source, with weighted weather, a frozen protocol and a one-time test. Full FAOSTAT archives and gridded weather are processed at real size (D9). The Kafka/Spark Structured Streaming replay is built (Week 6). It replays 1.86 million records end to end, passes every check, and resumes after a hard restart. | Deliver the end-to-end reproducible pipeline, M0–M4 implementation, local Kafka replay, and a concise runbook. |
| Technical correctness | **Strong.** Target, horizon, split and purge are confirmed (D1–D3). Two feature definitions were corrected before the test (D10). The test was pre-registered and opened once. Ukraine wheat from the full archives matches the model inputs exactly. Annual release timing is still a proxy, so a 3-year sensitivity run is reported. | Validate source joins and missingness; finalize target and purge rules without leakage; document the weather limitation; verify annual availability or report sensitivity; keep M0–M4 on identical rows. |
| Scalability | **First measurements in.** 22.7 million FAOSTAT rows (3.2 GB) in 2 min 40 s, with an 11–44× Parquet query speed-up, and 1.86 million daily weather rows in 11 s, D9 single runs on 2 cores. Week 7 repeated series: 186 million rows in 43 s (4.3 million rows/s), 79% two-core efficiency, shuffle partitions the main tuning lever. Streaming kept up to 4,000 events/s, with alert p95 at or below 2.7 s against the 10 s target in development runs; on the team laptop both targets were met with alert p95 of 3.1 s or less. | Measure batch input volume/runtime/resource scaling and streaming throughput, latency, and replay completeness under documented workloads/configurations. Report targets separately from measurements. |
| Impact | **Relevant problem; mixed evidence.** Multi-signal models were worse than price-only on 2014–2020 and better on the 2021–2022 test (M4 F1 0.62 vs 0.43). Too few events to settle it. The target is farm-gate pricing and does not measure consumer affordability or physical shortages. | Report warning lead time and false alarms honestly, state the market stage/geographic limits, and discuss what a validated warning could and could not support. |

**Overall assessment:** the project is well aligned with the professor's stated course goals and report structure. Its strongest grading opportunity is a technically correct, reproducible comparison plus measured Spark/Kafka scaling evidence. The main risk is claiming predictive value from too few independent events: 8 backtest episodes and 3 test episodes, which point in different directions. Report both, state the small-event limitation, and do not treat months as independent events. A multi-country panel (D9-D, 31 feasible countries) is the main way to add events, and is planned as extension work after the project (D13).
