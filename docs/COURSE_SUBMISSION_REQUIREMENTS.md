# DA231o final submission requirements

**Source:** professor's final-project instruction slide shared by the team.

**Purpose:** internal checklist for preparing one consistent team submission. This file tracks course logistics; it is not part of the professor-facing research manuscript.

## Team report

| Requirement | Expected content or format | Current status |
|---|---|---|
| One report per team | Avoid duplicate submissions from team members. | Pending final submission. |
| Length and format | 6–8 A4 pages, 10-point font, 2.5 cm margins, PDF. | Manuscript draft is maintained in `PROJECT_SUBMISSION.md`; final page layout and PDF export remain pending. |
| Heading | Project title, authors, and email address. | Title and author names are present; team email address(es) need to be supplied/confirmed. |
| Problem and goals | Problem definition, motivation, design goals, required features, scalability/performance goals. | Core problem and wheat-only M0–M4 scope are in the manuscript; batch scaling protocol and proposed local replay goals are now explicit. Confirm goals after baseline measurement and review against the proposal before PDF export. |
| Approach and methods | High-level design, architecture/data model, big-data platforms, and ML methods. | Architecture and Spark/Kafka direction are documented; model methods remain subject to Week 4 target/protocol decisions. |
| Evaluation | Experiment design, scalability/performance targets, evaluation metrics, plots, and analysis. | Metrics and proposed targets are documented; batch/streaming measurements and plots remain future evidence. Targets must not be presented as achieved. |
| Summary | Assess progress toward the proposal's design/performance goals and describe future extensions. | Manuscript conclusion exists; add a final evidence-based assessment and concise future-extension discussion after experiments. |

The manuscript keeps local source-extract counts out of its data description. The final evaluation should include only the benchmark workload sizes and measured performance details needed to make scalability results reproducible; those are experimental settings, not claims about the amount or completeness of the source data.

## Other team submissions and participation

| Requirement | Expected content or format | Current status |
|---|---|---|
| Presentation slides | One slide deck per team; avoid duplicate submissions. | Pending. |
| Public GitHub repository link | One link per team; verify public accessibility before submission. | The working branch is shared remotely; public accessibility has not been confirmed. |
| Contribution summary | Each team member submits the MS Form entry describing tasks performed and estimated effort percentage. | The repository records assigned workstream ownership and artifact status; this is not an individual-contribution declaration. Each member must provide their own form response. |
| Demo/presentation session | Use the session-selection MS Form when posted. All team members attend the full session and all presentations in that session. | Session details not yet available. |
| Peer evaluation | Each student completes the circulated evaluation form for presentations in the session. | Pending session and form. |

## Assessment dimensions

Use these dimensions for the final internal review: **novelty, technical effort, technical correctness, scalability, and impact**. The manuscript and demo should provide evidence for each dimension without claiming results that have not been measured.

### Current project fit against the professor's expectations

| Assessment dimension | Current fit | Evidence needed for a strong final submission |
|---|---|---|
| Novelty | **Moderate and applied.** The project combines leakage-aware price monitoring, heterogeneous wheat/weather/production/trade signals, historical event evaluation, and a local replay path. It is not proposing a new ML algorithm. | Demonstrate a clear M0–M4 ablation and explain what the integrated early-warning experiment reveals beyond portal charts or single-source summaries. |
| Technical effort | **Strong design scope; implementation is in progress.** Public-source snapshots, provenance, feature rules, Spark layers, and the Kafka/Spark Structured Streaming requirement are documented. The selected farm-gate source still needs wiring into the current Spark M0–M4 pipeline. | Deliver the end-to-end reproducible pipeline, M0–M4 implementation, local Kafka replay, and a concise runbook. |
| Technical correctness | **Good foundations; core validation remains open.** Source rights/attribution, units, country–wheat–month grain, past-only features, and chronology are explicit. The target formula/horizon are not approved; annual release timing is a proxy; weather is one point. | Validate source joins and missingness; finalize target and purge rules without leakage; document the weather limitation; verify annual availability or report sensitivity; keep M0–M4 on identical rows. |
| Scalability | **Objective fits the course; no performance result yet.** Spark batch processing and local Kafka/Spark Structured Streaming directly support the data-engineering-at-scale theme. | Measure batch input volume/runtime/resource scaling and streaming throughput, latency, and replay completeness under documented workloads/configurations. Report targets separately from measurements. |
| Impact | **Relevant problem, impact not yet demonstrated.** Timely wheat producer-price stress warnings could help interpret market pressure; the current target is farm-gate pricing and does not measure consumer affordability or physical shortages. | Report warning lead time and false alarms honestly, state the market stage/geographic limits, and discuss what a validated warning could and could not support. |

**Overall assessment:** the project is well aligned with the professor's stated course goals and report structure. Its strongest grading opportunity is a technically correct, reproducible comparison plus measured Spark/Kafka scaling evidence. The main risk is claiming predictive value from too few independent test events: only the 2022 proposal context currently fits the selected multi-signal price window, with nine M4 origins under the provisional three-month horizon. Use earlier rolling-origin backtests and state the small event-test limitation; do not treat the nine origins as nine independent events.
