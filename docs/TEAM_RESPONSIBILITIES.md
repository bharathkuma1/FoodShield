# FoodShield team responsibilities across eight weeks

**Status snapshot:** 1 November 2026.

**Allocation status:** The current owner mapping for all eight weeks is the agreed working default. As directed by the project lead, update it when a change is explicitly called out. This agreement covers workstream ownership; the progress percentages below separately track documented deliverables.

**Use:** Working allocation based on the submitted proposal, TA feedback, documented source feasibility, and current implementation. Individual responsibilities can be adjusted as target, coverage, and implementation decisions become concrete.

## Responsibility and progress matrix

| Week | Bharath Kumar | Abhijith C R | Jyoti Prakash Das | Naman Singh Shekhawat | Shared outcome |
|---|---|---|---|---|---|
| **1 — source feasibility** | Price and M0 source profile | Production source profile and availability-lag proposal | Weather source profile, time/spatial method | Trade profile and event evidence | Compare source coverage, country feasibility, event contexts, and unresolved gates. **Agreed.** Details: `WEEK1_TASK_ASSIGNMENTS.md`. |
| **2 — Spark ETL** | Price Bronze/Silver and M0 Gold | Production Bronze/Silver and lag metadata | Weather Bronze/Silver and monthly screening aggregate | Trade Bronze/Silver and retained metadata | Reproducible local Spark layers, schema/grain checks, source limitations. **Agreed.** Details: `WEEK2_TASK_ASSIGNMENTS.md`, `WEEK2_ETL.md`. |
| **3 — alignment and features** | Price/M0 lags, rolling summaries, normalized/anomaly candidates | Annual production alignment, as-of dates, availability-aware features | Weather monthly aggregation, anomalies/lags, spatial-basis caveat | Trade alignment/features and cross-signal join completeness | Country–wheat–month feature panel and reviewed temporal/join evidence. **Agreed default.** Details: `WEEK3_TASK_ASSIGNMENTS.md`. |
| **4 — M0 and evaluation protocol** | Implement and document price-only M0 detector/scoring baseline | Profile and propose observable outcome/label data and publication timing; preserve target/source caveats | Propose chronological split/event-period eligibility from verified coverage; identify weather-eligible periods | Compile event evidence and draft evaluation protocol/metric definitions; keep event windows separate from labels | Team review and approval of outcome, horizon, threshold-selection method, and chronological train/validation/test rules before metrics are claimed. **Agreed default.** |
| **5 — M1–M4 and ablation** | Maintain M0 reference; implement price-only comparator and consistent score interface | Implement/check M2 production addition and its availability sensitivity | Implement/check M1 weather addition and document point versus representative-weather sensitivity | Integrate M3 trade and M4 temporal/cross-signal additions; produce staged ablation comparison | Compare feasible M0–M4 stages on consistent eligible records; report unavailable signals and coverage changes. **Agreed default.** |
| **6 — Kafka replay and Spark Structured Streaming** | Produce processed historical records using the agreed replay envelope | Implement/coordinate Spark Structured Streaming input, state/processing path, and bounded run | Implement alert output fields/reason traceability and inspect sample alerts | Configure local Kafka topic/replay orchestration and reconcile input/output accounting | One repeatable local end-to-end replay with documented configuration and no live-feed claim. **Agreed default.** |
| **7 — evaluation and scalability** | Summarize M0–M4 event metrics, lead time, and price-only comparison | Report feature quality, join completeness, temporal/as-of checks, and ablation support | Run and document bounded batch Spark volume/resource scaling measurements | Measure streaming throughput, latency, replay completeness, and bottlenecks | Results distinguish measured values from targets; record data volume, configuration, machine limits, and limitations. **Agreed default.** |
| **8 — dashboard, report, presentation, reproducibility** | Draft research question, method, model progression, and interpretation sections | Document data lineage, normalization, quality, temporal availability, and reproducibility | Build/assemble dashboard or result visuals and explain their coverage | Integrate demo/runbook, limitations, final presentation flow, and artifact checklist | Review the report/demo together; reproduce a clean run; state scope and limitations without unsupported claims. **Agreed default.** |

## Completion percentages by documented workstream

Percentages indicate the workstream evidence currently present in this repository. They are not measures of personal contribution, attendance, or model success. Future weeks remain at 0% until their acceptance evidence is implemented and recorded.

| Week | Bharath | Abhijith | Jyoti | Naman | Status basis |
|---|---:|---:|---:|---:|---|
| 1 | 100% | 100% | 100% | 100% | Feasibility handoffs documented. |
| 2 | 100% | 100% | 100% | 100% | Local ETL outputs and owner deliverables documented. |
| 3 | 100% | 100% | 100% | 100% | Spark feature build, real Week 2 monthly/as-of integration, coverage profile, and hand-reviewed synthetic lag/anomaly plus real annual-availability examples are documented. Local rights-bound data remains ignored; shared teammate review is pending. See `WEEK3_TASK_ASSIGNMENTS.md` and `WEEK3_FEATURES.md`. |
| 4 | 80% | 95% | 55% | 65% | In progress; selected-source Spark features and M0 training/validation calibration run successfully. The target and candidate split are working choices marked review needed; final-test labels remain untouched. Overall Week 4 progress: **74%** (mean, rounded). |
| 5 | 0% | 0% | 0% | 0% | Planned; depends on Week 3 features and Week 4 protocol. |
| 6 | 0% | 0% | 0% | 0% | Planned; depends on processed event contract. |
| 7 | 0% | 0% | 0% | 0% | Planned; depends on model and replay outputs. |
| 8 | 0% | 0% | 0% | 0% | Planned; depends on reviewed results and reproducible demo. |

### Week 4 percentage basis

| Owner | Progress | Evidence present | Remaining for this workstream |
|---|---:|---|---|
| Bharath | 80% | Implemented and ran the Spark selected-source feature panel and price-only M0 MLlib logistic-regression candidate, trained on training origins with validation-only alert-cutoff calibration. Final-test labels and predictions were not loaded or generated. | Review model settings, target, split and validation calibration with the team; revise if needed; later compare M1–M4 with the same estimator family and origins. |
| Abhijith | 95% | Selected the CC BY FAOSTAT farm-gate price source, documented the SSSU reuse basis, produced the CPI-adjusted candidate with source hashes, generated target/horizon sensitivity, and integrated selected-source input into the Week 4 Spark path. | Review the current 3-month/10% target choice; confirm annual availability assumptions and keep retrieval/provenance current. |
| Jyoti | 55% | Profiled exact shared-source M0–M4 feature-eligible dates, common horizon coverage, and a candidate chronological split. | Review and approve the split/purge against the selected horizon; improve representative weather construction and document weather/as-of constraints. |
| Naman | 65% | Mapped proposal event contexts to selected-source coverage and added candidate positive-origin/episode counts for common M0–M4 validation periods. The 2007–08 context is unavailable; 2010/11 lacks multi-year pre-event history; 2022 has nine M4 origins under the provisional three-month window. | Align approved target labels to evidence-backed event definitions, finalize metric definitions, document small-event uncertainty, and review the final protocol. |

The **74% Week 4 overall figure** is the arithmetic mean of these four repository-evidence estimates, rounded to the nearest whole percent. It indicates milestone artifact completion only; it is not an estimate of personal effort or a claim that the review-needed target, split, or protocol is final. Remaining work can proceed in parallel where inputs permit; the team must review the shared evaluation choices before final-test metrics are reported.

## Assignment boundaries

- **Parallel work is the default:** an owner may proceed as soon as their input contract, scope, and acceptance check are clear. Do not make all four people wait for one teammate's entire weekly task to reach 100%.
- **Use dependency gates, not person-completion gates:** move a dependent milestone forward when its required shared artifact/interface is reviewed and usable. Record blocked subtasks and continue independent work. A week can be in progress while another workstream is still open.
- **Bridge unavailable inputs explicitly:** use the documented schema or synthetic fixture to develop downstream plumbing when real data is delayed; keep the substitute clearly marked and rerun/review against eligible real inputs before making project claims.
- **Keep shared sign-off at the milestone boundary:** review the week-level acceptance evidence together before calling that milestone complete. One person's deliverable does not imply the full milestone is accepted.
- Keep the submitted wheat scope, country–commodity–month grain, M0–M4 progression, event-based historical evaluation, Spark scalability work, and TA-required local Kafka replay as the governing objectives.
- Week 4 target, horizon, threshold, eligible event windows, and split are not approved by this assignment table. Record them in `DECISIONS.md` after team review and source checks.
- Each owner provides a small, reviewable handoff; integration, shared acceptance, interpretation, and final sign-off remain team responsibilities.
- Do not use synthetic fixtures for historical event labels, reported coverage, model performance, or final evaluation.
- Change this allocation when the project lead explicitly calls out a change. Update the roadmap and progress basis at the same time.
