# FoodShield decisions, assumptions, and open questions

This log separates requirements established by the submitted proposal and TA feedback from proposed implementation choices. Update it when the team makes a decision; retain the rationale and date.

## Confirmed requirements

| Decision | Basis |
|---|---|
| Research wheat supply stress and whether multi-source temporal signals give earlier warning than price-only monitoring. | Submitted proposal PDF and updated DOCX. |
| Do not claim prediction of geopolitical events or their causes. | Submitted proposal problem definition. |
| Use a country–commodity–month analytical grain as the working alignment. | Submitted proposal's example grain and data model. |
| Compare M0 price-only, M1 + weather, M2 + production, M3 + trade, M4 all signals plus temporal/lag/cross-signal features. | Submitted proposal and supplied project context. |
| Use Spark DataFrames/SQL as the primary distributed processing approach; use historical validation, ablation, data-quality and scalability evaluation. | Submitted proposal. |
| Add local Kafka replay of historical/processed records through Spark Structured Streaming and produce timely stress alerts. | TA feedback. No live feed required. |
| Establish numeric throughput and latency targets and report actual outcomes only after measurement. | TA feedback. The TA did not prescribe exact values. |
| Use future domestic wheat-price stress as the primary evaluation target type for the Ukraine wheat pilot, not physical-shortage prediction. At prediction month `t`, M0–M4 features must use information available no later than `t`; the target is measured strictly after `t`. | Project lead confirmation, aligned with the proposal's progressive warning experiment. This is a temporal separation, not a claim of a source-independent label: FPMA supplies both historical price features and future price outcomes. Exact stress rule, forecast horizon, threshold method, eligible test events, and chronological split remain open; FPMA reuse rights and metadata remain gates. |
| Keep workflows runnable locally and avoid cloud resources or paid billing without explicit approval. | Project direction. The current maintainer's Windows 11, 8 GB RAM, and D: drive are one development host's constraints, not team-wide system requirements; benchmark each host and record its environment. |
| Share compact, reviewed source snapshots so collaborators can start from the same inputs; automate retrieval for inputs that remain local. | User direction. The current Ukraine wheat FAOSTAT QCL/TCL extracts and NASA POWER point snapshot are versioned with attribution and provenance. FPMA/APK-Inform data and derived tables containing its values stay local until reuse rights are confirmed. Full FAOSTAT bulk archives and unrelated candidate downloads remain uncommitted. |
| Preserve proposal/presentation files and archive; no relocation or overwrite. | Team working constraints. |
| Assign Week 1 source-profiling ownership as Bharath—price/baseline; Abhijith—production; Jyoti—weather; Naman—trade/event evidence. | Team agreement recorded in `docs/WEEK1_TASK_ASSIGNMENTS.md`; this assigns workstreams and does not select the final country, source, or event labels. |
| Carry the same four signal owners into Week 2 Spark ETL: Bharath—price/M0; Abhijith—production; Jyoti—weather; Naman—trade. | Team agreement recorded in `docs/WEEK2_TASK_ASSIGNMENTS.md`; ownership agreement does not itself establish individual effort or implementation completion. |

## Proposed engineering choices (not yet team-approved)

| Proposal | Rationale / boundary |
|---|---|
| Use bronze/silver/gold Parquet layers for local data organization. | Fits the requested ETL stages and technology direction; define schemas and retention after profiling. |
| Start local streaming performance targets at ≥100 events/s sustained for 10 minutes, ≥250 events/s for 60 seconds, and p95 end-to-end alert latency ≤10 seconds. | Laptop-conscious targets proposed in `docs/PLAN.md`; measure in a fixed workload before treating as commitment. These are not TA-prescribed and are not achieved results. |
| Measure replay completeness by reconciling valid input records with processed accounting; do not promise exactly-once behavior. | Avoids expanding the task into delivery semantics beyond the TA request. |
| Use local Spark mode and a single-node Kafka setup for the initial replay demonstration. | A bounded local starting point; tune per host. Exact Kafka runtime/deployment method remains open. |
| Keep source code in a `src/foodshield/` package with configs, scripts, tests, local data, and generated artifacts separated. | Recommended project organization; it keeps implementation, local datasets, and outputs distinct. |
| Use chronological train/validation/test evaluation: start with 3–5 pre-event years for simple calibration, reserve later event periods as untouched tests, and use earlier rolling-origin folds when coverage permits. | Matches early-warning deployment and prevents looking ahead. At monthly grain 3–5 years provide only 36–60 aligned rows, so treat the span as an initial minimum, not a mandatory cap; keep the detector simple and document the actual split. |

## Week 1 working decisions (1 November 2026)

These are the feasibility decisions recorded at the user's request. They are working project choices for the next stage, not a claim that all teammates separately voted or that third-party rights have been cleared. Revisit a choice if its stated evidence or constraint changes.

| Decision | Working choice | Boundary / reason |
|---|---|---|
| Pilot geography | Ukraine remains the strongest current single-country starting candidate based on the downloaded input-source overlap; India remains an alternative/comparator. Reassess geography after representative weather and outcome coverage are checked. | Current monthly price and point-weather files overlap in 2001–2022 for Ukraine; India's current price/weather overlap is 2022 only. A point-weather series is not a country-representative exposure. |
| M0 price | Use FPMA Ukraine National Average, wheat 3rd class bid/EXW/processing, wholesale, UAH/tonne, calculated monthly series (UUID `a9d7b6ee-6a9d-43a7-bca4-aaa419311498`). | Preserve original weekly observations and monthly calculated series separately. Metadata says `market_type=Retail` while `price_type=WHOLESALE`; document the mismatch and verify before treating it as resolved. |
| Price reuse | Retain the downloaded source for feasibility and controlled project analysis with FAO/source attribution. Do not redistribute raw source rows or publish charts/tables that expose underlying APK-Inform values until reuse rights are confirmed. | FAO's terms warn that third-party components may require separate permission ([FAO terms](https://www.fao.org/contact-us/terms/en/)); the FPMA partner page identifies APK-Inform ([partner profile](https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/)). This is a conservative rights disposition, not a legal finding that the existing copy is licensed. |
| M1 weather plan | NASA POWER daily parameters `PRECTOTCORR`, `T2M_MAX`, `T2M_MIN`; next stage should request grids over wheat-producing administrative regions and weight by verified regional wheat harvested area. | The current 49°N, 32°E point is API/coverage screening only. The downloaded Ukraine regional crop-statistics archive lists 2011–2021; it does not substantiate area weights for the 2007–08 or 2010 cases. Until historical weights are found, compare M1–M4 on their common verified period and report earlier cases as M0-only context. |
| M2/M3 annual availability | For monthly forecast year `Y`, provisionally use annual data only when `data_year <= Y - 2`; preserve FAOSTAT flags and notes. | A two-year lag is a cautious proxy, not an observed release date or historical data vintage. Run sensitivity or exclude annual features from strict as-of claims if vintage evidence is unavailable. |
| M3 trade grain | Use country-reporter aggregate wheat import/export quantity and value from TCL. Do not fetch bilateral TM for Week 1. | Current TCL extracts do not include partners. The 52M-row detailed matrix is unnecessary unless a partner-specific hypothesis is approved. |
| Event/case selection | Treat 2007–08, 2010–11, and 2022 as proposal examples, not required cases. Select final evaluation instances after the target/outcome definition and common, quality-checked source coverage are established; other documented wheat-stress episodes may be substituted. | Current Ukraine feature files overlap from 2001–2022 at a single weather point; India overlaps in 2022 at a single Haryana point. Neither overlap establishes representative weather or a valid independent label. See `WEEK1_EVENT_CASES.md` for coverage. |
| Week 1 completion | Mark all four owner workstreams 100% for the Week 1 feasibility handoff. | Means source profiling, recommendation, caveats, and next-stage boundary are documented in `WEEK1_TASK_ASSIGNMENTS.md`; it does not mean teammates reviewed the decisions or implementation/model work is complete. |
| Weeks 1–8 workstream ownership | Treat the current per-week owner mapping in `TEAM_RESPONSIBILITIES.md` as the agreed working default; update it when a change is explicitly called out. | User-directed working agreement. It assigns responsibilities and does not change the proposal's wheat-only, country–commodity–time, M0–M4, event-evaluation, or Spark scalability objectives. Completion percentages remain separate from ownership agreement. |

## Assumptions to validate

- FAOSTAT QCL/TCL, NASA POWER, and selected FPMA data can support a useful common wheat/country/month analysis after temporal and geographic alignment. Feasibility is not proof of model adequacy.
- The provisional two-year lag is conservative enough for the intended feature cadence; actual public-release dates may require longer lags.
- Regional crop-area weights can be extracted consistently for some years; no representative weather signal is yet produced.
- The broad event-study windows are suitable validation contexts, but are not operational positive/negative labels.
- Kafka and Spark Structured Streaming can run together within laptop memory limits for a bounded replay workload.
- Proposed numeric streaming targets are appropriate after a baseline measurement on the actual laptop.

## Later project decisions (outside the Week 1 feasibility handoff)

1. Can the Ukraine FPMA underlying source terms and metadata be confirmed before public release? If not, which verified-permission price series can replace it while preserving the research scope?
2. What exact future price-stress rule, forecast horizon, and threshold-selection method should be used? **Target type selected:** future domestic wheat-market price stress, not physical shortage. At prediction month `t`, all M0–M4 features stop at `t`; the outcome is measured only in a later window. This is future-outcome evaluation from the same FPMA price source, not an independent-source label. Price-source rights and metadata remain gates. Proposal event periods are contexts, not automatic labels or fixed test cases; choose eligible cases from the validated common period without tuning on the final holdout. Exact outcome formula, horizon, threshold method, and chronological split remain open. See `WEEK1_EVENT_CASES.md` and `WEEK4_EVALUATION_PROTOCOL.md`.
3. Can historical regional wheat-area weights and FAOSTAT publication vintages be obtained for strict as-of validation?
4. Which detector/thresholding method will be used for M0–M4, and how will thresholds be selected without holdout leakage?
5. What replay event schema, ordering, pacing, checkpoint/output behavior, and duplicate/restart semantics will the streaming demo support?
6. What Kafka local runtime is practical on the Windows laptop (for example, native process or container), given memory and setup constraints?
7. After a small benchmark run, should the initial 100/250 events/s targets be retained or adjusted with a documented rationale?
8. Which dashboard technology and final output format best fit the remaining time and laptop constraints?

## Source materials reviewed

- `Dataflux - FoodShield_Project_Proposal.pdf` (submitted proposal)
- `FoodShield_Project_Proposal_Updated_2.docx` (updated proposal; same central scope)
- `Revised with TA comments.docx` (streaming replay and target feedback)
- `FoodShield_Proposal_Discussion_Deck_v5.pptx` (scope context, candidate events/sources, and architecture)
- Earlier proposal and deck versions under `archive/` (historical context; preserved)

The current discussion deck describes streaming as an extension/stretch item, while the later TA feedback makes local replay a required project improvement. The working plan incorporates replay while retaining the proposal's core batch research and M0–M4 comparisons.
