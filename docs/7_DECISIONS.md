# 7. Decisions, assumptions and open questions

This log separates requirements established by the submitted proposal and TA feedback from implementation choices, and records each team decision (D1–D15) with its rationale and the week it was made. Evidence for each decision is in the numbered documents: [`1_PROJECT.md`](1_PROJECT.md), [`2_DATA.md`](2_DATA.md), [`3_ARCHITECTURE.md`](3_ARCHITECTURE.md), [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md), [`5_STREAMING.md`](5_STREAMING.md), [`6_SCALABILITY.md`](6_SCALABILITY.md).

## 7.1 Confirmed requirements

| Decision | Basis |
|---|---|
| Research wheat supply stress and whether multi-source temporal signals give earlier warning than price-only monitoring. | Submitted proposal PDF and updated DOCX. |
| Do not claim prediction of geopolitical events or their causes. | Submitted proposal problem definition. |
| Use a country–commodity–month analytical grain as the working alignment. | Submitted proposal's example grain and data model. |
| Compare M0 price-only, M1 + weather, M2 + production, M3 + trade, M4 all signals plus temporal/lag/cross-signal features. | Submitted proposal and supplied project context. |
| Use Spark DataFrames/SQL as the primary distributed processing approach; use historical validation, ablation, data-quality and scalability evaluation. | Submitted proposal. |
| Add local Kafka replay of historical/processed records through Spark Structured Streaming and produce timely stress alerts. | TA feedback. No live feed required. |
| Establish numeric throughput and latency targets and report actual outcomes only after measurement. | TA feedback. The TA did not prescribe exact values. |
| For the Ukraine pilot, use the public FAOSTAT monthly wheat producer-price series as the price input and future outcome; describe the measured target specifically as farm-gate producer-price stress. Deflate with the shareable SSSU all-items CPI chain for real-price analysis. | Project lead decision, Week 4. The FAOSTAT series is catalogued CC BY 4.0 and consistently available over the pilot window; the SSSU series has an official-statistics reuse basis with attribution. This is a precise market-stage definition of the proposal's future wheat-price-stress goal. |
| Use future Ukraine wheat farm-gate producer-price stress as the primary evaluation target type, not physical-shortage prediction. At prediction month `t`, M0–M4 features must use information available no later than `t`; the outcome is measured strictly after `t` using the same FAOSTAT producer-price series. | Project lead decision, Week 4, aligned with the proposal's progressive warning experiment. This is temporal separation, not a claim of source-independent labelling. The stress rule, horizon, threshold method and split were then fixed as D1–D4. Cite FAOSTAT and SSSU; preserve their source definitions and caveats. |
| Keep workflows runnable locally and avoid cloud resources or paid billing without explicit approval. | Project direction. The maintainer's Windows 11, 8 GB RAM machine is one development host, not a team-wide system requirement; benchmark each host and record its environment. |
| Share compact, reviewed source snapshots so collaborators can start from the same inputs; automate retrieval for inputs that remain local. | User direction. The Ukraine wheat FAOSTAT QCL/TCL/PP extracts, SSSU CPI and NASA POWER snapshot are versioned with attribution and provenance. FPMA/APK-Inform data and derived tables containing its values stay local until reuse rights are confirmed. Full FAOSTAT bulk archives and unrelated candidate downloads remain uncommitted. |
| Preserve proposal/presentation files and archive; no relocation or overwrite. | Team working constraints. |
| Assign Week 1 source-profiling ownership as Bharath—price/baseline; Abhijith—production; Jyoti—weather; Naman—trade/event evidence. | Team agreement recorded in [`1_PROJECT.md`](1_PROJECT.md) §1.5; this assigns workstreams and does not select the final country, source or event labels. |
| Carry the same four signal owners into Week 2 Spark ETL: Bharath—price/M0; Abhijith—production; Jyoti—weather; Naman—trade. | Team agreement recorded in [`1_PROJECT.md`](1_PROJECT.md) §1.5; ownership agreement does not itself establish individual effort or implementation completion. |

## 7.2 Engineering choices proposed in Week 1, and how they were settled

| Proposal | Rationale / boundary | Outcome |
|---|---|---|
| Use bronze/silver/gold Parquet layers for local data organisation. | Fits the requested ETL stages and technology direction; define schemas and retention after profiling. | Implemented from Week 2 ([`3_ARCHITECTURE.md`](3_ARCHITECTURE.md) §3.3). |
| Start local streaming performance targets at ≥100 events/s sustained for 10 minutes, ≥250 events/s for 60 seconds, and p95 end-to-end alert latency ≤10 seconds. | Laptop-conscious targets; measure in a fixed workload before treating as commitment. Not TA-prescribed. | Kept as D12c; met on the laptop and in the workspace ([`5_STREAMING.md`](5_STREAMING.md)). |
| Measure replay completeness by reconciling valid input records with processed accounting; do not promise exactly-once behaviour. | Avoids expanding the task into delivery semantics beyond the TA request. | Adopted as D12d (at-least-once with de-duplication); checked on every run. |
| Use local Spark mode and a single-node Kafka setup for the initial replay demonstration. | A bounded local starting point; tune per host. | Adopted as D12a (native Kafka 4.4.0, KRaft, one broker). |
| Keep source code in a `src/foodshield/` package with configs, scripts, tests, local data and generated artifacts separated. | Keeps implementation, local datasets and outputs distinct. | Implemented ([`3_ARCHITECTURE.md`](3_ARCHITECTURE.md) §3.7). |
| Use chronological train/validation/test evaluation: start with 3–5 pre-event years for simple calibration, reserve later event periods as untouched tests, and use earlier rolling-origin folds when coverage permits. | Matches early-warning deployment and prevents looking ahead. At monthly grain 3–5 years give only 36–60 aligned rows, so treat the span as a minimum and keep the detector simple. | Confirmed as D3, with the 2014–2020 rolling-origin backtest. |

## 7.3 Week 1 working decisions

These are the feasibility decisions recorded at the user's request. They are working project choices for the next stage, not a claim that all teammates separately voted or that third-party rights have been cleared.

| Decision | Working choice | Boundary / reason |
|---|---|---|
| Pilot geography | Ukraine is the strongest single-country candidate based on the downloaded input-source overlap; India remains an alternative/comparator. | Monthly price and point-weather files overlap in 2001–2022 for Ukraine; India's price/weather overlap is 2022 only. A point-weather series is not a country-representative exposure. |
| M0 price and future outcome | Use FAOSTAT Ukraine monthly Wheat Producer Price (LCU/tonne), element 5530, as the farm-gate price series for both price-only monitoring and the strictly future outcome. Use the SSSU all-items CPI chain where a real-price measure is needed. | Sources and retrieval snapshots are shared with provenance ([`2_DATA.md`](2_DATA.md) §2.5). FAOSTAT observations are 2010-01–2023-01; SSSU CPI is chained with December 2000 = 100. The CPI deflator is broad consumer inflation, not wheat-specific. The outcome is price stress at the producer/farm-gate stage. *(Recorded here in Week 4, when this choice replaced the Week 1 FPMA candidate.)* |
| FPMA secondary data | Keep Ukraine FPMA/APK-Inform observations local and outside shared model inputs and labels unless reuse permission is obtained. | FAO identifies APK-Inform as a source partner and warns that third-party material can have separate terms. FPMA metadata also contains a Retail/Wholesale discrepancy. The shared target does not depend on FPMA. |
| M1 weather plan | NASA POWER daily `PRECTOTCORR`, `T2M_MAX`, `T2M_MIN`; request grids over wheat-producing regions and weight by verified regional wheat harvested area. **Done:** 24-oblast wheat-area-weighted series (D5); full 232-cell grid check (D9-B). | The 49°N, 32°E point is API/coverage screening only. The regional crop-statistics archive lists 2011–2021 and does not give area weights for 2007–08 or 2010. |
| M2/M3 annual availability | For monthly forecast year `Y`, use annual data only when `data_year <= Y - 2`; preserve FAOSTAT flags and notes. | A two-year lag is a cautious proxy, not an observed release date or historical vintage. Confirmed as D6 with a 3-year sensitivity run. |
| M3 trade grain | Use country-reporter aggregate wheat import/export quantity and value from TCL. Do not fetch the bilateral trade matrix. | The TCL extracts have no partners. The 52M-row detailed matrix is unnecessary unless a partner-specific hypothesis is approved. |
| Event/case selection | Treat 2007–08, 2010–11 and 2022 as proposal examples, not required cases. Select evaluation instances after the target definition and common, quality-checked source coverage are established. | See [`2_DATA.md`](2_DATA.md) §2.3. |
| Week 1 completion | Mark all four owner workstreams 100% for the Week 1 feasibility handoff. | Means source profiling, recommendation, caveats and next-stage boundary are documented; it does not mean teammates reviewed the decisions or that implementation or model work was complete. |
| Weeks 1–8 workstream ownership | Treat the per-week owner mapping in [`1_PROJECT.md`](1_PROJECT.md) §1.5 as the agreed working default; update it when a change is explicitly called out. | User-directed working agreement. It assigns responsibilities and does not change the proposal's objectives. Completion percentages remain separate from ownership agreement. |

## 7.4 Week 4 decisions (D1–D9, confirmed at the Week 4 review)

Abhijith C R confirmed the recommended option for each item at the Week 4 review and asked to proceed to Week 5. This records that direction; it is not a claim that every teammate voted. The Week 4 working choices for the label, horizon, split and M0 were confirmed unchanged as D1–D4. Evidence: [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.1–4.3.

| # | Decision | Confirmed choice |
|---|---|---|
| D1 | Stress label | Positive if the maximum CPI-adjusted FAOSTAT farm-gate price in t+1..t+3 is at least 10% above the price at t. 5% and 15% remain sensitivity checks. |
| D2 | Forecast horizon | 3 months; 1 and 6 months remain sensitivity checks. |
| D3 | Chronological split | Train 2011–2017, validate 2018–2020, test 2021–2022, horizon-length purge at each boundary. The 2014–2020 rolling-origin backtest is the standard second evaluation for every stage. |
| D4 | M0 baseline | Unchanged Spark MLlib logistic regression on two price features (L2 0.1, standardisation); alert cutoff by best F1 on validation. Always report threshold-free AUC and the always-alert and momentum baselines next to F1. |
| D5 | Weather for M1–M4 | Wheat-area-weighted 24-oblast NASA POWER series (`configs/week4_v2.local.json`); the single point is kept only as a sensitivity check. |
| D6 | Annual availability lag | Keep `data_year + 2` (1 January) and report a 3-year-lag sensitivity run for M2–M4. |
| D7 | Synthetic data | Demo and pipeline testing only; never in results, labels or coverage claims. |
| D8 | Real 2021–2022 test | Open once, for M0–M4 together, after the M1–M4 configuration is frozen. |
| D9 | Data-engineering scale | (A) process full FAOSTAT bulk archives in Spark; (B) gridded NASA POWER weather over the wheat area; (C) daily-granularity Kafka replay; (E) labelled synthetic scaling runs. (D) a multi-country wheat panel is raised with the team and professor because it changes scope. |

## 7.5 Week 5 decisions (D10, D11 and the D8 go-ahead)

Abhijith C R confirmed D10, D11 and the D8 go-ahead in Week 5, after the evidence in [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.4. This record was made before the real test was opened.

| # | Decision | Confirmed choice |
|---|---|---|
| D10 | M1–M3 feature definitions | Corrected features are primary: weather vs the same calendar month in the prior 10 years; annual signals as bounded year-on-year change. The Week 3 definitions are reported as a robustness variant. |
| D11 | Calendar season | Price-only M0 stays the primary baseline; "season in every stage" is pre-registered as a robustness analysis and reported alongside. |
| D8 | Real 2021–2022 test | Go: run `scripts/run_final_test_once.py --confirm-open-test` once against `configs/week5_frozen_protocol.json`. |

The real test was opened once, in Week 5; results are in [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.6.

### D9 progress

| Item | Status |
|---|---|
| A. Full FAOSTAT archives in Spark | **Done.** QCL, TCL and PP: 22.7 million rows, 3.2 GB of CSV → 248 MB of Parquet in 2 min 40 s. Ukraine wheat matches the model extracts exactly. The optional detailed trade matrix (TM, 411 MB) was not downloaded, consistent with the Week 1 M3 trade-grain decision. |
| B. Gridded NASA POWER weather | **Done.** 232 cells, 1,864,120 daily rows. The national series correlates with the 24-point series at ≥ 0.9986 after removing the season, so D5's 24-point weather is kept. |
| C. Daily Kafka replay | **Built and checked (Week 6, D12).** 1,864,486 records through Kafka 4.4.0 and Spark Structured Streaming; every check passed; about 5,000 events/s at full speed on 2 cores; alert p95 under 3 s. Laptop runs (Windows 11, 16 cores): every check passed; 6,610 events/s at full speed; both targets met with alert p95 of 3.1 s or less ([`5_STREAMING.md`](5_STREAMING.md) §5.4). |
| E. Synthetic scaling runs | **Done (Week 7).** 1×–100× labelled expansions of the grid weather, up to 186 million rows ([`6_SCALABILITY.md`](6_SCALABILITY.md) §6.2). |
| D. Multi-country panel | **Extension work after the core project (D13, revised).** Feasibility: 31 countries have at least 120 of 156 monthly wheat producer prices for 2010–2022. |

Evidence: [`6_SCALABILITY.md`](6_SCALABILITY.md) §6.4.

## 7.6 Week 6 and scope decisions (D12–D14, confirmed at the start of Week 6)

Abhijith C R confirmed these at the start of Week 6. The team is doing a joint review of the whole project, and his confirmation is used to move forward in the meantime.

| # | Decision | Confirmed choice |
|---|---|---|
| D12 | Week 6 streaming design | **(a) Runtime:** native Apache Kafka 4.4.0 in KRaft mode, one broker on the project laptop, no Docker. The demo and the reported streaming measurements run on that laptop; development tests may also run in the Claude workspace. **(b) Replay content:** the 1,864,120 daily grid-weather rows plus the monthly price and annual production/trade records, sent in the order they would have become available. Spark Structured Streaming builds the monthly features and scores the already-fitted M0 and M4 models. The pass test is that streamed alerts equal batch alerts month for month. The replay is an engineering demonstration and adds no test evidence. **(c) Targets:** keep the proposed ones (at least 100 events/s sustained and 250 events/s burst, alert p95 ≤ 10 s); measure first, then revisit. **(d) Delivery:** at-least-once delivery, with checkpoints and de-duplication by `record_id`; no exactly-once claim. |
| D13 | Multi-country wheat panel (D9-D) | **Revised at the start of Week 6: extension work, not project scope.** Following the team's approval, the project stays single-country (Ukraine). The panel is planned as extended work after the core project is complete. Feasibility evidence is kept: 31 countries have enough monthly wheat producer prices. *(Earlier the same day, D13 read "Go"; it was revised before any panel work started.)* |
| D14 | Working mode | Claude does the work of all four workstreams in one shared place; the whole team reviews it together. The repository stays in the local folder (`feature/workinprogress_v2`) for now; pushing to GitHub waits for the joint review. Workstream ownership in [`1_PROJECT.md`](1_PROJECT.md) §1.5 is kept for review and accountability. |

## 7.7 Week 8 decision (D15)

Abhijith C R confirmed this in Week 8, on the same basis as D12–D14.

| # | Decision | Confirmed choice |
|---|---|---|
| D15 | Dashboard technology and format (closes question 8) | **One static, offline HTML page** (`docs/dashboard/index.html`, built by `scripts/build_week8_dashboard.py`; no framework, nothing to install). Five tabs: Overview; **Explorer** (compare any two stages, period, stress definition, annual delay and alert cutoff, recomputed in the browser from the saved month-by-month scores, nothing retrained); **What if?** (nudge one month's inputs for the frozen test models); **Streaming** (replay a saved laptop run, plus a live view); Scale. Defaults reproduce the reported numbers exactly; any other setting is labelled "exploring", and a cutoff moved on the test period carries a warning that it is not a fair test. **Live mode:** `scripts/serve_dashboard.py` (Python standard library, 127.0.0.1 only, read-only) serves the page and the running replay's alerts; the page pops up each new stress alert (from January 2022 by default, the paced demo window) with optional sound, and both can be switched off. `RUN_DEMO_LIVE.bat` starts the server and the demo together. Streamlit was not chosen: it needs an install and a running process and adds demo risk. |

## 7.8 Assumptions to validate

- FAOSTAT QCL/TCL, NASA POWER and the selected price data can support a useful common wheat/country/month analysis after temporal and geographic alignment. Feasibility is not proof of model adequacy.
- The two-year lag is conservative enough for the intended feature cadence; actual public-release dates may require longer lags. *Update, Week 4:* it fits the 2022–2023 releases; the 2020 production release may have been about 3 months later; earlier years are unknown. Covered by the D6 3-year sensitivity run.
- Regional crop-area weights can be extracted consistently for some years. *Update, Week 4:* SSSU oblast wheat areas are available. The weighted weather series uses fixed 2015–2020 shares (D5), and the 232-cell grid confirms it (D9-B). Weights for years before 2011 are still unavailable.
- The broad event-study windows are suitable validation contexts, but are not operational positive/negative labels.
- Kafka and Spark Structured Streaming can run together within laptop memory limits for a bounded replay workload. *Update, Week 6:* the laptop runs passed every check.
- Proposed numeric streaming targets are appropriate after a baseline measurement on the actual laptop. *Update, Weeks 6–7:* met with a wide margin; kept unchanged (D12c).

## 7.9 Open questions

**Status (Week 8):**

- **Resolved:** questions 2 and 4, by D1–D4.
- **Partly resolved:** question 3. Area weights are fixed at 2015–2020 shares, and the FAO publication vintage is approximated by the D6 lag with a 3-year sensitivity run.
- **Resolved for Week 6 (D12):** questions 5, 6 and 7.
- **Resolved for Week 8 (D15):** question 8.
- **Not needed:** question 1, which is not a blocker.

1. Can Ukraine FPMA source terms and the Retail/Wholesale metadata discrepancy be resolved for any separate local comparison? This is not a blocker for the shared target.
2. *(Resolved by D1–D4.)* Review the operational stress rule, forecast horizon and alert-threshold method. **Working choice at the time:** a ≥10% increase in the maximum CPI-adjusted farm-gate price within three months, with a 3-month horizon, using FAOSTAT PP and the SSSU CPI chain; candidate split training 2011–2017, validation 2018–2020, final test 2021–2022, with horizon purges. Proposal event periods are contexts, not automatic labels; 2007–08 is outside the selected series, and 2010/11 has insufficient pre-event history ([`2_DATA.md`](2_DATA.md) §2.3, [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.1).
3. Can historical regional wheat-area weights and FAOSTAT publication vintages be obtained for strict as-of validation?
4. *(Resolved by D4.)* Which detector/thresholding method will be used for M0–M4, and how will thresholds be selected without holdout leakage?
5. *(Resolved by D12.)* What replay event schema, ordering, pacing, checkpoint/output behaviour, and duplicate/restart semantics will the streaming demo support?
6. *(Resolved by D12.)* What Kafka local runtime is practical on the Windows laptop, given memory and setup constraints?
7. *(Resolved by D12.)* After a small benchmark run, should the initial 100/250 events/s targets be retained or adjusted?
8. *(Resolved by D15.)* Which dashboard technology and final output format best fit the remaining time and laptop constraints?

## 7.10 Source materials reviewed

- `Dataflux - FoodShield_Project_Proposal.pdf` (submitted proposal)
- `FoodShield_Project_Proposal_Updated_2.docx` (updated proposal; same central scope)
- `Revised with TA comments.docx` (streaming replay and target feedback)
- `FoodShield_Proposal_Discussion_Deck_v5.pptx` (scope context, candidate events/sources, and architecture)
- Earlier proposal and deck versions under `archive/` (historical context; preserved)

These files are kept locally outside Git. The discussion deck describes streaming as an extension/stretch item, while the later TA feedback makes local replay a required project improvement. The working plan incorporates replay while retaining the proposal's core batch research and M0–M4 comparisons.
