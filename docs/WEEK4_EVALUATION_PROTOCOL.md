# Week 4 evaluation protocol: open draft

**Status:** Project-level scope and evaluation principles are in [`ARCHITECTURE.md`](ARCHITECTURE.md). This Week 4 document tracks the remaining target, horizon, threshold, event, and calendar decisions needed to instantiate that design. No target formula, horizon, event holdout, or train/validation/test date is locked. Do not report model metrics until these are defined and implemented.

## What timeframe is currently available?

| Input family | Current Ukraine coverage | Consequence |
|---|---|---|
| FAOSTAT monthly wheat producer price | January 2010–January 2023 | **Selected price input and future outcome** for the Ukraine pilot. There are continuous monthly records through December 2022; the series measures farm-gate producer prices. |
| SSSU all-items CPI | August 1991–September 2026 | Shared official monthly CPI-change series; chained to a real-price index with December 2000=100. Broad inflation deflator, not wheat-specific. |
| FPMA monthly wheat price | January 2001–October 2026 | Local exploratory comparator only; source-partner reuse terms and a Retail/Wholesale metadata mismatch remain unresolved. Not used for the selected target. |
| NASA POWER weather | January 2001–December 2022 | Caps the current price/weather overlap at December 2022. It is a single-point screening series, not representative wheat-area exposure. |
| FAOSTAT wheat production and trade | Annual reference years 1992–2024 | These are features, not monthly outcomes; the current availability lag is provisional, so the input years must be shifted to when they could have been available. |

The selected-price all-signal input overlap is January 2010–December 2022 before lookback and missingness rules; January 2023 supplies a further outcome observation. A source-only profile applying the current Week 3 feature-completeness rules reports M0 145 origins (2011-01–2023-01), M1 144 (2011-01–2022-12), M2 132 (2011-02–2022-12), and M3/M4 121 (2011-02–2022-12). For a provisional 3-month future window, 119 M4 origins have all future price observations through 2022-10. The detailed monthly eligibility file and rule are in `data/processed/week4_ukraine_m0_m4_eligibility.csv` and `scripts/profile_week4_m0_m4_eligibility.py`. These are candidate feature/outcome availability results, not positive labels or model results. See [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md).

## Outcome options already profiled

| Candidate | What it would measure | Current boundary |
|---|---|---|
| Future Ukraine farm-gate wheat producer-price stress | Whether information available through month `t` warns of an explicitly defined producer-price stress condition in a later period. | **Selected target type and price source.** Use FAOSTAT producer price as M0 history and future outcome, and cite the source. This is time-separated but not independent by source. No horizon, stress rule, or threshold is selected. |
| Monthly wheat export-flow disruption | Changes in reported wheat exports, useful as a separate supply/trade outcome. | Full monthly coverage has not been established; sparse query samples cannot define missing months or event labels. Do not use it until the complete candidate window is profiled. |
| Annual production or trade | Slow-moving supply context. | Annual cadence makes these unsuitable as monthly outcomes in the current design; retain them as lagged features. |
| Proposal event contexts | Historical context for wheat-market disruptions. | Global event descriptions do not automatically establish a country-month outcome. Define geography, onset, and evidence independently of predictor behavior before evaluation. |

The outcome comparison and cited event evidence are summarized in [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md). The project lead has selected the farm-gate target; the operational label rule remains open.

## Previous FPMA screen: superseded for selected target

The earlier local FPMA proxy sensitivity screen is not used for the selected project target and must not be carried forward as evidence about FAOSTAT farm-gate producer-price stress. FPMA raw values and derived analyses remain local pending rights/metadata resolution.

It remains a separate exploratory source check; its label frequencies and real-price calculations are excluded from shared reporting because they are tied to an unresolved third-party source.

**Deliverable outcome:** public target source and CPI source selected; the farm-gate CPI-adjusted candidate is reproducible and shared; event/source alignment and M0–M4 feature eligibility are profiled. Still open: finalize label formula and horizon, compute label sensitivity and independent episodes, approve split/threshold procedure, integrate the selected source into the Spark M0–M4 pipeline, and implement M0. Week 4 remains in progress.

### CPI source, reuse and real-price construction

The shared SSSU all-items CPI snapshot contains 422 consecutive observations from August 1991 through September 2026. Ukraine's Official Statistics Law 2524-IX, Article 27(1), gives users the right to use, disseminate, and store official state statistical information; Article 27(3) requires identifying the producer when publishing or distributing it. Cite the State Statistics Service of Ukraine (SSSU) and retain its source metadata and territorial coverage caveat. See the [SSSU SDMX API guide](https://stat.gov.ua/en/development-api/sdmx-api-v3), [Law 2524-IX](https://zakon.rada.gov.ua/laws/show/2524-20), and the retrieval record in [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md).

The series is monthly CPI change relative to the previous month, not an index level. `scripts/profile_week4_price_deflator.py` chains it with December 2000 set to 100 and computes `real LCU/tonne = nominal FAOSTAT producer price × 100 / chained CPI`. The output has 157 monthly records from January 2010 through January 2023, with complete monthly price observations through December 2022. This provides consistent input/output provenance and is the selected working deflator for the farm-gate pilot. The all-items CPI is a broad inflation adjustment, not a wheat-specific cost or affordability measure. This result is not an IFPA score.

The FAOSTAT CP archive extract remains local because its general CPI values carry the `X` external-source flag and separate underlying reuse terms are unconfirmed. It is not needed for the selected public-source outcome. FPMA/APK-Inform remains local as an optional comparison source and is not needed for the shared target.

### Candidate price deflator profile

The reproducible screen in `scripts/profile_week4_price_deflator.py` chains the SSSU month-over-month all-items CPI observations with December 2000 set to 100. It deflates FAOSTAT monthly wheat producer prices using `real LCU/tonne = nominal LCU/tonne × 100 / chained CPI`. The output is `data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv`; its sidecar records formula and input/output hashes. There are 157 consecutive monthly records from January 2010 through January 2023, with complete calendar years through 2022 and all source wheat-price rows flagged `A`.

This resolves the mechanical CPI-chain and join-feasibility item for the selected farm-gate outcome. It does not define the stress label: all-items CPI is a broad inflation deflator, not a wheat-specific stress measure. The output is not an IFPA score. The operational target rule and horizon remain open; the FPMA wholesale series is not required for this pilot.

## Week 4 work order

1. Define and profile candidate future producer-price-stress labels from the shared FAOSTAT/SSSU candidate; report positive months and independent event episodes against the common M0–M4 dates.
2. Select the prediction cutoff, primary forecast horizon, stress rule, and positive/negative examples. Select any alert threshold on training/validation only; never tune on a held-out event.
3. Instantiate project-level train/validation/test rules with dates from verified eligible coverage. The proposed date ranges below are a starting point only; consider rolling-origin evaluation if the final event sample is small.
4. Build and profile M0 using only information available at or before each cutoff. Then compare M1–M4 under the shared evaluation contract in [`ARCHITECTURE.md`](ARCHITECTURE.md), reporting warning lead time, missed events/false alarms, and agreed metrics.

### Candidate chronological schedule (proposed, not locked)

| Period | Role | Use |
|---|---|---|
| 2010 | Price-history warmup | First complete source year; allow 12-month feature history to accrue, but do not treat the 2010/11 context as a final test. |
| 2011–2017 | Training candidate | Fit each model stage and learned preprocessing on these earlier eligible origins only. |
| 2018–2020 | Validation candidate | Select feature/model settings and alert thresholds; keep a horizon-length purge before the test period. |
| 2021–2022 | Final-test candidate | Evaluate once after choices are fixed; for a three-month outcome window, the latest fully observed M4 origin is October 2022. Apply a horizon-length purge at the boundary. |

The three-month horizon is a working candidate, not a confirmed decision. For it, the source-only feature profile identifies 119 eligible M4 origins through October 2022 and nine origins (February–October 2022) within the global 2022 event context. Feature completeness excludes January observations because annual-signal anomaly variance can be zero; M3/M4 also exclude February–December 2016 because trade anomaly features are undefined. Exact monthly eligibility is in the generated CSV. The calendar above is a proposal, not a locked split. Apply purges only after confirming the horizon and label interval. Since the 2022 event sample is small, add rolling-origin backtests using only prior data and report uncertainty rather than relying on one event alone.

Project-wide candidate feature groups and the data model are documented in [`ARCHITECTURE.md`](ARCHITECTURE.md). The selected price source and deflator are FAOSTAT producer prices and SSSU CPI. The stress formula, horizon, alert-threshold procedure, and date split remain open; do not report classification performance until label sensitivity and positive-event counts have been profiled without holdout tuning.

## Next item requiring team decision

The price concept and public source are selected: FAOSTAT farm-gate producer price with SSSU CPI adjustment. The next decisions are the label formula, primary horizon, sensitivity checks, and chronological split. Then integrate this source into the Spark M0–M4 pipeline, implement M0, and profile labels on identical eligible origins. Do not select test events by inspecting model results or treat source overlap as a validated evaluation period.

Warning lead time is the interval between the first valid model warning and the later onset of price stress. Processing/alert latency is elapsed system runtime from input to output; that is a separate streaming/scalability measure, not the historical warning-lead-time objective shown in the proposal slide.

The separate course scalability requirement can proceed in parallel under [`SCALABILITY_EXPERIMENT.md`](SCALABILITY_EXPERIMENT.md); it does not require a final model outcome.
