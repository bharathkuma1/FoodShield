# Week 4 evaluation protocol: open draft

**Status:** Project-level scope and evaluation principles are in [`ARCHITECTURE.md`](ARCHITECTURE.md). This Week 4 document tracks the remaining target, horizon, threshold, event, and calendar decisions needed to instantiate that design. No target formula, horizon, event holdout, or train/validation/test date is locked. Do not report model metrics until these are defined and implemented.

## What timeframe is currently available?

| Input family | Current Ukraine coverage | Consequence |
|---|---|---|
| FPMA monthly wheat price | January 2001–October 2026 | Supports a longer M0-only history, subject to unresolved source-partner reuse terms and a metadata mismatch. |
| NASA POWER weather | January 2001–December 2022 | Caps the current price/weather overlap at December 2022. It is a single-point screening series, not representative wheat-area exposure. |
| FAOSTAT wheat production and trade | Annual reference years 1992–2024 | These are features, not monthly outcomes; the current availability lag is provisional, so the input years must be shifted to when they could have been available. |

The nominal Ukraine all-signal input overlap is January 2001–December 2022 before lookback and missingness rules. The source coverage, sharing status, feature completeness, and India comparison are maintained at project level in [`ARCHITECTURE.md`](ARCHITECTURE.md). The proposal event contexts fall inside the nominal overlap, but are not selected labels or locked test periods. See [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md) for case-specific evidence.

## Outcome options already profiled

| Candidate | What it would measure | Current boundary |
|---|---|---|
| Future domestic wheat-price stress | Whether information available through month `t` gives warning before a defined domestic price-stress condition in a later period. | **Selected target type.** It uses the same price series at a later time, so it is separated in time but is not an independent data source. FPMA reuse rights and metadata must be addressed before any public result. No horizon or stress rule is selected. |
| Monthly wheat export-flow disruption | Changes in reported wheat exports, useful as a separate supply/trade outcome. | Full monthly coverage has not been established; sparse query samples cannot define missing months or event labels. Do not use it until the complete candidate window is profiled. |
| Annual production or trade | Slow-moving supply context. | Annual cadence makes these unsuitable as monthly outcomes in the current design; retain them as lagged features. |
| Proposal event contexts | Historical context for wheat-market disruptions. | Global event descriptions do not automatically establish a country-month outcome. Define geography, onset, and evidence independently of predictor behavior before evaluation. |

The outcome comparison and cited evidence are already summarized in [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md). This draft does not approve its recommendation.

## First deliverable: local target feasibility screen

**Status:** Preliminary, local-only sensitivity screen; not the approved target formula, a model result, or a public data release.

The local FPMA response contains provider-calculated monthly observations from January 2001 through October 2026. The monthly `price_value_real` field is present for all but the latest month in this snapshot; the series metadata does not explain the exact deflator/calculation for this API field. FAO's official [SDG 2.c.1 methodology](https://unstats.un.org/sdgs/metadata/files/Metadata-02-0C-01.pdf) says its price-anomaly indicator uses real prices to account for inflation, requires a five-year series, and combines standardized quarterly and annual compound-growth anomalies. This supports a CPI-adjusted target design but does **not** verify that the local API field follows that exact calculation. Preserve the provider-calculated monthly series and do not average weekly rows into a replacement monthly series. The official [FAO database terms](https://www.fao.org/contact-us/terms/db-terms-of-use/en) state that third-party datasets may have separate reuse restrictions; FAO identifies [APK-Inform as this series' data partner](https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/), but no public permission covering this series' redistribution/reuse was identified in this feasibility check. Keep the data and derived outputs local until terms are confirmed. The Retail/Wholesale metadata discrepancy also remains unresolved.

To see whether a simple future-stress label would be usable, a local screening proxy was calculated as follows: for each month with five previous same-calendar-month real-price values, take the z-score of the current log real price against the mean and sample standard deviation of those five previous same-month log prices. This uses only prior years for the reference. It is a simple seasonal-level proxy, **not** FAO's official Indicator of Food Price Anomalies (IFPA), whose published method combines CPI-adjusted quarterly and annual compound-growth anomalies. The official IFPA categories (moderately high at 0.5–<1; abnormally high at ≥1) apply to the official IFPA score; they must not be transferred to this custom proxy as if the scores were equivalent. See the [FAO SDG 2.c.1 metadata](https://unstats.un.org/sdgs/metadata/files/Metadata-02-0C-01.pdf) and [GIEWS/FPMA anomaly-method paper](https://www.fao.org/giews/food-prices/research/detail/en/c/235685/).

For candidate forecast origins January 2006–December 2022 (204 months), a positive `H`-month label means at least one of the next `H` monthly proxy scores reaches the listed cutoff. Outcome prices are available after December 2022 in the local FPMA snapshot, so labels for late-2022 origins can use future outcome months without extending the predictor-input window beyond the weather period.

| Proxy score cutoff | H=1 positive origins | H=3 positive origins | H=6 positive origins | Interpretation for sensitivity only |
|---:|---:|---:|---:|---|
| 0.5 | 108/204 (52.9%) | 148/204 (72.5%) | 168/204 (82.4%) | Too frequent for a useful primary binary warning target in this proxy. |
| 1.0 | 69/204 (33.8%) | 105/204 (51.5%) | 135/204 (66.2%) | Still labels about half of three-month origins positive. |
| 1.5 | 27/204 (13.2%) | 46/204 (22.5%) | 73/204 (35.8%) | More selective; plausible candidate for follow-up profiling. |
| 2.0 | 12/204 (5.9%) | 21/204 (10.3%) | 33/204 (16.2%) | Strict; leaves few positive origins and only three contiguous high-score episodes in the scored price history through March 2023. |

Overlapping future windows cause adjacent origins to share the same stress month, so these positive-origin counts are not independent event counts. The screen suggests that a three-month horizon is workable for sensitivity analysis, while a 0.5 or 1.0 cutoff may be too common under this particular proxy. A 1.5 cutoff is a useful comparison candidate, not a selected threshold. Validate candidate labels against the proposal's externally documented contexts, explain the 2022 local-price behavior, and check a provider/official IFPA measure before fixing the rule. The current proxy's effective date range is not yet intersected with the exact M4 feature-complete dates.

**Deliverable outcome:** price-series fields and monthly coverage profiled; candidate-label prevalence sensitivity recorded; official target-method and third-party-rights boundaries checked. Still open: verify the exact `price_value_real` calculation or obtain CPI and calculate real prices explicitly; compute and profile an official-faithful IFPA-style score; inspect event-level label alignment; compute exact common M0–M4 dates; and obtain/confirm reuse terms. Week 4 remains in progress.

## Week 4 work order

1. Profile candidate future price-stress labels, the FPMA real-price field definition, source rights, and metadata; establish positive-event counts and independently documented context.
2. Select the prediction cutoff, primary forecast horizon, stress rule, and positive/negative examples. Select any alert threshold on training/validation only; never tune on a held-out event.
3. Instantiate project-level train/validation/test rules with dates from verified eligible coverage. The proposed date ranges below are a starting point only; consider rolling-origin evaluation if the final event sample is small.
4. Build and profile M0 using only information available at or before each cutoff. Then compare M1–M4 under the shared evaluation contract in [`ARCHITECTURE.md`](ARCHITECTURE.md), reporting warning lead time, missed events/false alarms, and agreed metrics.

### Candidate chronological schedule (proposed, not locked)

| Period | Role | Use |
|---|---|---|
| 2001–2005 | Price-history warmup | Supply the initial five-year seasonal/anomaly reference where applicable; do not score model performance here. |
| 2006–2015 | Training | Fit each model stage and any learned preprocessing on this partition only. |
| 2016–2018 | Validation | Choose feature/model settings and alert thresholds; do not fit final model parameters from these rows under the current no-refit protocol. |
| 2019–2022 | Final test | Evaluate once after choices are fixed; no model fitting, threshold tuning, or feature selection from these rows. |

The candidate price target uses a three-month primary horizon, with shorter and longer horizons as sensitivity checks. Insert a three-month purge at each boundary if that horizon is confirmed. These dates are a workable starting split within the nominal Ukraine price/weather overlap, not a claim that every month in these partitions is eligible. Recompute the exact common M0–M4 dates after the final target and feature availability rules are applied. Run proposal-event rolling-origin checks as supplemental historical checks, each trained only on dates before that fold; do not use them to tune the final holdout after inspecting its results.

Project-wide candidate feature groups and the data model are documented in [`ARCHITECTURE.md`](ARCHITECTURE.md). The latest target recommendation—a leakage-controlled price-anomaly definition with a proposed three-month primary horizon and shorter/longer sensitivity checks—remains open until the FPMA real-price field definition (or CPI inputs if recomputing real prices), positive-event counts, source rights, and label behavior are profiled and the team confirms it.

## First item to start

Start by implementing and profiling candidate target labels (including the proposed anomaly rule) on the local historical price series, then report positive months/events and the exact M0–M4 common eligible month range. Confirm the stress formula and horizon only after reviewing that profile. Then set the chronological partitions and run M0 as a baseline. Do not select test events by inspecting model results or treat nominal source overlap as a validated evaluation period.

Warning lead time is the interval between the first valid model warning and the later onset of price stress. Processing/alert latency is elapsed system runtime from input to output; that is a separate streaming/scalability measure, not the historical warning-lead-time objective shown in the proposal slide.

The separate course scalability requirement can proceed in parallel under [`SCALABILITY_EXPERIMENT.md`](SCALABILITY_EXPERIMENT.md); it does not require a final model outcome.
