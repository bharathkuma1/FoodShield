# Week 4 evaluation protocol: open draft

**Status:** The project lead selected future domestic wheat-price stress as the target type. The exact stress rule, horizon, threshold, event holdout, and train/validation/test dates remain open. The comparison contract below is the recommended common protocol; proposed parameter values remain to be confirmed after profiling labels and eligible months. Do not report model metrics until these are defined and implemented.

## What timeframe is currently available?

| Input family | Current Ukraine coverage | Consequence |
|---|---|---|
| FPMA monthly wheat price | January 2001–October 2026 | Supports a longer M0-only history, subject to unresolved source-partner reuse terms and a metadata mismatch. |
| NASA POWER weather | January 2001–December 2022 | Caps the current price/weather overlap at December 2022. It is a single-point screening series, not representative wheat-area exposure. |
| FAOSTAT wheat production and trade | Annual reference years 1992–2024 | These are features, not monthly outcomes; the current availability lag is provisional, so the input years must be shifted to when they could have been available. |

The nominal all-signal input overlap is January 2001–December 2022 before lookback and missingness rules. M0 can extend later, but M0–M4 comparisons should use the same eligible months. The candidate proposal event contexts fall inside that nominal overlap, but they are not selected labels or locked test periods. See [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md) for source-specific coverage and caveats.

## Source readiness and team sharing

| Signal | Current Ukraine availability | Current quality/sharing boundary |
|---|---|---|
| FPMA monthly wheat price | Local series spans January 2001–October 2026; only the January 2001–December 2022 period overlaps the current weather sample. | Raw series and real-source derived features are Git-ignored/local pending confirmation of APK-Inform reuse rights. Metadata also has a Retail/Wholesale mismatch. The team must not assume the price rows or real-source Parquet are present on the remote branch. |
| NASA POWER weather | Daily point sample January 2001–December 2022; monthly aggregation covers 264 of the 310 price months. | Ukraine point snapshot and request metadata are tracked on the shared branch. It is one central point, not a representative or crop-weighted national signal. |
| FAOSTAT wheat production | Annual 1992–2024; production, yield, and harvested area. | Filtered Ukraine extract and provenance sidecar are tracked on the shared branch. |
| FAOSTAT wheat trade | Annual 1992–2024; import/export quantity and value. | Filtered Ukraine extract and provenance sidecar are tracked on the shared branch. Flags and units are retained. |

The existing local Week 3 profile has 220 M4 feature-complete rows out of 310 price-month rows after source coverage and lookback requirements. This is a row-count profile, not a verified contiguous evaluation interval; derive and record the exact first/last eligible months after the outcome rule is selected. Production/trade joins are available across the local price panel only under the provisional two-year lag; historical publication vintages have not been verified.

India is not currently a like-for-like multi-year backup. Its profiled FPMA price series spans January 2014–September 2026, while the local India weather extract is 2022-only; its filtered India FAOSTAT files are not tracked on the shared branch. It could support a 2022 comparison or become a full alternative after consistent historical wheat-area weather and shared/reproducible inputs are assembled. No other profiled country currently has a better verified all-signal common period. Reassess if source rights or coverage change.

## Outcome options already profiled

| Candidate | What it would measure | Current boundary |
|---|---|---|
| Future domestic wheat-price stress | Whether information available through month `t` gives warning before a defined domestic price-stress condition in a later period. | **Selected target type.** It uses the same price series at a later time, so it is separated in time but is not an independent data source. FPMA reuse rights and metadata must be addressed before any public result. No horizon or stress rule is selected. |
| Monthly wheat export-flow disruption | Changes in reported wheat exports, useful as a separate supply/trade outcome. | Full monthly coverage has not been established; sparse query samples cannot define missing months or event labels. Do not use it until the complete candidate window is profiled. |
| Annual production or trade | Slow-moving supply context. | Annual cadence makes these unsuitable as monthly outcomes in the current design; retain them as lagged features. |
| Proposal event contexts | Historical context for wheat-market disruptions. | Global event descriptions do not automatically establish a country-month outcome. Define geography, onset, and evidence independently of predictor behavior before evaluation. |

The outcome comparison and cited evidence are already summarized in [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md). This draft does not approve its recommendation.

## Week 4 work order

1. Confirm the target's interpretation as domestic market-price stress, not physical shortage, and keep the same-source/future-time construction explicit.
2. Profile FPMA reuse terms, temporal coverage, release timing, missingness, metadata, and the target's relation to independently documented event contexts.
3. Define the prediction cutoff, forecast horizon, price-stress rule, and positive/negative examples. Derive any threshold from training/validation data only; never tune it on a held-out event.
4. Fix chronological training, validation, and test periods from verified common coverage. The proposed three-to-five pre-event training years is only an initial guideline, not an approved split or maximum; consider longer history or rolling-origin folds if coverage permits.
5. Implement and profile M0 using only features available at or before each cutoff. Compare it with later M1–M4 stages only on common eligible months and report warning lead time, missed events/false alarms, and agreed metrics.

## Common M0–M4 comparison contract

- Use identical forecast-origin months, target labels, chronological partitions, and horizon for M0, M1, M2, M3, and M4. Each stage changes the predictors only.
- The primary comparison uses the intersection of months where every required predictor stage is eligible and the target is defined. M0 may also have a separate price-only extended-coverage result, clearly marked as supplementary and not directly comparable to the primary ablation.
- Fit learned preprocessing and model parameters from the training partition only. Use validation to select model settings and alert thresholds; do not use validation or test rows to fit the model. Keep the final test untouched until choices are fixed.
- Leave a purge gap at each partition boundary equal to the selected forecast horizon, so forward-looking labels cannot include observations from a later partition. Keep all features point-in-time: each predictor must have been available by its forecast origin.
- Report source and feature completeness for each stage, plus the common comparison sample. Never fill unavailable weather/annual signals with zero merely to retain rows.
- Use benchmark-only synthetic data, if needed, solely for Spark/Kafka scale measurements. Keep it separate from real-data model training, validation, and test results.

This common-sample design follows the professor's request for an ablation comparison: differences in results should reflect the added signal groups rather than different test dates or labels.

### Candidate chronological schedule (proposed, not locked)

| Period | Role | Use |
|---|---|---|
| 2001–2005 | Price-history warmup | Supply the initial five-year seasonal/anomaly reference where applicable; do not score model performance here. |
| 2006–2015 | Training | Fit each model stage and any learned preprocessing on this partition only. |
| 2016–2018 | Validation | Choose feature/model settings and alert thresholds; do not fit final model parameters from these rows under the current no-refit protocol. |
| 2019–2022 | Final test | Evaluate once after choices are fixed; no model fitting, threshold tuning, or feature selection from these rows. |

The candidate price target uses a three-month primary horizon, with shorter and longer horizons as sensitivity checks. Insert a three-month purge at each boundary if that horizon is confirmed. These dates are a workable starting split within the nominal Ukraine price/weather overlap, not a claim that every month in these partitions is eligible. Recompute the exact common M0–M4 dates after the final target and feature availability rules are applied. Run proposal-event rolling-origin checks as supplemental historical checks, each trained only on dates before that fold; do not use them to tune the final holdout after inspecting its results.

## Candidate variable contract for the submission

The professor's report checklist asks for the data model, methods, platforms, evaluation design, metrics, scalability goals, plots, analysis, and summary. The following variables make those methods concrete; they are candidate features, not additional project objectives. Final formulas and exclusions belong in the versioned feature/target schema.

| Variable group | Candidate fields | Stage / interpretation |
|---|---|---|
| Panel key and provenance | `country_code`, `commodity_code`, `month`, source/series ID, unit/currency, availability date, quality flags, spatial basis | Shared country–wheat–month grain; metadata, not model predictors. |
| Price history | Real wheat price; lags (1, 3, 6, 12 months); prior-window means/standard deviations; month-over-month change; past-only price anomaly | M0 baseline; keep nominal, real, and USD fields distinct. |
| Weather | Monthly corrected precipitation total; mean daily maximum and minimum temperature; valid-day counts; location/spatial method; past-only lags/anomalies | M1. Current Ukraine values are single-point screening features. |
| Production | Annual wheat production (t), yield (kg/ha), area harvested (ha), reference year, `feature_available_from`, flag/note | M2; join only annual observations available by the forecast month. Current two-year lag is a proxy. |
| Trade | Annual import/export quantity (t) and value (1000 USD), reference year, `feature_available_from`, flag/note | M3; quantity and value are separate measures and use the same as-of rule. |
| Temporal/cross-signal | Selected lagged relationships, rolling/anomaly features, and price–weather interaction | M4; calculate with past-only windows and document feature selection. |
| Target and evaluation | Future price-stress score/label at the selected horizon; model alert score/flag; split ID; forecast origin; event onset and warning lead time | Target formula, threshold and horizon remain open; none may be derived from predictor behavior or tuned on final test. |

The latest candidate target recommendation is a leakage-controlled, price-anomaly definition with a three-month primary horizon and sensitivity checks at shorter/longer horizons. Treat it as **proposed** until the FPMA real-price field definition (or CPI inputs if recomputing real prices), positive-event counts, source rights, and label behavior are profiled and the team confirms it.

## First item to start

Start by implementing and profiling candidate target labels (including the proposed anomaly rule) on the local historical price series, then report positive months/events and the exact M0–M4 common eligible month range. Confirm the stress formula and horizon only after reviewing that profile. Then set the chronological partitions and run M0 as a baseline. Do not select test events by inspecting model results or treat nominal source overlap as a validated evaluation period.

Warning lead time is the interval between the first valid model warning and the later onset of price stress. Processing/alert latency is elapsed system runtime from input to output; that is a separate streaming/scalability measure, not the historical warning-lead-time objective shown in the proposal slide.

The separate course scalability requirement can proceed in parallel under [`SCALABILITY_EXPERIMENT.md`](SCALABILITY_EXPERIMENT.md); it does not require a final model outcome.
