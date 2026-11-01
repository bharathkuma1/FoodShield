# Week 4 evaluation protocol: open draft

**Status:** Feasibility framing only. The team has not selected an outcome, horizon, threshold, event holdout, or exact train/validation/test dates. Do not report model metrics until those are approved and implemented.

## What timeframe is currently available?

| Input family | Current Ukraine coverage | Consequence |
|---|---|---|
| FPMA monthly wheat price | January 2001–October 2026 | Supports a longer M0-only history, subject to unresolved source-partner reuse terms and a metadata mismatch. |
| NASA POWER weather | January 2001–December 2022 | Caps the current price/weather overlap at December 2022. It is a single-point screening series, not representative wheat-area exposure. |
| FAOSTAT wheat production and trade | Annual reference years 1992–2024 | These are features, not monthly outcomes; the current availability lag is provisional, so the input years must be shifted to when they could have been available. |

The nominal all-signal input overlap is January 2001–December 2022 before lookback and missingness rules. M0 can extend later, but M0–M4 comparisons should use the same eligible months. The candidate proposal event contexts fall inside that nominal overlap, but they are not selected labels or locked test periods. See [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md) for source-specific coverage and caveats.

## Outcome options already profiled

| Candidate | What it would measure | Current boundary |
|---|---|---|
| Future domestic wheat-price stress | Whether information available through month `t` gives warning before a defined domestic price-stress condition in a later period. | Closest operational interpretation of the research question. It uses the same price series at a later time, so it is separated in time but is not an independent data source. FPMA reuse rights and metadata must be addressed before any public result. No horizon or stress rule is selected. |
| Monthly wheat export-flow disruption | Changes in reported wheat exports, useful as a separate supply/trade outcome. | Full monthly coverage has not been established; sparse query samples cannot define missing months or event labels. Do not use it until the complete candidate window is profiled. |
| Annual production or trade | Slow-moving supply context. | Annual cadence makes these unsuitable as monthly outcomes in the current design; retain them as lagged features. |
| Proposal event contexts | Historical context for wheat-market disruptions. | Global event descriptions do not automatically establish a country-month outcome. Define geography, onset, and evidence independently of predictor behavior before evaluation. |

The outcome comparison and cited evidence are already summarized in [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md). This draft does not approve its recommendation.

## Week 4 work order

1. Decide whether the primary outcome must be independent by **time** (future values after the prediction cutoff) or by **source** (a separate observation source). This distinction determines whether future FPMA prices are acceptable or only a baseline/secondary target.
2. Profile the chosen outcome's rights, temporal coverage, release timing, missingness, geographic meaning, and relationship to the proposal event contexts.
3. Define the prediction cutoff, forecast horizon, outcome rule, and positive/negative examples. Derive any threshold from training/validation data only; never tune it on a held-out event.
4. Fix chronological training, validation, and test periods from verified common coverage. The proposed three-to-five pre-event training years is only an initial guideline, not an approved split or maximum; consider longer history or rolling-origin folds if coverage permits.
5. Implement and profile M0 using only features available at or before each cutoff. Compare it with later M1–M4 stages only on common eligible months and report missed events/false alarms as well as agreed metrics.

## First item to start

Start with item one: clarify the required meaning of an independent outcome. Then use the already documented source profiles to decide which candidate is actually supportable. Do not begin M0 metric reporting, select test events by inspecting price spikes, or treat the nominal source overlap as a validated evaluation period.

The separate course scalability requirement can proceed in parallel under [`SCALABILITY_EXPERIMENT.md`](SCALABILITY_EXPERIMENT.md); it does not require a final model outcome.
