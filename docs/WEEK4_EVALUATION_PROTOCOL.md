# Week 4 evaluation protocol: open draft

**Status:** The project lead selected the 3-month/10% real-price-rise rule as the current working choice; mark it **review needed** and revise if team review warrants. The candidate chronological split and M0 alert-threshold method also remain review items. A bounded Spark selected-source feature build and training/validation M0 calibration now run successfully. Final-test labels and scores remain untouched.

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
| Future Ukraine farm-gate wheat producer-price stress | Whether information available through month `t` warns of a defined farm-gate producer-price increase in a later period. | **Current working target and price source; review needed.** Use FAOSTAT producer price as M0 history and future outcome, deflated by attributed SSSU CPI. The working label is a ≥10% rise within the next 3 months. This is time-separated but not independent by source. |
| Monthly wheat export-flow disruption | Changes in reported wheat exports, useful as a separate supply/trade outcome. | Full monthly coverage has not been established; sparse query samples cannot define missing months or event labels. Do not use it until the complete candidate window is profiled. |
| Annual production or trade | Slow-moving supply context. | Annual cadence makes these unsuitable as monthly outcomes in the current design; retain them as lagged features. |
| Proposal event contexts | Historical context for wheat-market disruptions. | Global event descriptions do not automatically establish a country-month outcome. Define geography, onset, and evidence independently of predictor behavior before evaluation. |

The outcome comparison and cited event evidence are summarized in [`WEEK1_EVENT_CASES.md`](WEEK1_EVENT_CASES.md). The project lead has selected the farm-gate target type and a provisional operational label rule; both the label rule and evaluation calendar carry a review-needed status.

## Previous FPMA screen: superseded for selected target

The earlier local FPMA proxy sensitivity screen is not used for the selected project target and must not be carried forward as evidence about FAOSTAT farm-gate producer-price stress. FPMA raw values and derived analyses remain local pending rights/metadata resolution.

It remains a separate exploratory source check; its label frequencies and real-price calculations are excluded from shared reporting because they are tied to an unresolved third-party source.

**Deliverable outcome:** public target source and CPI source selected; the farm-gate CPI-adjusted candidate is reproducible; event/source alignment, M0–M4 feature eligibility, and train/validation-only label sensitivity are profiled. Current working choice: 3-month horizon and a ≥10% real-price rise, marked review needed. The candidate split is also review needed. The selected-source Spark feature integration and M0 training/validation calibration have run successfully; do not use final-test labels until review is complete.

## Candidate target and horizon sensitivity (provisional)

`scripts/profile_week4_stress_labels.py` creates candidate binary labels only on common M0–M4 feature-eligible origins in the proposed training and validation periods. It deliberately does not calculate or emit labels for origins on or after 2021-01. To keep definitions comparable, all candidates use common origins with 36 consecutive real-price observations available through forecast month `t`. The future-level candidate compares months `t+1` through `t+h` against that trailing history; the real-price-rise candidate compares the maximum price over `t+1` through `t+h` directly with the price at `t`. Boundaries are purged by each horizon. The proposed calendar is still only a candidate; the saved label counts are feasibility evidence, not performance results.

For a three-month horizon, two candidate definitions were compared:

| Candidate label | Training positives / eligible origins (episodes) | Validation positives / eligible origins (episodes) | Interpretation |
|---|---:|---:|---|
| Maximum future real price at least 5% above origin-month real price | 15/42 (6) | 14/30 (6) | More frequent, less severe increase. |
| **Maximum future real price at least 10% above origin-month real price** | **12/42 (4)** | **9/30 (5)** | **Recommended provisional primary target:** directly measures a material real-price rise while retaining multiple validation episodes. |
| Maximum future real price at least 15% above origin-month real price | 9/42 (4) | 4/30 (2) | More severe, but validation support is sparse. |
| Future price exceeds trailing 36-month real-price 90th percentile | 12/42 (5) | 15/30 (2) | Measures an unusually high level, not the size of a rise; labels cluster into few long episodes. |

The scan also covers 1- and 6-month horizons and 5/10/15% rise thresholds, plus 80th/90th/95th-percentile historical-level cutoffs. The configurable label run uses all common M0–M4 origins that have the required 36-month history and whose complete outcome window fits inside the training or validation period. With the 10% rise rule, the 1-month case has 3/44 training and 2/32 validation positives (3 and 2 episodes); the 3-month case has 12/42 and 9/30 (4 and 5 episodes); the 6-month case has 21/39 and 13/27 (4 and 4 episodes). The 3-month option gives a useful compromise between a short warning window with few labels and a longer window with more frequent positives. Across this short validation period, the 3-month 10% rise rule gives similar positive-month rates in training (29%) and validation (30%). The 15% rule gives only two validation episodes; the trailing-quantile rule produces long clustered episodes and a higher validation rate. These small counts do not establish predictive value. An episode is currently a contiguous run of positive eligible calendar origins; a gap or purged month separates episodes.

**Current working choice — review needed:** use a 3-month forecast horizon and label a positive when the maximum CPI-adjusted FAOSTAT farm-gate price over the next three months is at least 10% above the real price observed at origin `t`. Use 5% and 15% as target-definition sensitivity checks, and 1/6 months as horizon sensitivity checks. The real-price rise threshold defines the outcome; it is separate from the model alert-score threshold, which is selected using validation only. The project lead requested that the team review this choice and revise it later if needed; see `DECISIONS.md`.

The detailed rule-by-period counts and row-level train/validation labels are in `data/processed/week4_stress_label_sensitivity.json` and `.csv`. These current candidate artifacts are committed for the team and can be regenerated from configured inputs. The script records input hashes and the no-holdout-label rule. No test-period labels, alert scores, or model metrics are included.

### CPI source, reuse and real-price construction

The shared SSSU all-items CPI snapshot contains 422 consecutive observations from August 1991 through September 2026. Ukraine's Official Statistics Law 2524-IX, Article 27(1), gives users the right to use, disseminate, and store official state statistical information; Article 27(3) requires identifying the producer when publishing or distributing it. Cite the State Statistics Service of Ukraine (SSSU) and retain its source metadata and territorial coverage caveat. See the [SSSU SDMX API guide](https://stat.gov.ua/en/development-api/sdmx-api-v3), [Law 2524-IX](https://zakon.rada.gov.ua/laws/show/2524-20), and the retrieval record in [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md).

The series is monthly CPI change relative to the previous month, not an index level. `scripts/profile_week4_price_deflator.py` chains it with December 2000 set to 100 and computes `real LCU/tonne = nominal FAOSTAT producer price × 100 / chained CPI`. The output has 157 monthly records from January 2010 through January 2023, with complete monthly price observations through December 2022. This provides consistent input/output provenance and is the selected working deflator for the farm-gate pilot. The all-items CPI is a broad inflation adjustment, not a wheat-specific cost or affordability measure. This result is not an IFPA score.

The FAOSTAT CP archive extract remains local because its general CPI values carry the `X` external-source flag and separate underlying reuse terms are unconfirmed. It is not needed for the selected public-source outcome. FPMA/APK-Inform remains local as an optional comparison source and is not needed for the shared target.

### Candidate price deflator profile

The reproducible screen in `scripts/profile_week4_price_deflator.py` chains the SSSU month-over-month all-items CPI observations with December 2000 set to 100. It deflates FAOSTAT monthly wheat producer prices using `real LCU/tonne = nominal LCU/tonne × 100 / chained CPI`. The output is `data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv`; its sidecar records formula and input/output hashes. There are 157 consecutive monthly records from January 2010 through January 2023, with complete calendar years through 2022 and all source wheat-price rows flagged `A`.

This resolves the mechanical CPI-chain and join-feasibility item for the selected farm-gate outcome. It does not make CPI wheat-specific: it is a broad inflation deflator. The output is not an IFPA score. The working stress label is documented above and remains review needed; the FPMA wholesale series is not required for this pilot.

## Week 4 work order

1. **Done:** profile candidate labels/horizons on common M0–M4 dates, with training/validation counts and episodes only. The 3-month/10% rise remains the working choice; review needed.
2. **Done:** generate labels from configured inputs and train the price-only M0 MLlib logistic-regression model. Train-only fitting and validation-only cutoff selection are implemented; final-test labels and predictions are excluded.
3. **In progress:** have the team review and lock the target, forecast horizon, chronological split and purge rules. Add rolling-origin evaluation because the final event sample is small. Keep the final-test set unopened until those choices are recorded.
4. **In progress:** the selected-source Spark feature pipeline and M0 baseline are implemented. Implement M1–M4 with the same classifier and common-origin contract, then report warning lead time, missed events/false alarms and the agreed metrics after protocol review.

### Candidate chronological schedule (proposed, not locked)

| Period | Role | Use |
|---|---|---|
| 2010 | Price-history warmup | First complete source year; allow 12-month feature history to accrue, but do not treat the 2010/11 context as a final test. |
| 2011–2017 | Training candidate | Fit each model stage and learned preprocessing on these earlier eligible origins only. |
| 2018–2020 | Validation candidate | Select feature/model settings and alert thresholds; keep a horizon-length purge before the test period. |
| 2021–2022 | Final-test candidate | Evaluate once after choices are fixed; for a three-month outcome window, the latest fully observed M4 origin is October 2022. Apply a horizon-length purge at the boundary. |

The three-month horizon is the current working choice, marked review needed. For it, the source-only feature profile identifies 119 eligible M4 origins through October 2022 and nine origins (February–October 2022) within the global 2022 event context. Feature completeness excludes January observations because annual-signal anomaly variance can be zero; M3/M4 also exclude February–December 2016 because trade anomaly features are undefined. Exact monthly eligibility is in the generated CSV. The calendar above is a candidate split, marked review needed. Current development labels are purged at train/validation boundaries; final-test labels are not generated. Since the 2022 event sample is small, add rolling-origin backtests using only prior data and report uncertainty rather than relying on one event alone.

## Spark feature and M0 run

Run the selected-source panel and price-only baseline from the repository root:

```powershell
python scripts\profile_week4_stress_labels.py
python scripts\run_week4_m0.py --config configs\week4.local.json
```

The Week 4 runner reads the selected monthly real-price candidate and attaches the configured NASA weather and FAOSTAT QCL/TCL inputs using the existing Spark readers. It does not read the local FPMA price file. Country/commodity and source paths are selected through `configs/week2.local.json` and `configs/week4.local.json`; the label profiler reads those same configured price/eligibility paths and writes the configured label outputs. The current paths point to Ukraine wheat. The generated feature panel is `data/processed/week4_ukraine_faostat_features/`; the M0 profile and train/validation scores are under `data/processed/week4_ukraine_m0_candidate/`. These are derived outputs. The profile matched the source-only eligibility counts: 157 panel months; M0 145 feature-complete origins; M1 144; M2 132; and M3/M4 121.

M0 uses only `price_real_anomaly_z_12m` and `price_real_mom_change`, with Spark MLlib binomial logistic regression as the detector. The estimator is fit on 42 training origins (12 positive, 30 negative); MLlib standardization and L2 regularization are used. The model is then applied to 30 validation origins (9 positive, 21 negative), where the alert cutoff is selected by F1. The 3-month/10% rule defines the supervised future outcome label; it is not the detector. The selected cutoff is 0.2377; validation calibration has 8 true positives, 11 false positives, and 1 false negative (precision 0.421, recall 0.889, F1 0.571). These are threshold-selection figures from the same validation set, not an unbiased performance estimate. Small sample size and alert-threshold instability remain limitations. The threshold, model settings, target, and split are review-needed working choices.

The project requirement is `pyspark[ml]==4.3.0`; the ML optional extra installs NumPy for the DataFrame-based Spark ML API. It is installed in the local D-drive project environment, while teammates should install the pinned project requirements into their own chosen environment. Reuse the same estimator family for M1–M4 so the ablation measures added signal groups. See the [Spark 4.3 MLlib guide](https://spark.apache.org/docs/4.3.0/ml-guide.html) and [PySpark installation guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).

The run produced no final-test labels or predictions. The working target, candidate split, model settings, and alert-threshold procedure are recorded with **review needed** status; the source and feature contracts are in `configs/week4.local.json`, `src/foodshield/features/week4.py`, and `src/foodshield/models/m0.py`.

### Reusing the framework for another country or commodity

The experiment runner takes the selected price file, Week 2 country/commodity source configuration, annual-signal availability lag, eligibility and label files, model settings, target ID, and chronological split from configuration. Price currency, unit, source, element code, and deflator metadata are configured too. This makes the analysis pipeline reusable for one selected country–commodity series at a time. It does **not** mean changing a country or commodity code alone is sufficient: the replacement needs matching, quality-profiled producer-price and CPI-adjusted monthly data, compatible weather/production/trade inputs and identifiers, an eligibility table and labels, and a justified time split. The current config and evidence remain Ukraine wheat; the project’s approved wheat analysis scope is unchanged.

Project-wide candidate feature groups and the data model are documented in [`ARCHITECTURE.md`](ARCHITECTURE.md). The selected price source and deflator are FAOSTAT producer prices and SSSU CPI. The current target, horizon, alert-threshold procedure, and date split are working choices marked review needed. Do not claim final-test performance until the review is complete and the held-out period is evaluated once.

## Current implementation status and next item

The selected price concept and shared source are FAOSTAT farm-gate producer price with SSSU CPI adjustment. The project lead chose the 3-month/10% real-price-rise outcome as the current working default, with review needed. The candidate train/validation/test calendar is also review needed. Current implementation uses train/validation labels only; final-test labels remain unaccessed. Next: review the candidate target, split, M0 score and validation calibration; then carry the same target and date contract into M1–M4 ablation work. Any review revisions must be recorded before final-test evaluation.

Warning lead time is the interval between the first valid model warning and the later onset of price stress. Processing/alert latency is elapsed system runtime from input to output; that is a separate streaming/scalability measure, not the historical warning-lead-time objective shown in the proposal slide.

The separate course scalability requirement can proceed in parallel under [`SCALABILITY_EXPERIMENT.md`](SCALABILITY_EXPERIMENT.md); it does not require a final model outcome.
