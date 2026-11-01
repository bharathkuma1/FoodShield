# Week 3 monthly feature contract

## Scope and run

The Week 3 feature job aligns an input panel at one row per country, wheat commodity code, and calendar month. It creates predictor candidates only; it does not define a stress outcome, train M0–M4 detectors, or evaluate event performance. The checked-in starter configuration uses the explicitly synthetic development panel, so its output must not be presented as observed data or final evaluation evidence.

From the repository root, after installing the documented project requirements in the chosen Python environment and configuring Windows Hadoop native files where needed:

```powershell
python scripts\run_week3_features.py
```

The synthetic output is written under ignored `data/processed/week3_ukraine_synthetic/`: Parquet at `features/` and a `profile.json` with stage completeness counts, grain, feature list, and caveats. Configuration is `configs/week3.local.json`; output replacement is limited to that generated directory. For local Week 2 Silver/Gold inputs, use `configs/week3.real.local.json`; that run writes restricted source-derived values only under ignored `data/processed/week3_ukraine_real/`.

## Monthly grain and temporal rules

- Key: `country_code`, `commodity_code`, `month` (month start). Duplicate source keys fail the run.
- Missing calendar months between each key's first and last observations are represented as rows with null source values. This makes a 1-month lag mean the prior calendar month rather than the previous available row. Coverage counts include these gaps; observed input count is also recorded.
- Price and other signal lags use values from strictly earlier months. Rolling windows are trailing and exclude the current month. The 3/6/12-month means and sample standard deviations remain null until the full respective count of prior non-null months is available.
- A 12-month anomaly candidate is `(current value - trailing 12-month mean) / trailing 12-month sample SD`; it is null until 12 prior values exist and SD is positive. Month-over-month price change is current / prior-calendar-month - 1.
- Annual production/trade inputs require `feature_available_from`; their values are nulled before that date. The configured two-year availability lag remains a provisional proxy, not verified publication-vintage evidence.
- In the real-input mode, the base is Week 2 Gold monthly price. Week 2 monthly weather is left-joined on the country/commodity/month key. Production and trade are each joined as-of: for each month, select the latest annual row whose `feature_available_from` is no later than that month. Keep separate feature year, availability date, and arrays of source element/unit/flag/note metadata for production and trade.
- M4 includes an explicit interaction between the price-real and precipitation trailing-anomaly candidates where both are present. All original source, unit, spatial-basis, availability, and synthetic provenance fields are retained by the feature build.
- Synthetic/debug/label columns are carried for provenance and must never be selected as predictors. `is_synthetic` remains available in output.

## Signal stage completeness

The profile reports two distinct coverage views at each stage: complete core signal rows (M0 price; M1 plus monthly weather; M2 plus annual production; M3 plus trade; M4 same core signals plus its feature), and feature-complete rows (price lags/rolling/anomaly for M0, then each stage's signal anomalies, with the M4 price/weather interaction). It includes per-signal and per-feature missing counts. Early months naturally lack full lookback history; missing source signals remain null, with no zero-fill or silent row removal.

Weather features inherit `spatial_basis`. The existing Ukraine point is a screening series, not a national or crop-weighted weather measure. Weather daily-to-monthly aggregation is performed in Week 2; this job consumes those monthly fields.

## Current results and remaining decisions

The feature module and bounded Spark runner support both the shared synthetic panel and local Week 2 Silver/Gold inputs. The synthetic run completed at 96 rows with 96 complete core-signal rows in each M0–M4 stage. Feature-complete counts are M0/M1 84 and M2/M3/M4 77 because of lookback requirements and annual-feature availability. A hand-reviewed sample confirms a null first-month lag, prior-month values for lags, and anomaly availability only after 12 prior months. The synthetic output is plumbing evidence only.

The local real-source run completed with 310 unique price-month rows. M0 has 309 core-complete rows because one row lacks real- and USD-price values; 297 rows have the required M0 lookback features. NASA point-weather coverage is 264/310 months (46 months missing). M1, M2, and M3 each have 264 core-complete rows; the 46 incomplete rows coincide with absent monthly weather. Feature-complete counts are M1 252, M2 231, M3 220, and M4 220, reflecting missing weather coverage and the required lookback history. The current production and trade panels have values available for all 310 price months under the provisional lag rule. A hand-reviewed as-of sample selected production/trade data year 1999 for January 2001, 2013 for January 2015, and 2020 for January 2022, each becoming available on that forecast year's January 1. A manual daily-to-monthly recomputation for January 2015 matched the recorded Week 2 weather aggregate: mean daily maximum 0.451935 C, mean daily minimum -5.371613 C, and precipitation sum 34.26 mm. The single-point weather basis remains screening-only.

Real-source Parquet and profile files are under ignored `data/processed/week3_ukraine_real/`. They were built from the earlier FPMA price input and remain local because APK-Inform reuse rights are unresolved; do not commit or redistribute those source-derived rows. The Week 3 run is historical implementation evidence, not the selected shared target pipeline. Week 4 has selected FAOSTAT farm-gate prices with an SSSU CPI chain and separately profiled its feature eligibility, but this source still needs integration into the Spark M0–M4 pipeline. The stress rule, horizon, threshold and chronological protocol remain open.
