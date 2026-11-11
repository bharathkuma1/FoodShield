# Week 7: data quality, joins and as-of checks

**Branch:** `feature/workinprogress_v2` · **Run:** Week 7
**Scripts:**

- `scripts/run_week7_data_quality.py` → `data/processed/week7_data_quality/report.json` and `feature_completeness_by_year.csv`;
- tests in `tests/` (`python -m unittest discover -s tests -v`).

This covers the Week 7 data-quality items: source coverage, feature completeness, join completeness, temporal (as-of) checks, and the support behind the M0–M4 comparison.

## 1. Source coverage

| Input | Coverage | Missing values | Quality flags |
|---|---|---:|---|
| Real farm-gate wheat price (FAOSTAT PP ÷ SSSU CPI) | 2010-01 to 2023-01, 157 months, no duplicates | 0 | All 157 producer prices flag `A` (official); CPI release 14.10.2026 |
| Wheat-area-weighted weather (24 oblast points, D5) | 2001-01 to 2022-12, 264 months | 0 | Full area weight covered in every month |
| Gridded daily weather (D9-B, 232 cells) | 2001-01-01 to 2022-12-31, 1,864,120 rows | 0 | Every request URL and raw SHA-256 in the manifest |
| FAOSTAT production (QCL), Ukraine wheat | 1992–2024, 99 rows (3 measures) | 0 | All flag `A` since 2005 |
| FAOSTAT trade (TCL), Ukraine wheat | 1992–2024, 132 rows (4 measures) | 0 | All flag `A` since 2005 |
| SSSU wheat harvested area by region | 2015–2021, 175 rows, 25 regions | 0 | No observation flags; weights use 2015–2020 (D5) |

The full FAOSTAT archives reproduce the Ukraine extracts value for value (`D9_SCALE.md`), so the extracts are a faithful slice of the official release.

## 2. Feature completeness and joins (2010–2022, 156 months)

The panel joins the monthly price (the driving table) with the monthly weather and the annual values available at each month. Every price month from 2010 to 2022 finds its weather month and its annual data year; no join leaves a gap.

| Feature group | Months missing | Why |
|---|---:|---|
| Price change month on month (M0) | 1 (2010-01) | No earlier month in the series |
| Price 12-month anomaly (M0) and the M4 interaction | 12 (all of 2010) | Needs 12 earlier months |
| Weather climatology anomalies (M1) | 0 | Weather from 2001 gives at least 5 prior years from 2006 on |
| Production and trade year-on-year changes (M2, M3) | 0 | Data years 1992 on; the 2-year lag (D6) always has two years available |

So every stage is complete in every month from 2011-01 (`feature_completeness_by_year.csv`). The common origin set is 117 months (2011-01 to 2020-09): every M0–M4 feature is present and the 3-month outcome ends by 2020-12. This matches Week 5 exactly.

## 3. Temporal (as-of) checks

| Check | How | Result |
|---|---|---|
| No feature uses later data | Replay all 1,864,486 records in availability order. For each of 156 months, compare the features computed when the month closes with the features recomputed after everything has arrived (`tests/test_integration.py`). | **Identical**, exactly, in every month and feature |
| Streaming features = batch features | All 13 corrected features from the streaming monitor against the Spark batch pipeline the frozen models were trained on, every month | **Identical** (tolerance 1×10⁻⁹) |
| Annual availability lag (D6) | Every annual value becomes available on 1 January two years after its data year | Holds for all 210 annual records |
| Labels stay before the test | Backtest labels need the 3-month outcome to end by 2020-12; the last labelled origin is 2020-09 | Holds |
| Frozen protocol unchanged, test opened once | SHA-256 of `configs/week5_frozen_protocol.json` starts `c6fc95aa`; `OPENED.json` present | Holds (`tests/test_core.py`) |

**Unit tests (16, all passing, under 1 second):**

- labels, including the exact-threshold and missing-future cases and the holdout guard;
- price features;
- confusion counts and the F1 cutoff;
- the streaming monitor: month closing, duplicate-day protection, weight renormalisation, climatology window, bounded year-on-year change, annual lag, scoring, state round-trip;
- replay availability rules.

## 4. Support for the M0–M4 comparison

| Split | Months | Months with stress ahead |
|---|---:|---:|
| Train 2011–2017 (purged) | 81 | 23 |
| Validation 2018–2020 (purged) | 33 | 10 |
| Backtest 2014–2020 | 81 | 24 |
| Test 2021–2022 (opened once) | 22 | 5 |

Stress months ahead by year (common origins): 2011 3 · 2012 3 · 2013 3 · 2014 6 · 2015 5 · 2016 3 · 2017 0 · 2018 2 · 2019 2 · 2020 6.

Every stage is scored on exactly the same months, so differences between stages come from the added signals alone. The support is thin: 8 backtest episodes and 3 test episodes (`WEEK7_MODEL_EVALUATION.md`).

## 5. Known data limitations

- **Annual release timing.** FAO release dates are known only for recent data years, so D6 is an approximation; the 3-year sensitivity run covers it.
- **Revisions.** FAOSTAT and SSSU revise past values, and the archives are updated in place. Recorded SHA-256 hashes identify the exact release used.
- **Fixed area weights.** Regional wheat area is only available from 2015, so weights are held at 2015–2020 shares. Within-oblast cropland is not mapped, although the 232-cell grid agrees closely with the 24 points.
- **2022.** Official statistics exclude occupied territories from March 2022; NASA POWER weather is unaffected.
- **Same source for features and outcome.** The outcome comes from the same price series as the M0 features, strictly after each origin.
