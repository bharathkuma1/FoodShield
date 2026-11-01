# Synthetic development data

## Purpose and boundary

FoodShield can develop and integrate Week 3 monthly features without waiting for outstanding source, rights, or coverage decisions. The generator creates a small Ukraine wheat panel calibrated from distribution summaries of the local FPMA, NASA POWER, FAOSTAT QCL, and FAOSTAT TCL files. It generates new values; it does not copy rows from those sources.

Every generated row has `is_synthetic=true`; output metadata also names the local calibration inputs, seed, ranges, lags, splits, and synthetic scenario. The two named Week 3 fixture files under `data/synthetic/week3_ukraine/` are versioned for team development. Other synthetic outputs remain ignored. All synthetic data stays separate from `data/raw/` and real Week 2 outputs. Synthetic rows, event IDs, split labels, and price-stress labels are for pipeline/model-interface development only. They are not observed data, real event reconstructions, model results, or final evaluation evidence. Do not combine them with real observations in results or describe their synthetic scenarios as historical events.

## Generate the Week 3 fixture

From the repository root in PowerShell:

```powershell
python scripts\generate_synthetic_week3_fixture.py
```

The script uses only Python's standard library and the source files already in `data/raw/`. It writes:

- `data/synthetic/week3_ukraine/ukraine_wheat_monthly_synthetic.csv`
- `data/synthetic/week3_ukraine/ukraine_wheat_monthly_synthetic.metadata.json`

The fixture has 96 monthly rows (2015-01 through 2022-12), source-compatible wheat price columns, point-weather monthly columns, and annual production/trade features. The annual features use `feature_year = forecast_year - 2` and become available on January 1 of the forecast year, matching the project's provisional lag assumption. The lag is not verified historical release-vintage evidence.

`demo_split` is a code-development split: 2015 warm-up, 2016–2020 training (five years), 2021 validation, 2022 test. Three marked `SIM-*` scenarios create synthetic weather anomalies followed by synthetic price shocks in those splits. The `synthetic_demo_future_price_stress` column uses a demo-only rule: a 15% real-price increase within the next three months; it is not the team-approved target. The final three labels are blank because the full future horizon is not present.

## Safe use

- Use the fixture to exercise Spark joins, schema handling, temporal cutoffs, M0–M4 feature code, and model/report interfaces.
- Keep `is_synthetic`, `demo_split`, `synthetic_event_id`, and `synthetic_demo_future_price_stress` out of predictor columns. Split and scenario identifiers are provenance/debug fields, not model inputs.
- Do not report accuracy, lead time, recall, false alarms, data coverage, or model performance from this fixture as FoodShield results.
- Before final historical evaluation, use verified observations, an approved target definition, documented release timing, source reuse clearance, and coverage-based held-out periods. Synthetic data can be regenerated or replaced without changing those final contracts.

The generation recipe and seed are in `configs/synthetic_week3.local.json`; implementation is in `scripts/generate_synthetic_week3_fixture.py`.
