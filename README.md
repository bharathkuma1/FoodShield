# FoodShield: early warning of wheat price stress (DA 231o, Data Engineering at Scale)

FoodShield asks whether combining price, weather, production and trade signals warns of wheat price stress earlier or better than watching prices alone. The project uses Ukraine's monthly farm-gate wheat price, deflated by consumer prices, as the pilot.

- **Stress:** a month is labelled stress if the real price rises at least 10% within the next three months.
- **Models:** M0 (price only) to M4 (price, weather, production, trade and an interaction), all built with Apache Spark.
- **Streaming:** a local Kafka replay into Spark Structured Streaming shows the alerting path.

**Branch:** `feature/workinprogress_v2`. **Scope:** single country (Ukraine); a multi-country panel is planned as extension work (D13).

## Where things stand (Week 8)

| Week | Topic | Status | Read |
|---|---|---|---|
| 1–3 | Sources, Spark ETL, features | Done | `docs/WEEK1_*`, `WEEK2_ETL.md`, `WEEK3_FEATURES.md` |
| 4 | Target, split, M0 baseline, backtest | Done (D1–D9) | `docs/WEEK4_REVIEW_V2.md` |
| 5 | M1–M4 ablation, frozen protocol, one-time test | Done (D10, D11, D8) | `docs/WEEK5_ABLATION.md`, `docs/FINAL_TEST_RESULTS.md` |
| D9 | Data at real scale: full FAOSTAT archives, 232-cell gridded weather | Done | `docs/D9_SCALE.md` |
| 6 | Kafka 4.4.0 → Spark Structured Streaming replay | Done; laptop runs passed every check | `docs/WEEK6_STREAMING.md`, `docs/WEEK6_LAPTOP_RESULTS.md` |
| 7 | Evaluation and scalability | Done (development runs) | `docs/WEEK7_MODEL_EVALUATION.md`, `WEEK7_DATA_QUALITY.md`, `WEEK7_SCALABILITY.md` |
| 8 | Dashboard, report, slides, demo | Interactive dashboard with live alerts (D15); report and slide deck drafted (shared Claude doc and deck) with the laptop numbers; joint review pending | `docs/dashboard/index.html`, `docs/WEEK8_REPORT_AND_SLIDES_PLAN.md` |

**Model result:** on Ukraine, the multi-signal models did not consistently beat price-only monitoring.

- **2014–2020 backtest:** they were worse (M0 F1 0.57 vs M1–M4 0.42–0.44).
- **2021–2022 test, opened once:** they were better (M4 F1 0.62 vs M0 0.43), with half the false alarms.
- Eleven stress episodes are too few to settle it.

Decisions and their evidence are in `docs/DECISIONS.md`.

## Repository layout

| Path | Contents |
|---|---|
| `configs/` | One JSON config per run; `week5_frozen_protocol.json` is the frozen test protocol |
| `src/foodshield/` | `etl/` (Week 2 and full FAOSTAT archives), `features/`, `models/`, `evaluation/`, `streaming/` |
| `scripts/` | Entry points, one per step (listed below) |
| `tests/` | `unittest` checks: fast unit tests plus slower integration tests on real inputs |
| `data/raw/`, `data/processed/` | Inputs and outputs. Only small, documented files are in Git; large downloads stay local (`.gitignore`) |
| `docs/` | Plan, decisions, data sources, per-week write-ups, figures, report draft |

## Setup

Python 3.11 or later, Java 17 or later, then `pip install -r requirements.txt` (PySpark 4.3.0, kafka-python 3.0.13).

- **Windows:** Spark also needs the Hadoop 3.5.0 native files (`winutils.exe`, `hadoop.dll`) with `HADOOP_HOME` set; see `docs/PREREQUISITES.md`.
- **Week 6:** Kafka and the Spark–Kafka connector files go in a `Kafka/` folder; see `docs/WEEK6_STREAMING.md`.

## Runbook

All commands are run from the repository folder. Inputs that are not in Git are re-downloaded with the URLs and SHA-256 hashes recorded in `docs/DATA_SOURCE_REGISTER.md`.

```powershell
# Checks
python -m unittest discover -s tests -v                        # about 1-2 minutes; FOODSHIELD_SKIP_SLOW_TESTS=1 for the fast ones only

# Weeks 2-5: ETL, features, models
python scripts\run_week2_etl.py
python scripts\run_week3_features.py --config configs\week3.real.local.json
python scripts\run_week4_rolling_origin.py                     # M0 backtest, weighted weather
python scripts\run_week5_ablation.py                           # M0-M4 ablation (2-year lag)
python scripts\run_week5_ablation.py --annual-lag 3 --output data\processed\week5_ablation_lag3
python scripts\run_final_test_once.py --dry-run                # the real test is already opened and cannot be rerun

# Data at scale (D9)
python scripts\run_d9a_faostat_bulk.py                         # needs the archives in data\raw\faostat_bulk
python scripts\run_d9b_grid_weather.py                         # needs data\raw\nasa_power_grid\*.csv

# Week 6: streaming replay (one command: Kafka, Spark consumer, producer, checks, report)
python scripts\run_week6_replay.py --name demo --work-root C:\fs6 --replay-from 2011-01-01 --measure-from 2022-01-01 --rate 250
RUN_LAPTOP_STEPS.bat                                            # one click on Windows: setup checks, tests, laptop runs, results, demo rehearsal
powershell -ExecutionPolicy Bypass -File scripts\week6_laptop.ps1   # all four laptop runs
python scripts\apply_laptop_results.py                        # then: laptop summary, report/slide text, dashboard rebuild

# Week 7: evaluation and scalability
python scripts\run_week7_model_evaluation.py
python scripts\run_week7_data_quality.py
python scripts\run_week7_batch_scaling.py --phase all          # about 1 hour; writes plots to docs\figures
python scripts\run_week7_batch_scaling.py --phase all --output-suffix _laptop   # optional laptop repeat, kept separate
python scripts\run_week5_ablation.py --rise-pct 5 --horizon 3 --variants corrected --output data\processed\week7_label_sensitivity\rise5_h3   # D1/D2 label checks
python scripts\run_week6_replay.py --name s7_rate_2000 --replay-from 2016-01-01 --measure-from 2022-01-01 --rate 2000   # streaming series run
python scripts\plot_week7_streaming.py

# Week 8
python scripts\export_week8_dashboard_features.py              # once: monthly model inputs for the "What if?" tab (needs Spark)
python scripts\build_week8_dashboard.py                        # docs\dashboard\index.html (open in any browser, works offline)
py -3.12 scripts\serve_dashboard.py --work-root C:\fs6          # same page plus the live view of a running replay (http://127.0.0.1:8765)
RUN_DEMO_LIVE.bat                                               # one click: dashboard server in its own window, then the 3-minute demo with live alerts
```

## Data and licences

| Data | Source | Terms as recorded in the register |
|---|---|---|
| Wheat producer prices, production, trade | FAOSTAT (FAO) | CC BY 4.0 with attribution. FAO notes possible third-party exceptions, so recheck on refresh. |
| Consumer price index, oblast wheat area | State Statistics Service of Ukraine (SSSU) | Free use and redistribution of official statistics, naming SSSU (Law 2524-IX, Art. 27) |
| Daily weather | NASA POWER Daily API | Public API; acknowledge NASA POWER as the source |
| Oblast centroids | geoBoundaries gbOpen (UKR ADM1) | Open boundary data; cite geoBoundaries |

Details, retrieval dates and hashes are in `docs/DATA_SOURCE_REGISTER.md`. Synthetic data, used only for demos and scaling benchmarks, is always labelled (`docs/SYNTHETIC_DATA.md`).

## Team

Bharath Kumar, Abhijith C R, Jyoti Prakash Das, Naman Singh Shekhawat. Ownership is in `docs/TEAM_RESPONSIBILITIES.md`; since Week 6 the work is done in one shared place and reviewed jointly (D14).
