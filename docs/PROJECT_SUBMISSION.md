# FoodShield: A Scalable Multi-Signal Early-Warning Platform for Future Wheat-Price Stress

**Authors:** Bharath Kumar; Abhijith C R; Jyoti Prakash Das; Naman Singh Shekhawat
**Course:** DA231o Data Engineering at Scale, Data-Flux
**Document type:** Continuing project manuscript and reproducibility guide
**Version:** 0.2 (evidence through Week 8; branch `feature/workinprogress_v2`)
**Note (Week 8):** the submission text is now the final report draft, a shared Claude doc that condenses this manuscript to the 6–8 page format (`WEEK8_REPORT_AND_SLIDES_PLAN.md`). This file stays as the detailed running manuscript.

## Abstract

FoodShield investigates whether multiple time-varying indicators can provide earlier or more reliable warning of future domestic wheat-price stress than a price-only monitor. The study is framed as historical analysis at a country–commodity–month grain. Its analytical progression compares a price-only baseline (M0) with models that add weather (M1), production (M2), trade (M3), and temporal or cross-signal features (M4), using the same eligible prediction months and held-out labels for every stage. The project does not seek to predict geopolitical events or physical shortages, or establish that any one signal causes a market response. It also includes a local replay demonstration in which historical or processed records are sent through Kafka and consumed with Spark Structured Streaming.

The feasibility-supported pilot is Ukraine wheat, with India retained as a comparator. The selected public-source families are FAOSTAT wheat producer prices, production and trade; SSSU consumer-price indices; and NASA POWER meteorology. The analytical workflow preserves source meaning and provenance while organizing observations into typed layers and a monthly wheat panel. The selected evaluation target type is future Ukraine wheat farm-gate producer-price stress, measured after each prediction cutoff from the same FAOSTAT series used by M0; it is a market-price outcome, not physical-shortage prediction, and it is temporally separated rather than source-independent. The SSSU all-items CPI series is chained for real-price analysis with its source attribution retained. A month is labelled stress if the real farm-gate price rises at least 10% within the next three months. Every stage uses the same Spark MLlib logistic regression, trained on 2011–2017 and calibrated on 2018–2020. Evidence comes from two places: a 2014–2020 rolling-origin backtest (81 months, 8 stress episodes) and a single pre-registered opening of the 2021–2022 test (22 months, 3 episodes).

The results disagree:

- **Backtest:** adding weather, production and trade made warnings worse than price-only monitoring.
- **Test:** the full model (M4) beat price-only monitoring beyond noise (F1 0.62 vs 0.43), mainly by raising fewer false alarms.

With 11 episodes in total, the evidence is too thin to decide between the two readings. On the engineering side, the full FAOSTAT archives (22.7 million rows, 3.2 GB) and 232-cell gridded weather (1.86 million daily rows) were processed in Spark with measured runtimes.

**Keywords:** future wheat-price stress; early warning; food prices; temporal data integration; Apache Spark; Kafka; data engineering

## 1. Introduction

Disruptions to wheat supply can affect prices, availability, and trade through interacting agricultural, weather, and market conditions. A price series is observable and timely, but may register stress only after market effects emerge. This project asks whether adding independent supply-related signals can improve the timing and quality of early warning relative to price monitoring alone.

The study focuses on wheat as required by the project proposal. Ukraine is the working pilot geography because the public monthly farm-gate price, production, trade, and weather inputs support a multi-signal historical study. The selected price series begins after the 2007–08 context and provides limited pre-event history for the 2010/11 context; 2021–2022, including the 2022 period, is the held-out test window. India is retained as a comparator, but its available weather coverage is currently too short for the same multi-year design. These choices support the current feasibility profile and do not establish that results generalize to other countries.

FoodShield is a research and educational prototype. It evaluates historical observations and demonstrates replay-based streaming. It is not a production warning service, does not provide a live feed, and does not predict conflict or its causes.

## 2. Research question and objectives

### 2.1 Research question

Can multi-source temporal signals warn of future Ukraine wheat farm-gate producer-price stress earlier or more reliably than price-only monitoring, while preserving valid information timing and making data coverage and uncertainty visible?

### 2.2 Objectives

1. Integrate wheat price, weather, production, and trade records at a country–wheat–month analytical grain where source coverage permits.
2. Compare a price-only baseline with progressively richer signal sets using common evaluation periods.
3. Prevent temporal leakage by distinguishing an observation's reference period from the date it could reasonably have been available.
4. Evaluate future Ukraine wheat farm-gate producer-price stress after each prediction cutoff, use external event evidence to contextualize candidate periods, and avoid treating broad global event descriptions as automatic country-level labels.
5. Demonstrate a bounded local historical replay through Kafka and Spark Structured Streaming, with measured throughput, latency, and record accounting.
6. Report source limitations, missingness, coverage, and measured outcomes so that results can be reproduced and interpreted within their scope.

## 3. Scope and analytical definitions

The study's commodity is **wheat only**. Country and source identifiers are configurable in the processing code to support explicitly profiled comparisons, but this configurability does not broaden the present research to rice or other commodities. The main analysis grain is country × commodity × calendar month. Each monthly date is represented by the first day of that month.

The model stages are defined by signal content:

| Stage | Signals | Analytical role |
|---|---|---|
| M0 | Wheat price | Baseline monitor. |
| M1 | Price + weather | Tests whether weather adds useful context to price. |
| M2 | Price + weather + production | Adds annual production measures, subject to availability timing. |
| M3 | Price + weather + production + trade | Adds annual reporter-level wheat trade measures. |
| M4 | All feasible signals plus temporal lags and cross-signal interactions | Tests temporal and combined relationships without using future information. |

The three historical cases retained from the proposal are **event-study contexts**, not approved binary labels: January 2007–December 2008, July 2010–June 2011, and March–December 2022. FAO publications describe substantial international wheat or cereal-market stress during these periods. For 2022, February is retained as disruption context and March as the documented international price-spike marker. These global contexts do not by themselves prove stress in each country's domestic wheat market. [FAO's 2007 Food Outlook](https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf), [FAO's 2010 Crop Prospects report](https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/), and [FAO's March 2022 report](https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en) provide the independent historical context.

## 4. Data and source characterization

The current source set follows the proposal's public-data direction. Raw source responses are retained separately from derived tables. Data-source retrieval URLs, filters, local relative paths, and provenance qualifications are maintained in [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md).

| Signal | Source and current pilot representation | Current evidence and constraints |
|---|---|---|
| Wheat price and primary outcome | FAOSTAT Ukraine Wheat Producer Price, monthly `Producer Price (LCU/tonne)` series; farm-gate producer level. | The FAOSTAT PP catalog states CC BY 4.0. The shared extract preserves official-value flags and source fields. Cite FAO and retain the distinction between producer and wholesale prices. Exact retrieval URL and filter are in [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md); the [official normalized PP archive](https://bulks-faostat.fao.org/production/Prices_E_All_Data_(Normalized).zip) and [catalog record](https://data.fao.org/catalog/dataset/47c17894-8ca1-4bd0-ba4d-6078e919e9b1) are public. |
| Real-price deflator | State Statistics Service of Ukraine (SSSU), all-items monthly CPI change, chained to an explicit index base for real-price analysis. | The API exposes source values and metadata in CSV. Cite SSSU; its metadata describes territorial coverage caveats. The [SSSU SDMX API documentation](https://stat.gov.ua/en/development-api/sdmx-api-v3) and exact request are recorded in the source register. |
| FPMA comparator | FAO GIEWS Food Price Monitoring and Analysis (FPMA), Ukraine domestic wheat-grain price series. | Retained locally as a potential descriptive comparator only. Its metadata has a market-type and price-type discrepancy and identifies APK-Inform as source partner; FAO inclusion alone does not establish reuse rights. Do not redistribute its raw values or value-revealing outputs without permission. See the [FPMA partner profile](https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/). |
| Production | FAOSTAT Crops and Livestock Products (QCL), Ukraine wheat production, yield, and harvested-area measures where present. | Units, flags, and notes are retained. FAOSTAT reference years do not provide historical publication vintages. The provisional availability rule is a modeling proxy, not a verified release date. FAOSTAT offers official [data and bulk downloads](https://www.fao.org/faostat/). |
| Trade | FAOSTAT Crops and Livestock Products Trade (TCL), Ukraine reporter-level wheat import/export quantity and value measures. | Units, flags, and notes are retained. The selected reporter-level extract supports aggregate measures; historical archive rows do not establish when each observation became public. |
| Weather | NASA POWER daily agroclimatology measures for corrected precipitation and maximum/minimum temperature. One grid cell is taken per wheat oblast (24, from geoBoundaries centroids), summarized monthly, and weighted by SSSU oblast wheat harvested area. | Area weights are fixed at 2015–2020 shares; earlier shares are unavailable. A check over all 232 grid cells in the 24 oblasts gives almost the same national series (correlation ≥ 0.9986 after removing the season). NASA describes the [Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/) as returning daily time series at the source spatial resolution. |

The selected FAOSTAT price observations and SSSU CPI are shared with attribution and source metadata. The local FPMA comparator remains controlled research data until reuse terms are confirmed. Missing records remain missing unless the source supports a defensible imputation rule; they are not silently converted to zero.

## 5. Methodology

### 5.1 Data preparation and layered representation

Source-specific records are parsed into Bronze tables while retaining source identifiers, units, notes, flags, original time fields, and source-file keys where available. Silver tables make dates and numeric values explicit and normalize fields needed for joins while retaining source-specific meaning. Gold tables represent research-ready analytical products. These are data-processing layers; each contains wheat data, and the layer names do not represent additional commodities.

The Gold baseline table is `m0_monthly_price`, keyed at country × wheat × month. For this pilot it uses the FAOSTAT monthly farm-gate producer price and its explicitly CPI-deflated counterpart, not an average recomputed from weekly market observations. The country × wheat × month feature panel joins this price baseline to monthly weather and the latest production and trade records whose availability dates are no later than the panel month. Missing calendar months are represented as null-valued rows; missing observations are not converted to zero. The panel retains source price basis, units, weather coordinates/spatial basis, annual reference years, availability dates, flags, and notes.

The processing uses Python and PySpark DataFrames/Spark SQL. Parquet stores the derived tables. Data-quality summaries describe schemas, coverage, and known limitations. The pipeline checks declared key completeness and uniqueness at each analytical grain. It does not infer absent months as zero.

### 5.2 Temporal availability and leakage control

Monthly prices retain the source observation date and frequency. Weather observations retain daily dates and are summarized to month with the spatial basis and valid-day coverage visible. For annual FAOSTAT production and trade measures, data year `Y` becomes available on 1 January of `Y + 2`. This is an assumption used to prevent same-year leakage, not a verified historical publication date. FAO release notes found for recent data years fit the rule: data years 2022 and 2023 were released in late December of `Y + 1`. Data year 2020 may have been released in April 2022, which would make the rule up to 3 months early for January–March 2022. Earlier years have no release dates. So every M2–M4 result is repeated with a three-year lag.

Feature joins admit annual signals only when `feature_available_from` is no later than the modeled month, selecting the latest eligible annual record separately for production and trade. Reference year and availability date remain separate fields. The availability convention is a provisional proxy, not a verified historical publication vintage; conclusions that depend on exact historical information availability require sensitivity analysis or vintage evidence.

Price features include configured calendar-month lags, trailing means and dispersion measures, month-over-month change, and a standardized anomaly based only on prior observations. The real-price measure is derived from FAOSTAT producer price and the SSSU CPI change series, chained with an explicit base month; nominal and real values remain separate. Other signal families use corresponding lagged and trailing summaries. A trailing window excludes the current row and remains null until its required history is available; a standardized anomaly also requires a positive standard deviation. Two Week 3 definitions were found not to measure what their names say, and were replaced before the test (decision D10):

| Signal | Week 3 problem | Corrected feature |
|---|---|---|
| Weather | The 12-month anomaly mostly measured the season: calendar month explained 95–96% of its variance for temperature. | Each month is compared with the same calendar month in the previous 10 years (at least 5). |
| Annual production and trade | The anomaly of an annual value repeated monthly kept only the sign of the year-on-year change. | Bounded year-on-year change of the latest available year, `(v_Y − v_Y−1) / ((\|v_Y\| + \|v_Y−1\|)/2)`. |

The final M0–M4 feature sets are:

- **M0:** month-over-month real-price change and the past-only 12-month price anomaly;
- **M1:** adds three weather anomalies;
- **M2:** adds three production changes;
- **M3:** adds four trade changes;
- **M4:** adds a price × precipitation-anomaly interaction.

The Week 3 versions are reported as a robustness variant. These features use no future rows, but current-month observations still require the source's release timing to support operational as-of claims.

Coverage assessment identifies the shorter producer-price history, gaps in point-weather coverage, and progressively fewer usable feature rows as additional signals and lookback requirements are applied. These are data-quality characteristics, not model results. FPMA-derived values remain local pending confirmation of third-party reuse rights and are not needed for the selected target.

### 5.3 Weather representation

NASA POWER daily corrected precipitation and maximum/minimum temperature are requested at one grid cell per wheat oblast (24 oblasts with wheat harvested area). The cells are summarized monthly, then averaged with weights equal to each oblast's share of national wheat harvested area (SSSU, mean 2015–2020). Shares for 2001–2010 are unavailable, so the weights are fixed. The single central point used in Weeks 2–3 is kept only as a sensitivity check.

To test the one-cell-per-oblast shortcut, the same series was rebuilt from all 232 native grid cells inside the 24 oblasts. The two national series agree closely:

- correlation 0.9986 for precipitation and 0.9998 for temperature, after removing calendar-month means;
- mean absolute difference 0.8 mm per month and about 0.15 °C.

Occupied territories drop out of SSSU-based coverage from March 2022.

### 5.4 Historical evaluation and target validity

The proposal's historical event periods provide external global event context; they are not country-level labels by themselves. The selected target type is future Ukraine farm-gate producer-price stress. At prediction month `t`, every model stage uses only features available by `t`; the stress label is calculated from price observations strictly after `t`, using a predeclared horizon and rule. The label uses the same FAOSTAT price source as M0 at a later time: it is temporally separated, not source-independent. M0–M4 use the same eligible origins, chronological partitions, and labels. Training fits the models and validation selects the alert threshold. Detection lead time measures how early a valid warning occurs before future price stress; processing/alert latency measures system runtime and is a separate streaming metric.

The confirmed protocol (decisions D1–D11, `DECISIONS.md`):

- **Label:** origin month `t` is positive if the maximum CPI-adjusted price in `t+1..t+3` is at least 10% above the price at `t`. Labels at 5% and 15%, and horizons of 1 and 6 months, are sensitivity checks.
- **Split:** train 2011–2017, validate 2018–2020, test 2021–2022. Origins whose 3-month outcome window crosses a boundary are purged.
- **Estimator:** one Spark MLlib binomial logistic regression for every stage (L2 0.1, standardized features). The alert cutoff is the best-F1 cutoff on validation. Stages differ only in their features.
- **Second evaluation:** a rolling-origin backtest over 2014–2020 with an expanding training window, using the same estimator and cutoff rule.
- **Reporting:**
  - precision, recall, F1, false-alarm rate, threshold-free AUC, and episodes caught with lead time;
  - always-alert and momentum baselines;
  - 90% bootstrap intervals for each stage's F1 difference from M0.
- **Pre-registered robustness:** the Week 3 feature variant, calendar season added to every stage, and the three-year annual lag.
- **One-time test:** the protocol and feature lists were frozen in `configs/week5_frozen_protocol.json` and committed before the test. The test was then opened once by a guarded script that refuses to run twice.

### 5.5 Streaming demonstration

The required streaming component is a local replay of processed historical records: a replay producer publishes records to Kafka; Spark Structured Streaming consumes them, applies a defined stress calculation, and writes traceable alerts. This is an engineering demonstration, not live monitoring. The event and alert schemas, replay ordering, pacing, checkpoint behavior, duplicate handling, and accounting must be documented before measurement. Kafka delivery semantics and Spark recovery behavior must not be described as exactly-once without evidence. The Spark [Structured Streaming and Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) documents the connector behavior and constraints.

## 6. System architecture

The batch research path is:

```text
FAOSTAT prices + SSSU CPI ─┐
FAOSTAT production/trade ──┼─> source-preserving records -> typed/normalized tables -> monthly wheat features
NASA POWER ────────────────┘                                                   -> M0–M4 -> historical evaluation
```

The replay demonstration is separate:

```text
processed historical observations -> local Kafka topic -> Spark Structured Streaming -> traceable alert output
```

Apache Spark is the primary batch and streaming compute engine. Kafka provides the local replay transport. Parquet is used for derived analytical tables. The current maintainer has developed the pilot on a Windows 11 machine with 8 GB RAM; this describes one local development environment, not a requirement for every teammate or the final project. Tune each run to its host and record hardware/software details for benchmark comparisons.

## 7. Current results and evidence

Source integration preserves source meaning and metadata while producing typed analytical layers, a monthly price baseline, and a joined wheat feature panel. With the corrected features, 117 monthly origins have every M0–M4 input and a complete 3-month outcome.

### 7.1 Rolling-origin backtest, 2014–2020

81 out-of-sample months, 24 with stress ahead, 8 episodes. Corrected features, 2-year annual lag.

| Stage | F1 (90% interval) | F1 − M0 (90% interval) | False-alarm rate | AUC | Episodes caught |
|---|---|---|---:|---:|---:|
| M0 price | 0.57 (0.44–0.68) | — | 0.46 | 0.77 | 6/8 |
| M1 + weather | 0.43 (0.28–0.57) | −0.27 to −0.02 | 0.28 | 0.61 | 5/8 |
| M2 + production | 0.42 (0.28–0.55) | −0.26 to −0.05 | 0.37 | 0.62 | 4/8 |
| M3 + trade | 0.43 (0.28–0.56) | −0.26 to −0.04 | 0.35 | 0.62 | 4/8 |
| M4 + interaction | 0.44 (0.29–0.57) | −0.25 to −0.02 | 0.32 | 0.61 | 4/8 |
| Price + season (control) | 0.50 (0.33–0.64) | −0.22 to +0.07 | 0.21 | 0.91 | 6/8 |

On 2014–2020, every added signal group made F1 worse than M0 beyond noise. Calendar season was the strongest single addition: it gave the best ranking (AUC 0.91) and halved false alarms. With the Week 3 weather feature, M1's apparent AUC of 0.88 came from the season.

### 7.2 Real test, 2021–2022 (opened once)

22 months (January 2021–October 2022), 5 with stress ahead, 3 episodes.

| Stage | Alerts | F1 | F1 − M0 (90% interval) | False-alarm rate | AUC | Episodes caught |
|---|---:|---:|---|---:|---:|---:|
| M0 price | 18 | 0.43 | — | 0.76 | 0.75 | 3/3 |
| M1–M3 | 12 | 0.47 | about −0.14 to +0.19 | 0.47 | 0.79–0.85 | 3/3 |
| M4 + interaction | 11 | 0.62 | +0.08 to +0.31 | 0.35 | 0.88 | 3/3 |
| Price + season (control) | 0 | 0.00 | −0.62 to −0.19 | 0.00 | 0.53 | 0/3 |

All stages caught all three episodes, so the gain came from fewer false alarms. M4 was the only stage that beat M0 beyond noise. Its advantage depends on the annual lag: with three years, M3 was best and M4 fell to F1 0.46. The seasonal pattern learned before 2018 broke in the war period.

The 2022 stress months are a rebound: after the February 2022 invasion the real farm-gate price fell 43% before recovering.

### 7.3 Reading the two results together

The backtest has about four times more data than the test and points the other way. The defensible conclusion for this one-country pilot is that multi-signal models did not consistently beat price-only monitoring: they did worse on 2014–2020 and better on 2021–2022. Eleven episodes are too few to decide between those readings.

Three further checks use only the backtest (`WEEK7_MODEL_EVALUATION.md`):

- **Label choice.** The backtest finding holds for every label in D1/D2 (5% and 15% rises; 1- and 6-month horizons). No multi-signal stage beat M0 beyond noise under any of them.
- **Probabilities.** The M0–M4 probabilities scored about the same as always predicting the 30% base rate (Brier 0.204–0.210 vs 0.209); only price + season improved on it (0.183).
- **Cost of recall.** Catching 80% of stress months costs about two false alarms per correct alert.

### 7.4 Data-engineering measurements

D9 runs were single runs; the Week 7 batch series repeated each configuration three times after a warm-up (`WEEK7_SCALABILITY.md`). Both used a 2-core, 8 GB Linux workspace with Spark 4.3.0 in local mode.

- **Workload size (Week 7).** The fixed weather-aggregation pipeline, on labelled synthetic expansions of the real 1.86 million daily rows:
  - 1.9 million rows in 2.3 s, 18.6 million in 5.7 s, 55.9 million in 14.0 s, 186 million in 43.2 s;
  - throughput levels off at about 4.3 million rows per second, and runtime grows almost in proportion to data from 30× on.
- **One factor at a time at 30× (Week 7).**
  - Two cores were 1.58× faster than one (79% parallel efficiency).
  - Spark's default 200 shuffle partitions ran 2.2× slower than the best setting (4).
  - Driver memory had no effect, because nothing spilled to disk.

- **FAOSTAT archives (production, trade, prices):**
  - 22.7 million rows from 3.2 GB of CSV, written as 248 MB of year-partitioned Parquet in 2 min 40 s;
  - the same query ran 2.6–14× faster on Parquet than on CSV in the Week 7 repeats, which include opening the dataset (the single D9 run timed the query alone at 7–44×);
  - Ukraine wheat filtered from the global data matched the model extracts exactly.
- **Gridded weather:** 1,864,120 daily rows aggregated to a wheat-weighted national monthly series in 11 s.
- **Multi-country feasibility:** FAOSTAT has at least 120 of 156 monthly wheat producer prices for 2010–2022 for 31 countries, enough for a multi-country panel on the price side.

### 7.5 Streaming replay (Week 6)

The full history was replayed into Kafka 4.4.0 (one KRaft broker) and consumed by Spark Structured Streaming: 1,864,486 records (daily grid weather, monthly price, annual values) in availability order. Spark de-duplicated the records, built the monthly features and scored the frozen M0 and M4 models.

In development runs on the same 2-core workspace:

- every unique record was processed exactly once, including after 1% re-sent records and a hard consumer restart;
- the streamed alerts equalled an offline replay of the same records, and the streamed M0 scores equalled the frozen test scores;
- the pipeline reached about 5,000 events/s at full speed;
- the 100 and 250 events/s targets were held, with an alert p95 latency of 1.7–1.9 s against the 10 s target;
- in the Week 7 series Spark kept up with every offered rate up to 4,000 events/s (p95 at or below 2.7 s). Latency was set mainly by the trigger interval, and the pure-Python producer, not Spark, was the limit on this 2-core machine.

The reported laptop runs (Windows 11, 16 cores) passed every check: 6,610 events/s at full speed, and both targets met with alert p95 of 2.9 s at 100/s and 3.1 s at 250/s (`WEEK6_LAPTOP_RESULTS.md`).

## 8. Evaluation plan and reporting criteria

### 8.1 Analytical evaluation

The rule, horizon and split are now fixed (Section 5.4), and Section 7 reports the measures they call for, for every stage on the same origins: warning lead time, precision, recall, F1, false-alarm rate and AUC, each with a bootstrap interval. The test is opened, so any further model change is reported as post-test exploration. Further analysis should go into more events, not more tuning. A multi-country panel (31 feasible countries) is planned as extension work after this project. Separate global event context from the observed future price outcome and avoid causal interpretation.

### 8.2 Data and engineering evaluation

Report source coverage and join completeness, missingness, key uniqueness, temporal availability, transformation reproducibility, and retained source metadata. For batch processing, measure runtime, input/output size, throughput, speedup, scaling efficiency where meaningful, and relevant join/shuffle costs across documented workload sizes and Spark resource configurations. Use source inputs for analytical conclusions; if synthetic workload expansion is needed to expose scaling behavior, label it as benchmark-only and exclude it from food-market conclusions. For streaming, measure sustained and burst throughput, p50/p95 end-to-end alert latency, and valid-record accounting under a fixed replay workload. The initial proposed local replay goals are sustained throughput of at least 100 events/s, burst throughput of at least 250 events/s, p95 alert latency no greater than 10 seconds, and accounting for all valid input records. These are project targets—not TA requirements or measured results—and remain subject to confirmation after a baseline run. Batch scale results will characterize measured trends; no numeric batch pass threshold is set. Report observed values with environment and method rather than implying target attainment.

## 9. Software prerequisites and reproducible use

### 9.1 Prerequisites

The batch and feature workflows require:

- Python 3.11 or later;
- Java 17 or later;
- PySpark 4.3.0, pinned in `requirements.txt`;
- on Windows only, Hadoop native files compatible with the PySpark-bundled Hadoop 3.5.0 client (`winutils.exe` and `hadoop.dll`) and `HADOOP_HOME` pointing to that Hadoop home;
- local source inputs listed in the run configuration. The synthetic feature configuration supports pipeline development without restricted source data.

Windows 11 is the currently documented host, not a team-wide operating-system requirement; other systems can run the same Python/Spark pipeline with OS-appropriate environment setup. No project-wide minimum RAM or drive letter has been established. The current maintainer's 8 GB host is an example only. Full setup notes and official download links are maintained in `PREREQUISITES.md`. The replay demonstration will additionally require a compatible local Apache Kafka broker and Spark's Kafka connector matching the Spark version. The Kafka runtime/version and connector invocation are not yet part of the demonstrated workflow. No cloud account or paid service is required by the project design.

### 9.2 Run the current batch pipeline

From the repository root in PowerShell, with prerequisites already available:

```powershell
$env:HADOOP_HOME = '<your Hadoop 3.5.0 Windows native home>'
python scripts\run_week2_etl.py
```

The run reads `configs/week2.local.json`, leaves `data/raw/` unchanged, and writes the pilot Parquet layers and `profile.json` beneath `data/processed/week2_ukraine/`. Generated outputs are local and Git-ignored. To repeat the analysis with changed inputs, update a copied configuration and use a distinct output directory; first profile alternate source identifiers and mappings.

To build the shared synthetic feature example, run `python scripts\run_week3_features.py`. For a local real-source feature build after Week 2 outputs exist and source-use conditions permit, run `python scripts\run_week3_features.py --config configs\week3.real.local.json`. Both workflows write feature Parquet and a coverage profile beneath the configured local output directory.

Week 4–5 evaluation and the D9 scale jobs:

```powershell
python scripts\run_week4_rolling_origin.py --config configs\week4_v2.local.json   # M0 backtest with weighted weather
python scripts\run_week5_ablation.py                                                # M0-M4 ablation, 2-year lag
python scripts\run_week5_ablation.py --annual-lag 3 --output data\processed\week5_ablation_lag3
python scripts\run_final_test_once.py --dry-run                                     # pre-test check; the real test is already opened
python scripts\run_d9a_faostat_bulk.py                                              # needs the FAOSTAT archives in data\raw\faostat_bulk
python scripts\run_d9b_grid_weather.py                                              # needs the gridded weather CSV
```

`WEEK5_ABLATION.md`, `FINAL_TEST_RESULTS.md` and `D9_SCALE.md` describe each run and its outputs.

### 9.3 Data access and provenance

Use [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md) for source URLs, API request examples, retrieval paths, filters, and known provenance gaps. Only explicitly reviewed, attributed snapshots are shared in version control; restricted or otherwise unreviewed acquisitions remain local. Do not place API keys, CAPTCHA contents, cookies, or account credentials in the repository. For future refreshes, record the exact secret-free request, retrieval date, filters, file size/checksum, and source terms in the register.

## 10. Limitations and responsible interpretation

1. **Price concept:** the selected outcome is FAOSTAT farm-gate producer-price stress, not a wholesale or retail price measure. FPMA/APK-Inform remains a local comparator because reuse terms and a Retail/Wholesale metadata discrepancy are unresolved.
2. **Weather geography:** weather is weighted by oblast wheat area with fixed 2015–2020 shares, using one grid cell per oblast. A full-grid check agrees closely, but within-oblast cropland is not mapped.
3. **Annual-data timing:** the two-year availability lag does not reconstruct what was publicly known at each historical date. A three-year sensitivity run changes which multi-signal stage is best.
4. **Outcome interpretation:** the selected target measures future Ukraine wheat farm-gate producer-price stress from the same source used in M0, at a strictly later time. It is not a source-independent label or a measure of physical shortage. International event windows are contextual evidence, not country-level labels.
5. **Coverage and comparability:** candidate countries and source series differ in start date, market level, product definition, and coverage. Cross-country conclusions require harmonized and profiled data.
6. **Model and runtime evidence:** results rest on one country, 81 backtest months and 22 test months, with 8 + 3 stress episodes. One alert more or less moves F1 by up to 0.03 on the backtest and up to 0.1 on the test. Batch timings are single runs on one machine. Scale-out behavior, streaming targets and causal relationships are not established.
7. **Scope:** findings, when available, apply to wheat and the geographies/time periods supported by validated sources. Other commodities are outside this study.

## 11. Discussion and conclusion

The project built a source-aware, layered Spark pipeline for a wheat-only historical study, and compared M0–M4 under one frozen protocol on identical months. On Ukraine, multi-signal models did not consistently beat price-only monitoring. They were worse on 2014–2020, better on the 2021–2022 test (M4 F1 0.62 vs 0.43, with half the false alarms), and too few events separate the two readings. Two methodological findings matter beyond the scores:

- naive monthly anomalies of weather mostly measure the season, and those of repeated annual values keep only the sign of change;
- calendar season alone is a strong but unstable signal.

The pipeline handles the full FAOSTAT archives and gridded weather at their real size. The local Kafka replay is built and measured separately from warning lead time. A multi-country panel, the main way to add events, is planned as extension work after this single-country project.

FAOSTAT and national statistics portals provide the underlying series and visual analysis. FoodShield's contribution is to join public source families with explicit time availability, test whether weather, production, and trade signals add warning value over a price-only farm-gate baseline, and measure a reproducible Spark/Kafka processing path. It evaluates a defined early-warning task across historical events rather than restating portal charts. This is an applied data-engineering and comparative-evaluation contribution, not a claim to originate the source data or establish causality. On the present evidence, early-warning improvement is not established for this pilot. It remains the research question.

## References

1. Food and Agriculture Organization of the United Nations. *FAOSTAT Producer Prices catalog record*. [https://data.fao.org/catalog/dataset/47c17894-8ca1-4bd0-ba4d-6078e919e9b1](https://data.fao.org/catalog/dataset/47c17894-8ca1-4bd0-ba4d-6078e919e9b1)
2. FAO. *FAOSTAT*. [https://www.fao.org/faostat/](https://www.fao.org/faostat/)
3. State Statistics Service of Ukraine. *SDMX API v3 documentation*. [https://stat.gov.ua/en/development-api/sdmx-api-v3](https://stat.gov.ua/en/development-api/sdmx-api-v3)
4. FAO. *Food Outlook, November 2007*. [https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf](https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf)
5. FAO. *Crop Prospects and Food Situation, No. 3, September 2010*. [https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/](https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/)
6. FAO. *FAO Food Price Index posts significant leap in March*. 8 April 2022. [https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en](https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en)
7. NASA Langley Research Center. *POWER Daily API*. [https://power.larc.nasa.gov/docs/services/api/temporal/daily/](https://power.larc.nasa.gov/docs/services/api/temporal/daily/)
8. Apache Software Foundation. *Structured Streaming + Kafka Integration Guide*. [https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
9. Food and Agriculture Organization of the United Nations. *Terms of Use*. [https://www.fao.org/contact-us/terms/en/](https://www.fao.org/contact-us/terms/en/)

## Ongoing manuscript refinement

This manuscript is maintained as validated data, methods, analyses, and measured system results become available. Each revision should preserve the wheat-only scope, distinguish observations from assumptions and targets, and replace provisional statements only when new evidence is recorded in the project sources and reproducibility artifacts.
