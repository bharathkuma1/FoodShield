# FoodShield: A Scalable Multi-Signal Early-Warning Platform for Future Wheat-Price Stress

**Authors:** Bharath Kumar; Abhijith C R; Jyoti Prakash Das; Naman Singh Shekhawat
**Course:** DA231o Data Engineering at Scale, Data-Flux
**Document type:** Continuing project manuscript and reproducibility guide
**Version:** 0.1 (evidence through 1 November 2026)

## Abstract

FoodShield investigates whether multiple time-varying indicators can provide earlier or more reliable warning of future domestic wheat-price stress than a price-only monitor. The study is framed as historical analysis at a country–commodity–month grain. Its analytical progression compares a price-only baseline (M0) with models that add weather (M1), production (M2), trade (M3), and temporal or cross-signal features (M4). The project does not seek to predict geopolitical events or physical shortages, or establish that any one signal causes a market response. It also includes a local replay demonstration in which historical or processed records are sent through Kafka and consumed with Spark Structured Streaming.

The feasibility-supported pilot is Ukraine wheat, with India retained as a comparator. The selected public-source families are FAO FPMA wheat prices, FAOSTAT wheat production and trade, and NASA POWER meteorology. The analytical workflow preserves source meaning and provenance while organizing observations into typed layers and a monthly wheat panel. The selected evaluation target type is future domestic wheat-price stress, measured after each prediction cutoff; it is a market-price outcome, not physical-shortage prediction, and it uses the FPMA series at a later time rather than an independent data source. Weather currently serves as a point-based screening signal, annual-data availability timing remains provisional, and the exact target rule and forecast horizon are not yet established. No detection-skill or performance result is claimed in this manuscript.

**Keywords:** future wheat-price stress; early warning; food prices; temporal data integration; Apache Spark; Kafka; data engineering

## 1. Introduction

Disruptions to wheat supply can affect prices, availability, and trade through interacting agricultural, weather, and market conditions. A price series is observable and timely, but may register stress only after market effects emerge. This project asks whether adding independent supply-related signals can improve the timing and quality of early warning relative to price monitoring alone.

The study focuses on wheat as required by the project proposal. Ukraine is the working pilot geography because the selected monthly wheat price series spans the proposal's historical event contexts. India is retained as a comparison for the 2022 context; its selected national wheat-price series does not extend back to the earlier cases. These choices support the current feasibility profile and do not establish that results generalize to other countries.

FoodShield is a research and educational prototype. It evaluates historical observations and demonstrates replay-based streaming. It is not a production warning service, does not provide a live feed, and does not predict conflict or its causes.

## 2. Research question and objectives

### 2.1 Research question

Can multi-source temporal signals warn of future domestic wheat-price stress earlier or more reliably than price-only monitoring, while preserving valid information timing and making data coverage and uncertainty visible?

### 2.2 Objectives

1. Integrate wheat price, weather, production, and trade records at a country–wheat–month analytical grain where source coverage permits.
2. Compare a price-only baseline with progressively richer signal sets using common evaluation periods.
3. Prevent temporal leakage by distinguishing an observation's reference period from the date it could reasonably have been available.
4. Evaluate future domestic wheat-price stress after each prediction cutoff, use external event evidence to contextualize candidate periods, and avoid treating broad global event descriptions as automatic country-level labels.
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
| Wheat price | FAO GIEWS Food Price Monitoring and Analysis (FPMA), selected Ukraine domestic wheat-grain price series with provider-calculated monthly observations for M0. | Source metadata contains a market-type and price-type discrepancy that remains unresolved. The series identifies APK-Inform as a source partner; FAO inclusion alone does not establish reuse rights. Do not redistribute underlying values or expose them in public output until rights are confirmed. The FPMA tool describes its price series and associated metadata [here](https://www.fao.org/fileadmin/user_upload/foodprice/docs/others/GIEWS_Price_tool_help_en_US_01.pdf); the partner is listed in the [FAO partner profile](https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/). |
| Production | FAOSTAT Crops and Livestock Products (QCL), Ukraine wheat production, yield, and harvested-area measures where present. | Units, flags, and notes are retained. FAOSTAT reference years do not provide historical publication vintages. The provisional availability rule is a modeling proxy, not a verified release date. FAOSTAT offers official [data and bulk downloads](https://www.fao.org/faostat/). |
| Trade | FAOSTAT Crops and Livestock Products Trade (TCL), Ukraine reporter-level wheat import/export quantity and value measures. | Units, flags, and notes are retained. The selected reporter-level extract supports aggregate measures; historical archive rows do not establish when each observation became public. |
| Weather | NASA POWER daily agroclimatology measures for corrected precipitation and maximum/minimum temperature, summarized monthly for a Ukraine screening location. | This is a single-point screening sample, not a crop-weighted or nationally representative exposure measure. NASA describes the [Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/) as returning daily time-series data at the source spatial resolution; agricultural interpretation still requires defensible wheat-area geography and weighting. |

The local FPMA input should be treated as controlled research data until reuse conditions are confirmed. Source attribution and data-use terms must be reviewed before public release. Missing records remain missing unless the source supports a defensible imputation rule; they are not silently converted to zero.

## 5. Methodology

### 5.1 Data preparation and layered representation

Source-specific records are parsed into Bronze tables while retaining source identifiers, units, notes, flags, original time fields, and source-file keys where available. Silver tables make dates and numeric values explicit and normalize fields needed for joins while retaining source-specific meaning. Gold tables represent research-ready analytical products. These are data-processing layers; each contains wheat data, and the layer names do not represent additional commodities.

The Gold baseline table is `m0_monthly_price`, keyed at country × wheat × month. It takes the provider-calculated monthly series rather than recomputing monthly values from weekly observations. A country × wheat × month feature panel now joins this price baseline to monthly weather and to the latest production and trade records whose availability dates are no later than the panel month. Missing calendar months are represented as null-valued rows; missing observations are not converted to zero. The panel retains source price basis, units, weather coordinates/spatial basis, annual reference years, availability dates, flags, and notes.

The processing uses Python and PySpark DataFrames/Spark SQL. Parquet stores the derived tables. Data-quality summaries describe schemas, coverage, and known limitations. The pipeline checks declared key completeness and uniqueness at each analytical grain. It does not infer absent months as zero.

### 5.2 Temporal availability and leakage control

Monthly prices retain the source observation date and frequency. Weather observations retain daily dates and are summarized to month with the spatial basis and valid-day coverage visible. For annual FAOSTAT production and trade measures, the current transformation applies a conservative lag before setting `feature_available_from`. This is an assumption used to prevent obvious same-year leakage; it is not a verified historical publication date. Sensitivity analysis or historical vintages are needed before strong as-of claims can be made.

Feature joins admit annual signals only when `feature_available_from` is no later than the modeled month, selecting the latest eligible annual record separately for production and trade. Reference year and availability date remain separate fields. The availability convention is a provisional proxy, not a verified historical publication vintage; conclusions that depend on exact historical information availability require sensitivity analysis or vintage evidence.

Price features include configured calendar-month lags, trailing means and dispersion measures, month-over-month change, and a standardized anomaly based only on prior observations. Other signal families use corresponding lagged and trailing summaries. A trailing window excludes the current row and remains null until its required history is available; a standardized anomaly also requires a positive standard deviation. The M4 feature set includes a real-price and precipitation anomaly interaction when both inputs are available. These features use no future rows, but current-month observations still require the source's release timing to support operational as-of claims. Nominal, real, and USD price concepts remain separate measures.

Coverage assessment identifies incomplete price fields, gaps in point-weather coverage, and progressively fewer usable feature rows as additional signals and lookback requirements are applied. These are data-quality characteristics, not model results. The FPMA-derived panel remains controlled project data pending confirmation of third-party reuse rights and is not redistributed with the source repository.

### 5.3 Weather representation

NASA POWER daily corrected precipitation and maximum/minimum temperature are the current weather candidates. The present point is useful for checking API fields, dates, units, sentinels, and monthly aggregation. It cannot substantiate nationwide or wheat-area exposure. A defensible M1 analysis needs a documented set of wheat-producing regions/grid cells and validated harvested-area weights for the relevant years. Where weights or coverage are unavailable, affected model stages must be limited to a common verified period or omitted; the point sample must not be relabeled as representative.

### 5.4 Historical evaluation and target validity

The proposal's historical event periods provide external global event context; they are not country-level labels by themselves. The selected target type is future domestic wheat-price stress. At prediction month `t`, every model stage uses only features available by `t`; the stress label is calculated from price observations strictly after `t`, using a predeclared horizon and rule. The label therefore uses the same FPMA source as M0 at a later time: it is temporally separated, not source-independent. FPMA reuse rights and its metadata discrepancy remain gates for public reporting. Define the stress rule and threshold on training/validation data only, and reserve eligible later periods/events for final evaluation. Detection lead time measures how early a valid warning occurs before future price stress; processing/alert latency measures system runtime and is a separate streaming metric.

### 5.5 Streaming demonstration

The required streaming component is a local replay of processed historical records: a replay producer publishes records to Kafka; Spark Structured Streaming consumes them, applies a defined stress calculation, and writes traceable alerts. This is an engineering demonstration, not live monitoring. The event and alert schemas, replay ordering, pacing, checkpoint behavior, duplicate handling, and accounting must be documented before measurement. Kafka delivery semantics and Spark recovery behavior must not be described as exactly-once without evidence. The Spark [Structured Streaming and Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) documents the connector behavior and constraints.

## 6. System architecture

The batch research path is:

```text
FAO FPMA ─┐
FAOSTAT ──┼─> source-preserving records -> typed/normalized tables -> monthly wheat features
NASA POWER┘                                                   -> M0–M4 -> historical evaluation
```

The replay demonstration is separate:

```text
processed historical observations -> local Kafka topic -> Spark Structured Streaming -> traceable alert output
```

Apache Spark is the primary batch and streaming compute engine. Kafka provides the local replay transport. Parquet is used for derived analytical tables. The current maintainer has developed the pilot on a Windows 11 machine with 8 GB RAM; this describes one local development environment, not a requirement for every teammate or the final project. Tune each run to its host and record hardware/software details for benchmark comparisons.

## 7. Current results and evidence

Source integration preserves source meaning and metadata while producing typed analytical layers, a monthly price baseline, and a joined wheat feature panel. Coverage profiling identifies gaps and shows where adding signals or requiring feature lookback history reduces the common analysis period. The available evidence characterizes data readiness and temporal coverage; it does not establish model skill or operational warning effectiveness.

No M0–M4 detection metrics have been measured in the current project evidence. The target type is selected, but the operational stress rule, horizon, threshold-selection method, and final chronological split remain open. No crop-weighted weather signal has been produced. No Kafka/Spark replay benchmark has been measured. Therefore, no conclusion about early-warning performance, detection lead time, throughput, or processing latency is currently warranted.

## 8. Evaluation plan and reporting criteria

### 8.1 Analytical evaluation

After the future price-stress rule, horizon, and chronological split are fixed, report warning lead time, precision, recall, F1, and false-positive rate with target definition, event dates, negative periods, geographic scope, thresholds, and holdout protocol. Compare M0–M4 on the same supported months wherever possible. Report how eligible coverage changes as signals are added. Separate global event context from the observed future price outcome and avoid causal interpretation.

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

Windows 11 is the currently documented host, not a team-wide operating-system requirement; other systems can run the same Python/Spark pipeline with OS-appropriate environment setup. No project-wide minimum RAM or drive letter has been established. The current maintainer's 8 GB host and `D:\Bharath\Installations` path are examples only. Full setup notes and official download links are maintained in `PREREQUISITES.md`. The replay demonstration will additionally require a compatible local Apache Kafka broker and Spark's Kafka connector matching the Spark version. The Kafka runtime/version and connector invocation are not yet part of the demonstrated workflow. No cloud account or paid service is required by the project design.

### 9.2 Run the current batch pipeline

From the repository root in PowerShell, with prerequisites already available:

```powershell
$env:HADOOP_HOME = '<your Hadoop 3.5.0 Windows native home>'
python scripts\run_week2_etl.py
```

The run reads `configs/week2.local.json`, leaves `data/raw/` unchanged, and writes the pilot Parquet layers and `profile.json` beneath `data/processed/week2_ukraine/`. Generated outputs are local and Git-ignored. To repeat the analysis with changed inputs, update a copied configuration and use a distinct output directory; first profile alternate source identifiers and mappings.

To build the shared synthetic feature example, run `python scripts\run_week3_features.py`. For a local real-source feature build after Week 2 outputs exist and source-use conditions permit, run `python scripts\run_week3_features.py --config configs\week3.real.local.json`. Both workflows write feature Parquet and a coverage profile beneath the configured local output directory.

### 9.3 Data access and provenance

Use [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md) for source URLs, API request examples, retrieval paths, filters, and known provenance gaps. Only explicitly reviewed, attributed snapshots are shared in version control; restricted or otherwise unreviewed acquisitions remain local. Do not place API keys, CAPTCHA contents, cookies, or account credentials in the repository. For future refreshes, record the exact secret-free request, retrieval date, filters, file size/checksum, and source terms in the register.

## 10. Limitations and responsible interpretation

1. **Price rights and metadata:** the Ukraine FPMA series has unresolved source-partner reuse rights and a Retail/Wholesale metadata discrepancy. Public redistribution of raw values or value-revealing outputs is restricted pending confirmation.
2. **Weather geography:** the current NASA data are a single point. They cannot support national or crop-weighted conclusions.
3. **Annual-data timing:** the provisional availability lag does not reconstruct what was publicly known at each historical date.
4. **Outcome interpretation:** the selected target measures future domestic wheat-price stress from the same price source used in M0, at a strictly later time. It is not a source-independent label or a measure of physical shortage. International event windows are contextual evidence, not country-level labels.
5. **Coverage and comparability:** candidate countries and source series differ in start date, market level, product definition, and coverage. Cross-country conclusions require harmonized and profiled data.
6. **Model and runtime evidence:** model performance, causal relationships, scale-out behavior, and streaming targets have not been established by the current run.
7. **Scope:** findings, when available, apply to wheat and the geographies/time periods supported by validated sources. Other commodities are outside this study.

## 11. Discussion and conclusion

The project has established a source-aware, layered processing foundation for a wheat-only historical study and has materialized a Ukraine pilot price baseline with associated production, trade, and exploratory weather tables. The evidence currently supports feasibility of a bounded local ETL workflow, not the effectiveness of a multi-signal warning model. The next analytical conclusions depend on an operational future-price-stress rule, forecast horizon, representative weather construction, defensible availability timing, and evaluation on common historical periods. The local Kafka replay remains an engineering demonstration to be measured separately from historical warning lead time.

FoodShield's central contribution will be assessed by whether a carefully timed and independently evaluated multi-signal approach improves on M0, while making data limits and uncertainty explicit. Until that comparison is completed, early-warning improvement remains the research question rather than a result.

## References

1. Food and Agriculture Organization of the United Nations. *Food Price Monitoring and Analysis Tool: Help*. [https://www.fao.org/fileadmin/user_upload/foodprice/docs/others/GIEWS_Price_tool_help_en_US_01.pdf](https://www.fao.org/fileadmin/user_upload/foodprice/docs/others/GIEWS_Price_tool_help_en_US_01.pdf)
2. FAO. *FPMA Data Partner: APK-Inform*. [https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/](https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/)
3. FAO. *FAOSTAT*. [https://www.fao.org/faostat/](https://www.fao.org/faostat/)
4. FAO. *Food Outlook, November 2007*. [https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf](https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf)
5. FAO. *Crop Prospects and Food Situation, No. 3, September 2010*. [https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/](https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/)
6. FAO. *FAO Food Price Index posts significant leap in March*. 8 April 2022. [https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en](https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en)
7. NASA Langley Research Center. *POWER Daily API*. [https://power.larc.nasa.gov/docs/services/api/temporal/daily/](https://power.larc.nasa.gov/docs/services/api/temporal/daily/)
8. Apache Software Foundation. *Structured Streaming + Kafka Integration Guide*. [https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
9. Food and Agriculture Organization of the United Nations. *Terms of Use*. [https://www.fao.org/contact-us/terms/en/](https://www.fao.org/contact-us/terms/en/)

## Ongoing manuscript refinement

This manuscript is maintained as validated data, methods, analyses, and measured system results become available. Each revision should preserve the wheat-only scope, distinguish observations from assumptions and targets, and replace provisional statements only when new evidence is recorded in the project sources and reproducibility artifacts.
