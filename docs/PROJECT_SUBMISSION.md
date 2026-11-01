# FoodShield: A Scalable Multi-Signal Early-Warning Platform for Wheat Supply Stress

**Authors:** Bharath Kumar; Abhijith C R; Jyoti Prakash Das; Naman Singh Shekhawat
**Course:** DA231o Data Engineering at Scale, Data-Flux
**Document type:** Continuing project manuscript and reproducibility guide
**Version:** 0.1 (evidence through Week 4)

## Abstract

FoodShield investigates whether multiple time-varying indicators can provide earlier warning of observable wheat-supply stress than a price-only monitor. The study is framed as historical analysis at a country–commodity–month grain. Its analytical progression compares a price-only baseline (M0) with models that add weather (M1), production (M2), trade (M3), and temporal or cross-signal features (M4). The project does not seek to predict geopolitical events or establish that any one signal causes a market response. It also includes a local replay demonstration in which historical or processed records are sent through Kafka and consumed with Spark Structured Streaming.

The current feasibility-supported pilot is Ukraine wheat, with India retained as a comparator for the 2022 period. The local batch data slice contains FAO FPMA wheat prices, FAOSTAT wheat production and trade, and NASA POWER meteorology. A bounded PySpark run has produced typed Bronze and Silver Parquet tables and a Gold M0 monthly price table. The current weather extract is a single-point screening sample, the annual-data availability lag is provisional, and an independent monthly stress target has not yet been established. No detection-skill or performance result is claimed in this manuscript. The work to date demonstrates a reproducible data-processing foundation; model comparison, target validation, representative weather construction, and streaming measurements remain necessary before conclusions about early-warning effectiveness can be drawn.

**Keywords:** wheat supply stress; early warning; food prices; temporal data integration; Apache Spark; Kafka; data engineering

## 1. Introduction

Disruptions to wheat supply can affect prices, availability, and trade through interacting agricultural, weather, and market conditions. A price series is observable and timely, but may register stress only after market effects emerge. This project asks whether adding independent supply-related signals can improve the timing and quality of early warning relative to price monitoring alone.

The study focuses on wheat as required by the project proposal. Ukraine is the working pilot geography because the selected monthly wheat price series spans the proposal's three historical event contexts. India is retained as a comparison for 2022; its selected national wheat-price series does not extend back to the earlier cases. These choices support the current feasibility profile and do not establish that results generalize to other countries.

FoodShield is a research and educational prototype. It evaluates historical observations and demonstrates replay-based streaming. It is not a production warning service, does not provide a live feed, and does not predict conflict or its causes.

## 2. Research question and objectives

### 2.1 Research question

Can multi-source temporal signals detect emerging wheat-supply stress earlier than price-only monitoring, while preserving valid information timing and making data coverage and uncertainty visible?

### 2.2 Objectives

1. Integrate wheat price, weather, production, and trade records at a country–wheat–month analytical grain where source coverage permits.
2. Compare a price-only baseline with progressively richer signal sets using common evaluation periods.
3. Prevent temporal leakage by distinguishing an observation's reference period from the date it could reasonably have been available.
4. Evaluate historical stress contexts using evidence independent of model inputs, and avoid treating broad global event descriptions as automatic country-level labels.
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
| Wheat price | FAO GIEWS Food Price Monitoring and Analysis (FPMA), Ukraine national-average wheat-grain wholesale series; selected series UUID `a9d7b6ee-6a9d-43a7-bca4-aaa419311498`; UAH/tonne; provider-calculated monthly records used for M0. | Local source spans January 2001–October 2026. Metadata contains a `market_type=Retail` and `price_type=WHOLESALE` mismatch that remains unresolved. The series identifies APK-Inform as source partner; FAO inclusion alone does not establish reuse rights. Do not redistribute underlying values or expose them in public output until rights are confirmed. The FPMA tool describes its price series and associated metadata [here](https://www.fao.org/fileadmin/user_upload/foodprice/docs/others/GIEWS_Price_tool_help_en_US_01.pdf); the partner is listed in the [FAO partner profile](https://www.fao.org/giews/food-prices/data-partners/detail/en/c/237592/). |
| Production | FAOSTAT Crops and Livestock Products (QCL), Ukraine wheat (item code 15), production/yield/harvested-area measures where present. | Current filtered extract has 99 rows. Units, flags, and notes are retained. FAOSTAT reference years do not give historical publication vintages. The current two-year availability lag is a conservative modeling proxy, not a verified release date. FAOSTAT offers official [data and bulk downloads](https://www.fao.org/faostat/). |
| Trade | FAOSTAT Crops and Livestock Products Trade (TCL), Ukraine wheat, reporter-level import/export quantity and value measures. | Current filtered extract has 132 rows. Units, flags, and notes are retained. The selected extract has no partner dimension; it supports aggregate reporter measures only. Historical archive rows do not establish the date each observation became public. |
| Weather | NASA POWER daily agroclimatology parameters `PRECTOTCORR`, `T2M_MAX`, and `T2M_MIN`, sampled at 49°N, 32°E for 2001–2022. | Current extract has 24,105 daily rows and monthly summaries for 264 location-months. This is a single-point screening sample, not a crop-weighted or nationally representative exposure measure. NASA describes the [Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/) as returning daily time-series data at the source spatial resolution; agricultural interpretation still requires defensible wheat-area geography and weighting. |

The local FPMA input should be treated as controlled research data until reuse conditions are confirmed. Source attribution and data-use terms must be reviewed before public release. Missing records remain missing unless the source supports a defensible imputation rule; they are not silently converted to zero.

## 5. Methodology

### 5.1 Data preparation and layered representation

Source-specific records are parsed into Bronze tables while retaining source identifiers, units, notes, flags, original time fields, and source-file keys where available. Silver tables make dates and numeric values explicit and normalize fields needed for joins while retaining source-specific meaning. Gold tables represent research-ready analytical products. These are data-processing layers; each contains wheat data, and the layer names do not represent additional commodities.

The current Gold table is `m0_monthly_price`, keyed at country × wheat × month. It takes the provider-calculated monthly series rather than recomputing monthly values from weekly observations. The current data slice does not yet include a joined monthly M1–M4 feature table.

The processing uses Python and PySpark DataFrames/Spark SQL. Parquet stores the derived tables. A profile records generated schemas, row counts, configuration identity, and known limitations. The pipeline rejects empty outputs, null declared key values, and duplicate records at the declared grain. It does not infer absent months as zero.

### 5.2 Temporal availability and leakage control

Monthly prices retain the source observation date and frequency. Weather observations retain daily dates and are summarized to month with the spatial basis and valid-day coverage visible. For annual FAOSTAT production and trade measures, the current transformation sets `feature_available_from` to 1 January of `data_year + 2`. This deliberately conservative rule is an assumption used to prevent obvious same-year leakage; it is not a verified historical publication date. Sensitivity analysis or historical vintages are needed before strong as-of claims can be made.

Feature joins must admit only signals available by the modeled month. Reference year and availability date must remain separate fields. Any future unit conversion must retain the original value and record the conversion rule and factor. Different price concepts, such as nominal, real, and USD values, remain separate measures.

### 5.3 Weather representation

NASA POWER daily corrected precipitation and maximum/minimum temperature are the current weather candidates. The present point is useful for checking API fields, dates, units, sentinels, and monthly aggregation. It cannot substantiate nationwide or wheat-area exposure. A defensible M1 analysis needs a documented set of wheat-producing regions/grid cells and validated harvested-area weights for the relevant years. Where weights or coverage are unavailable, affected model stages must be limited to a common verified period or omitted; the point sample must not be relabeled as representative.

### 5.4 Historical evaluation and target validity

The proposal's 2007–08, 2010/11, and 2022 periods provide independent global event-study context. Model detection timing may be described relative to these windows only with explicit geographic interpretation. Precision, recall, F1, and false-positive rate require a separate observable country-month target, negative periods, and a documented thresholding protocol. The target must not be constructed solely from the price series that M0 is evaluated against. Threshold selection must not use the holdout event periods.

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

Apache Spark is the primary batch and streaming compute engine. Kafka provides the local replay transport. Parquet is used for derived analytical tables. The workflow is designed for bounded execution on a Windows 11 laptop with 8 GB RAM; this is a project environment constraint, not a general performance claim.

## 7. Current results and evidence

The successful local ETL run produced the following pilot row counts:

| Layer/table | Rows |
|---|---:|
| Bronze FPMA prices | 804 |
| Bronze FAOSTAT production | 99 |
| Bronze FAOSTAT trade | 132 |
| Bronze NASA daily point observations | 24,105 |
| Silver prices | 804 |
| Silver production | 99 |
| Silver trade | 132 |
| Silver daily weather screening | 24,105 |
| Silver monthly weather screening | 264 |
| Gold M0 monthly wheat price | 310 |

These are counts in the local Ukraine wheat pilot extracts, not evidence of complete national coverage, model skill, or scalability. The run's schemas, counts, and limitations are recorded in `data/processed/week2_ukraine/profile.json`; the source contract and reproducibility instructions are in [`WEEK2_ETL.md`](WEEK2_ETL.md).

No M0–M4 detection metrics have been measured in the current project evidence. No independent monthly stress target has been approved. No crop-weighted weather signal has been produced. No Kafka/Spark replay benchmark has been measured. Therefore, no conclusion about early-warning performance, detection lead time, throughput, or latency is currently warranted.

## 8. Evaluation plan and reporting criteria

### 8.1 Analytical evaluation

Once an independent target and valid model inputs are available, report detection lead time, precision, recall, F1, and false-positive rate with the target definition, event dates, negative periods, geographic scope, thresholds, and holdout protocol. Compare M0–M4 on the same supported months wherever possible. Report how sample size and missingness change as signals are added. Separate event context from validated outcomes and avoid causal interpretation.

### 8.2 Data and engineering evaluation

Report source coverage and join completeness, missingness, key uniqueness, temporal availability, transformation reproducibility, and retained source metadata. For batch processing, measure runtime, input/output size, throughput, and relevant join/shuffle costs under a fixed documented configuration. For streaming, measure sustained and burst throughput, p50/p95 end-to-end alert latency, and valid-record accounting under a fixed replay workload. Proposed benchmark values remain targets until observed; report measured values with environment and method rather than implying target attainment.

## 9. Software prerequisites and reproducible use

### 9.1 Prerequisites

The current batch ETL requires:

- Windows 11 (current bounded local environment) or a compatible environment with the required Hadoop native support;
- Python 3.10 or later;
- Java 17;
- PySpark 4.3.0, pinned in `requirements.txt`;
- Hadoop Windows native files compatible with the PySpark-bundled Hadoop 3.5.0 client (`winutils.exe` and `hadoop.dll`) and `HADOOP_HOME` pointing to that Hadoop home on Windows;
- local copies of the four inputs listed in `configs/week2.local.json`.

The replay demonstration will additionally require a compatible local Apache Kafka broker and Spark's Kafka connector matching the Spark version. The Kafka runtime/version and connector invocation are not yet part of the demonstrated workflow. No cloud account or paid service is required by the project design.

### 9.2 Run the current batch pipeline

From the repository root in PowerShell, with prerequisites already available:

```powershell
$env:HADOOP_HOME = 'C:\hadoop'
python scripts\run_week2_etl.py
```

The run reads `configs/week2.local.json`, leaves `data/raw/` unchanged, and writes the pilot Parquet layers and `profile.json` beneath `data/processed/week2_ukraine/`. Generated outputs are local and Git-ignored. To repeat the analysis with changed inputs, update a copied configuration and use a distinct output directory; first profile alternate source identifiers and mappings.

### 9.3 Data access and provenance

Use [`DATA_SOURCE_REGISTER.md`](DATA_SOURCE_REGISTER.md) for the source URLs, API request examples, retrieval paths, filters, and known provenance gaps. Raw inputs are intentionally excluded from version control. Do not place API keys, CAPTCHA contents, cookies, or account credentials in the repository. For future refreshes, record the exact secret-free request, retrieval date, filters, file size/checksum, and source terms in the register.

## 10. Limitations and responsible interpretation

1. **Price rights and metadata:** the Ukraine FPMA series has unresolved source-partner reuse rights and a Retail/Wholesale metadata discrepancy. Public redistribution of raw values or value-revealing outputs is restricted pending confirmation.
2. **Weather geography:** the current NASA data are a single point. They cannot support national or crop-weighted conclusions.
3. **Annual-data timing:** the two-year lag is provisional and does not reconstruct what was publicly known at each historical date.
4. **Event labels:** international event windows are not country-level outcome labels. An independent stress target is required for classification metrics.
5. **Coverage and comparability:** candidate countries and source series differ in start date, market level, product definition, and coverage. Cross-country conclusions require harmonized and profiled data.
6. **Model and runtime evidence:** model performance, causal relationships, scale-out behavior, and streaming targets have not been established by the current run.
7. **Scope:** findings, when available, apply to wheat and the geographies/time periods supported by validated sources. Other commodities are outside this study.

## 11. Discussion and conclusion

The project has established a source-aware, layered processing foundation for a wheat-only historical study and has materialized a Ukraine pilot price baseline with associated production, trade, and exploratory weather tables. The evidence currently supports feasibility of a bounded local ETL workflow, not the effectiveness of a multi-signal warning model. The next analytical conclusions depend on representative weather construction, a defensible availability policy for annual signals, independent stress-target definition, and evaluation on common historical periods. The local Kafka replay remains an engineering demonstration to be measured separately from historical model evaluation.

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
