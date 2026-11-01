# Week 2 local ETL

## Purpose and current scope

This is a bounded Spark batch ingestion slice for the working Ukraine wheat pilot. The commodity remains wheat per the proposal. Country, FAOSTAT M49/item codes, FPMA series UUID, paths, units, annual lag, and output root are values in `configs/week2.local.json`; changing them requires pointing to a separately profiled compatible source extract and series. This makes the runner configurable without claiming that every country/commodity is already supported.

The pipeline leaves `data/raw/` unchanged and writes Parquet under the ignored `data/processed/week2_ukraine/` path:

- **Bronze:** source-specific flattened FPMA observations, FAOSTAT QCL/TCL rows, and NASA daily point observations, preserving source flags/notes and IDs where available.
- **Silver:** typed long-form prices, production and trade, plus daily/monthly NASA weather screening tables. Annual rows receive `feature_available_from = January 1 of (data_year + 2)` as a provisional rule. The weather spatial basis is explicitly `single_point_screening`.
- **Gold:** `gold/m0_monthly_price`, one row per Ukraine/wheat/month from FPMA's calculated monthly records. It is not an event label and does not join the exploratory point weather into a representative country feature.
- `profile.json`: output counts, Spark schemas, config identity, generation time, and material limitations.

## Local setup and run

Prerequisite: Python 3.10+, Java 17+, and a Hadoop Windows build matching PySpark's bundled Hadoop 3.5.0 client. The local Hadoop `bin` must contain both `winutils.exe` and `hadoop.dll`: Spark uses `winutils` for Windows filesystem operations and the native DLL for Hadoop `NativeIO`. The project pins PySpark 4.3.0 in `requirements.txt`; Spark's official install documentation lists Python 3.10+ and Java 17+ requirements ([PySpark installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)). Apache's Windows guidance explains that official Hadoop releases do not include Windows binaries and documents building from source ([Apache Hadoop Windows guidance](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems)).

For this workspace, both components were built from the official [Hadoop 3.5.0 source archive](https://downloads.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0-src.tar.gz), SHA-512 `90c4f187af0606443523249a40c9af487bd0bbee7af3ff777492f5b5198e599583d07e4d55f569e39842e3304a605e929babf6bbea7d7683c7fac0523147ab0b`. The source archive predates a Windows SDK 10.0.26100 name collision; the build applies Apache's merged [HADOOP-19977 fix](https://github.com/apache/hadoop/pull/8713.patch). Microsoft Visual Studio 2022 Build Tools, MSVC, Windows SDK, and CMake are installed on the maintainer's machine. The resulting local Hadoop home is in a local folder on that machine.

From the repository root in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
$env:HADOOP_HOME = 'C:\hadoop'
.\.venv\Scripts\python scripts\run_week2_etl.py
```

The runner adds `$HADOOP_HOME\bin` to the child process `PATH`. It uses `local[2]`, a 1 GiB Spark driver, four SQL shuffle partitions, and UTC session time. For the Windows JVM selector it reuses the ignored `data/processed/.spark-tmp/` directory, clearing prior files at the start of each run. Re-running replaces only the generated `data/processed/week2_ukraine/` tables/report; it does not write to the raw sources. Use a copied config with a new `output_root` for parallel runs.

## Data grain and quality checks

| Dataset | Grain / key | Important types and rules |
|---|---|---|
| Bronze/silver price | Series UUID × source record ID; gold is country × commodity × month | Price values are doubles; monthly rows are selected by FPMA `periodicity=monthly`, not re-aggregated from weekly observations. |
| QCL production | Country × commodity × element × data year | Decimal value; retain unit, flag, note; availability date uses the configured two-year lag. |
| TCL trade | Country × commodity × element × data year | Decimal value; retain unit, flag, note; aggregate reporter rows only; no partner key exists in current extract. |
| NASA daily screening | Country × commodity × date × parameter × coordinates | Daily value with API unit; `-999` sentinel excluded from silver; coordinates/time standard from API request metadata. |
| NASA monthly screening | Country × commodity × month × coordinates | Mean daily max/min temperature; sum daily corrected precipitation; precipitation valid-day count is retained. |

The runner fails on empty datasets, null key values, or duplicate rows at each declared grain. It does not infer missing months as zeros and does not claim crop-weighted coverage, licensing clearance, historical annual vintages, event-label quality, or model performance.

## Current execution status

PySpark 4.3.0 and Java 17 are installed. Matching Hadoop 3.5.0 `winutils.exe` and `hadoop.dll` were built from Apache source and stored in a local folder; the successful local run wrote Parquet layers and `profile.json` under the ignored `data/processed/week2_ukraine/` directory. Counts: Bronze prices 804, production 99, trade 132, weather point rows 24,105; Silver prices 804, production 99, trade 132, daily weather rows 24,105, monthly weather rows 264; Gold M0 monthly prices 310. The profile records source schemas and the limitations above. These counts describe the local pilot extracts, not nationwide coverage or model results.
