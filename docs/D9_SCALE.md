# D9: data-engineering scale

**Decision:** D9 (confirmed at the Week 4 review). Options A, B, C and E are in scope. D (multi-country panel) is extension work after the core project (D13, revised at the start of Week 6).
**Why:** The model table is small by design (monthly early warning for one country). The engineering work is handling the raw sources at their real size, and showing it in measured runtimes.
**Results:** A and B were run after the Week 5 test. The numbers below come from `data/processed/d9a_faostat_bulk/bulk_profile.json` and `data/processed/d9b_grid_weather/grid_weather_profile.json`.

## Status

| Item | What | Code | Status |
|---|---|---|---|
| A | Full FAOSTAT bulk archives (all countries, all items) → Spark → year-partitioned Parquet, with Ukraine wheat reconciled to the Week 2 extracts | `src/foodshield/etl/bulk.py`, `scripts/run_d9a_faostat_bulk.py` | **Done.** 22.7 million rows from 3.2 GB of CSV in 2 min 40 s. Ukraine wheat matches the model inputs exactly. |
| B | NASA POWER weather for all 232 native grid cells inside the 24 wheat oblasts, 2001–2022 daily → Spark → wheat-weighted national series | `configs/d9b_grid_cells.json`, `scripts/fetch_d9b_gridded_weather.py`, `scripts/run_d9b_grid_weather.py` | **Done.** 1,864,120 daily rows aggregated in 11 s. Almost identical to the 24-point series used by the models. |
| C | Kafka replay at daily granularity into Spark Structured Streaming | `src/foodshield/streaming/`, `scripts/run_week6_replay.py` | **Built (Week 6, D12).** 1,864,486 records; about 5,000 events/s at full speed on 2 cores; every check passed. See `WEEK6_STREAMING.md`. |
| E | Labelled synthetic scaling runs (10×, 100×, 1000×) | — | Week 7, starting from the A/B Parquet outputs. |
| D | Multi-country wheat panel | — | **Extension work** (D13). **31 countries** have enough monthly wheat producer prices (see A below), so D is feasible on price data. |
| TM | Detailed trade matrix (411 MB archive, optional) | Supported by the same code | Not downloaded. Optional; only needed if the team wants partner-level trade flows. |

Machine for all timings: Claude cloud workspace, Linux, **2 CPU cores, 7 GB RAM**, Spark 4.3.0 `local[2]`, driver memory 3 GB (A) or 2 GB (B). Timings on a team member's laptop will differ; the scripts record the machine with every run.

## A. Full FAOSTAT archives

Sizes (1 MB = 1,000,000 bytes):

| Domain | ZIP | CSV inside | Parquet | CSV ÷ Parquet | Rows | Years | Countries/areas | Items |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| Production (QCL) | 33.9 MB | 545.0 MB | 37.6 MB | 14.5× | 4,209,110 | 1961–2024 | 244 | 301 |
| Trade (TCL) | 273.5 MB | 2,432.8 MB | 200.8 MB | 12.1× | 17,143,873 | 1961–2024 | 283 | 616 |
| Producer prices (PP) | 11.7 MB | 214.3 MB | 9.3 MB | 23.1× | 1,319,583 | 1991–2025 | 182 | 235 |
| **Total** | **319.1 MB** | **3,192.1 MB** | **247.6 MB** | **12.9×** | **22,672,566** | | | |

Timings (seconds):

| Domain | Unzip | Count rows (CSV) | Write Parquet | Count rows (Parquet) | Same query on CSV | Same query on Parquet | Query speed-up |
|---|---:|---:|---:|---:|---:|---:|---:|
| QCL | 0.8 | 3.8 | 22.3 | 0.5 | 3.79 | 0.34 | 11× |
| TCL | 6.0 | 4.0 | 72.1 | 0.2 | 14.61 | 0.33 | **44×** |
| PP | 0.3 | 0.5 | 6.8 | 0.2 | 1.56 | 0.23 | 7× |

The query is "all countries, wheat, 2010–2022" (6,087 / 11,549 / 10,854 rows). Parquet is faster because it is split by year folder and stored by column, so Spark reads only the years and columns it needs. The bigger the file, the bigger the gain. The whole run, including Spark start-up, reconciliation and the coverage check, took 2 min 40 s.

**Ukraine reconciliation.** Ukraine wheat was filtered out of the global Parquet and compared value by value with the extracts the models use:

| Domain | Rows compared | Result |
|---|---:|---|
| QCL | 99 | Exact match |
| TCL | 132 | Exact match |
| PP | 157 monthly prices | Exact match. The bulk file also has 30 annual-average rows, which the monthly extract leaves out by design. |

So the at-scale path reproduces the model inputs.

**D9-D feasibility.** In PP, 31 countries have at least 120 of the 156 monthly wheat producer prices (local currency) for 2010–2022:

- **Complete (156):** Hungary, Lithuania, Poland, Canada, Finland, United Kingdom, Belarus, Slovakia, Ukraine, Romania, Czechia, Russian Federation.
- **150–155:** Austria 155, Spain 151, Netherlands 150.
- **140–149:** Serbia, South Africa, Peru, Rwanda, Armenia and United States 145; Türkiye, Palestine, Germany and Kazakhstan 144; Albania 140.
- **120–139:** Kyrgyzstan 134, Bulgaria 133, Croatia 132, Slovenia 122, Belgium 121.

A multi-country panel would multiply the number of stress episodes, which is the main limit of the one-country pilot. It would also need a CPI deflator per country. The FAOSTAT CP archive is the obvious candidate, but its per-country coverage and flags have not been checked yet.

## B. Gridded weather

- **Download:** 232 point requests to the NASA POWER Daily API (one per native 0.5° × 0.625° grid cell inside the 24 wheat oblasts). There were no errors or retries, giving 91.7 MB of raw JSON. The CSV has 1,864,120 daily rows (232 cells × 8,035 days, 2001–2022), is 68.4 MB, and has no missing values.
- **Spark:**
  - read and count: 2.8 s;
  - daily → monthly per cell (61,248 cell-months) → wheat-weighted national series (264 months): 8.3 s.
- **Weights:** each cell gets its oblast's wheat-area share divided by the number of cells in that oblast. The weights are fixed at the 2015–2020 shares, as in Week 4.

**Comparison with the 24-point series the models use** (one cell per oblast centroid):

| Variable | Correlation | Correlation after removing each calendar month's average | Mean absolute difference | Mean, 24 points | Mean, full grid |
|---|---:|---:|---:|---:|---:|
| Precipitation (mm/month) | 0.9988 | 0.9986 | 0.82 mm | 46.8 | 47.2 |
| Max temperature (°C) | 1.0000 | 0.9998 | 0.16 °C | 14.24 | 14.08 |
| Min temperature (°C) | 1.0000 | 0.9998 | 0.14 °C | 4.52 | 4.66 |

The middle column matters most. The corrected M1 features compare each month with the same calendar month in earlier years, so the agreement has to hold after the season is removed, and it does.

**What this means:** The Week 5 limit "one NASA grid cell per oblast" is not a material approximation at national level. MERRA-2 cells are coarse and neighbouring cells move together, so 24 well-placed cells already give the national signal. A model re-run with gridded weather would get almost the same features. Weather's weak result in the 2014–2020 backtest is therefore not caused by the sampling of points. No re-run was done: the real test is already opened, so any re-run would be post-test exploration only.

## How the downloads were done

The Claude workspace cannot reach FAO or NASA directly, so both downloads went through the Claude desktop app's built-in browser on a project member's computer. Typing into PowerShell from Claude is blocked; terminals only allow clicks.

- **FAOSTAT archives:** downloaded from the official URLs in `DATA_SOURCE_REGISTER.md` into the repository folder. They were copied into the workspace and checked with `zipfile.testzip`; the SHA-256 of each archive is in the profile.
- **NASA grid:** the same request URLs as `scripts/fetch_d9b_gridded_weather.py`, saved in four parts with a SHA-256 per part. All four hashes were verified. The parts were merged in cell order into `data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.csv`. The manifest (request URL and raw SHA-256 per cell) is committed next to it; the CSV is not.

The large files stay out of Git (`data/raw/*`, `data/interim/` and root-level `*.zip` are ignored). Only profiles, the grid manifest and derived outputs are committed.

## Reproduce on a team member's computer

The FAOSTAT archives (about 319 MB; TM adds 411 MB if wanted). In PowerShell, from the repository folder:

```powershell
New-Item -ItemType Directory -Force data\raw\faostat_bulk | Out-Null
$base = "https://bulks-faostat.fao.org/production/"
foreach ($f in "Production_Crops_Livestock_E_All_Data_(Normalized).zip",
               "Trade_CropsLivestock_E_All_Data_(Normalized).zip",
               "Prices_E_All_Data_(Normalized).zip") {
    curl.exe -L --fail -o "data\raw\faostat_bulk\$f" "$base$f"
}
python scripts\run_d9a_faostat_bulk.py           # needs Spark set up as in WEEK2_ETL.md; ~3.5 GB free disk
```

If the archives are already in the repository root (from the browser download), move them instead of downloading again:

```powershell
New-Item -ItemType Directory -Force data\raw\faostat_bulk | Out-Null
Move-Item -Path '.\*_E_All_Data_(Normalized).zip' -Destination data\raw\faostat_bulk\
```

Gridded weather. The fetch is resumable and takes a while (one request per cell). Then run the Spark step:

```powershell
python scripts\fetch_d9b_gridded_weather.py
python scripts\run_d9b_grid_weather.py
```

Archives can change after each FAO update. Compare the archive SHA-256 in `bulk_profile.json` before comparing results from different download dates.
