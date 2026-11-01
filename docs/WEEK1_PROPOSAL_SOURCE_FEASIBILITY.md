# Week 1 proposal-source feasibility: event coverage and pilot selection

**Checked:** Week 1
**Status:** Working Week 1 decision: Ukraine is the provisional pilot; India is the 2022 comparator. The three proposal periods are global event-study contexts, not country-level binary labels. Price reuse, regional weather weighting, and strict historical annual vintages remain later release/validation gates.

## Scope and decision

The approved objective remains wheat supply stress: compare price-only monitoring with progressively richer signals at a country–commodity–month grain, and validate against documented historical cases. The project does not predict wars or other geopolitical causes. Keep the M0–M4 sequence and local Kafka replay requirement unchanged.

The proposal discussion deck names three historical cases: 2007–08 global wheat-price stress; the 2010 CIS drought/Russian export-ban shock; and the 2022 Black Sea disruption. The event windows are candidate study intervals, not finalized labels. See [WEEK1_EVENT_CASES.md](WEEK1_EVENT_CASES.md).

**Ukraine is the working data-fit choice**, replacing India as the leading pilot for this three-case event-study design:

- FPMA has a national-average wholesale wheat-grain series from January 2001 to October 2026, so it spans all three cases. The API also supplies a calculated monthly series with 310 of 310 months present.
- The downloaded Ukraine series shows descriptive local price movement during the 2007–08 and 2010 cases. In 2022, the local series falls after February while FAO documents a sharp rise in international wheat prices. This difference is material: the 2022 label must represent supply/logistics disruption and allow local prices to move in either direction.
- FAOSTAT production and trade extracts for Ukraine cover 1992–2024, including the event years.
- The FPMA series is sourced from APK-Inform. Its metadata has a field mismatch (`market_type=Retail`, `price_type=WHOLESALE`) and does not explain the national-average construction. Confirm source methodology and reuse terms before making it the project contract.

India remains a strong 2022-only candidate, with a complete 2014–2026 national wholesale raw-wheat monthly series. The India price series cannot test 2007–08 or 2010. A FAO review also notes that domestic wheat prices in India were stable during the 2006–08 world food crisis because of export controls, so India is a weaker choice when the validation target is international price-shock transmission.

## Price-source profiles

| Candidate | Observed series | Event coverage / assessment |
|---|---|---|
| **Ukraine** | FPMA National Average; Wheat (3rd class, bid, EXW, processing); `WHOLESALE`; UAH/tonne; weekly source values plus calculated monthly values; Jan 2001–Oct 2026; 310/310 monthly values present | Best event-period coverage among checked series. Metadata labels `market_type` as Retail while `price_type` is Wholesale; validate this inconsistency, the national-average construction, and source terms. Local price response in 2022 is downward, unlike the international benchmark. |
| **India** | FPMA National Average; Wheat raw grain; `WHOLESALE`; INR/100 kg; Jan 2014–Sep 2026; 153/153 months present | Good national price baseline, but only covers the proposal's 2022 event. All 31 India FPMA wheat series checked begin in 2014 or later. |
| Ethiopia | Addis Ababa; white wheat; `WHOLESALE`; ETB/100 kg; Jan 2000–Apr 2023; 278 observations across 280 possible months | Covers event dates, but is city-level and the 2010 local price series does not show the same rise as the international shock. |
| Russia | National Average; milling wheat, 3rd class offer EXW; `WHOLESALE`; RUB/tonne; Jan 2005–Feb 2022 | Covers 2007–08 and 2010 but ends before the 2022 event response. |
| Pakistan | Lahore retail wheat; Jan 2006–Oct 2021, mixed monthly/weekly cadence | Does not cover 2022. |
| Bangladesh | National-average imported-wheat wholesale; Jan 2022–Sep 2026, mixed monthly/weekly cadence | Too short for 2007–08 and 2010; commodity is imported wheat. |
| Egypt | National-average retail wheat; Oct 2020–Sep 2026; 68/72 months present | Covers only the 2022 event and has four missing months. |

### Ukraine FPMA profile

- Series UUID: `a9d7b6ee-6a9d-43a7-bca4-aaa419311498`.
- Metadata: Ukraine; National Average; “Wheat (3rd class, bid, EXW, processing)”; `WHOLESALE`; UAH/tonne; source APK-Inform Agency. The `market_info` field is blank and `market_type` is labeled Retail, a source-metadata inconsistency to resolve.
- Local response includes 804 data records: 494 weekly and 310 calculated monthly values. Monthly values cover 2001-01 through 2026-10 with no missing month, duplicate month, or null nominal price in the downloaded response.
- Descriptive nominal monthly prices: Jan 2007 UAH 740/tonne to Feb 2008 UAH 1,326/tonne (+79.2%); Jun–Sep 2010 UAH 1,162.5 to 1,425/tonne (+22.6%); Feb–Dec 2022 UAH 8,516.67 to 6,425/tonne (−24.6%). These are data checks, not model outcomes or causal estimates. Do not infer why domestic prices moved from this series alone.
- Downloaded API resources: [series metadata](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSerieDomestic/?uuid__in=a9d7b6ee-6a9d-43a7-bca4-aaa419311498&format=json) and [price observations](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=a9d7b6ee-6a9d-43a7-bca4-aaa419311498&format=json).

### India FPMA profile kept as comparison

- Series UUID: `71077031-ad24-44b4-8358-50763b95d6db`.
- India; National Average; raw wheat; `WHOLESALE`; INR/100 kg; Jan 2014–Sep 2026; 153/153 months complete; source Ministry of Consumer Affairs.
- The 31 wheat price series returned for India all begin in 2014 or later. The downloaded series cannot validate 2007–08 or 2010.
- [Series metadata](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSerieDomestic/?uuid__in=71077031-ad24-44b4-8358-50763b95d6db&format=json) and [price observations](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=71077031-ad24-44b4-8358-50763b95d6db&format=json).

## FAOSTAT downloads and profiles

The API query endpoint required authentication during this check, so filtered extracts were produced from FAO's official normalized bulk files. The two domain archives and filtered CSVs are in the ignored local `data/raw/` folder. The archive metadata retains retrieval date, source URL, filters, years, units, flags, and fields. Archive updates are QCL 2025-12-31 and TCL 2026-07-24, as listed in the official [dataset catalog](https://bulks-faostat.fao.org/production/datasets_E.json).

| Country/domain | Filtered output | Coverage and profile |
|---|---|---|
| Ukraine QCL | `faostat_ukraine_wheat_production_2026-11.csv` | Production, yield, and harvested area; 1992–2024; 99 rows; units t, kg/ha, ha; 97 `A`, one `E`, one `I` flag. |
| Ukraine TCL | `faostat_ukraine_wheat_trade_all_measures_2026-11.csv` | Import/export quantity and value; 1992–2024; 132 rows (33 per measure); quantity t, value 1000 USD; 112 `A`, 11 `X`, 9 `I` flags. |
| India QCL | `faostat_india_wheat_production_2026-11.csv` | Production, yield, harvested area; 1961–2024; 192 rows; units t, kg/ha, ha; all `A` flags. |
| India TCL | `faostat_india_wheat_trade_all_measures_2026-11.csv` | Import/export quantity and value; 1961–2024; 256 rows (64 per measure); quantity t, value 1000 USD; 235 `A`, 21 `X` flags. Retain flags and notes. |

These TCL aggregate extracts have no partner dimension. FAOSTAT's [Trade Matrix documentation](https://files-faostat.fao.org/production/TM/TM_e.pdf) describes unreconciled bilateral discrepancies from timing, trade-system and classification differences, confidentiality, and valuation (exports FOB versus imports CIF). A bilateral matrix is only needed if the team selects a partner-specific feature; it is not required for this aggregate feasibility profile.

The archives retained locally are `faostat_qcl_production_bulk_2026-11.zip` (about 34 MB) and `faostat_tcl_trade_bulk_2026-11.zip` (about 274 MB). `faostat_tcli_trade_indicators_2026-11.zip` (about 0.9 MB) was also checked, but it does not contain trade quantities and is not the M3 quantity extract.

FAOSTAT's official database terms generally apply CC BY 4.0 and require attribution, but explicitly warn that third-party-provided records may have additional restrictions. Review the [FAO terms](https://www.fao.org/contact-us/terms/db-terms-of-use/en) and record the required attribution. Annual production and trade must use explicit publication/availability lags; same-year values cannot be assumed to have been available during the event year.

## Weather feasibility

- Existing India sample `nasa_power_haryana_daily_2022.json` is not appropriate if Ukraine is selected.
- A Ukraine screening sample, `nasa_power_ukraine_central_daily_2001_2022.json`, was downloaded at 49.0°N, 32.0°E. It contains 8,035 daily values per parameter from 2001-01-01 to 2022-12-31, with no `-999` sentinels: `PRECTOTCORR` (mm/day), `T2M_MAX` (°C), and `T2M_MIN` (°C). This single point only verifies API availability and historical coverage; it is not national or crop-weighted weather.
- NASA POWER's [Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/) supports daily point and regional requests from 1981 to near real time. For a national signal, select grid cells that overlap wheat-growing areas and define their aggregation/weights. A bounding box or city point alone would not provide a Ukraine-specific national weather series.

## Local files

The files listed below were acquired locally under `data/raw/`. The current sharing policy versions only the reviewed, attributed Ukraine wheat FAOSTAT extracts and NASA POWER snapshot; these filtered snapshots are enough for collaborators to start from the same inputs. Full bulk archives, FPMA data pending reuse-rights confirmation, unrelated candidate downloads, and generated/derived outputs remain local and Git-ignored. Missing inputs can be retrieved with `python scripts/fetch_week2_inputs.py`; provenance and current sharing status are in [DATA_SOURCE_REGISTER.md](DATA_SOURCE_REGISTER.md).

See [DATA_SOURCE_REGISTER.md](DATA_SOURCE_REGISTER.md) for the relative raw paths and exact download URLs/API requests, extraction filters, and retrieval instructions. The register also marks sources where the original request details were not retained.

| Source | Local files |
|---|---|
| FPMA Ukraine price | `fpma_ukraine_national_wheat_grain_wholesale.json`, `fpma_ukraine_national_wheat_grain_wholesale_metadata.json` |
| FPMA India price comparison | `fpma_india_national_wheat_wholesale.json`, `fpma_india_national_wheat_wholesale_metadata.json`, `fpma_india_haryana_wheat.json` |
| FPMA other country comparisons | `fpma_ethiopia_addis_ababa_wheat_wholesale.json`, `fpma_pakistan_lahore_wheat_retail.json`, `fpma_bangladesh_imported_wheat_wholesale.json`, `fpma_egypt_national_wheat_retail.json` |
| FAOSTAT normalized source archives | `faostat_qcl_production_bulk_2026-11.zip`, `faostat_tcl_trade_bulk_2026-11.zip`; the small TCLI indicator archive is retained for reference |
| FAOSTAT filtered outputs and extraction metadata | Ukraine and India QCL production CSVs plus metadata; Ukraine and India TCL `*_trade_all_measures_2026-11.csv` plus metadata. Quantity-only TCL extracts are also retained from the earlier pass. |
| Ukraine regional crop statistics | `ukraine_state_stats_crop_monthly_area_yield_2025-11.zip` (9,860,183 bytes; [official dataset page](https://stat.gov.ua/en/datasets/areas-gross-harvests-and-crop-yield-monthly), direct [archive URL](https://stat.gov.ua/sites/default/files/2025-11/ovuzp_sg.zip); archive contains annual 2011–2021 folders with PDF/XLS bulletins; contents not yet normalized or suitability-checked). It does not establish weights for 2007–08 or 2010. |
| NASA POWER samples | `nasa_power_ukraine_central_daily_2001_2022.json` and `.metadata.json`; earlier comparison `nasa_power_haryana_daily_2022.json` |
| India alternative | `ogd_variety_daily_market_prices_2026-10-31.csv` |

## Event-window alignment

The proposal event descriptions and primary-source evidence are summarized in [WEEK1_EVENT_CASES.md](WEEK1_EVENT_CASES.md). The downloaded Ukraine price series spans every proposal case; the annual production and trade data also span the years. However, annual values do not mean that same-year observations can be used as predictive features. The event labels should be independent, externally documented stress windows; the local price series is an outcome/input signal and must not be used by itself to define the target and then claim M0 predictive success.

## Later gates after the Week 1 feasibility handoff

1. Before public redistribution, confirm APK-Inform terms and explain the FPMA `market_type=Retail` / `price_type=WHOLESALE` discrepancy. Pending that, keep raw values local and do not publish reproductions of the series.
2. Profile regional bulletin fields and build NASA POWER wheat-area-weighted features only for years with verified area weights. The downloaded archive covers 2011–2021; it cannot supply weights for 2007–08 or 2010. Restrict full M0–M4 comparisons to their common verified period or obtain historical weights from a documented source.
3. Retain `data_year <= forecast_year - 2` for provisional feature engineering, but do not claim strict historical as-of availability without publication dates or vintage snapshots. Keep flags and notes and report sensitivity.
4. Define an independent monthly observed-stress target before classification metrics are computed. Current event windows support event-study timing only.

Week 1 source feasibility and recommendations are complete. The gates above are later implementation, validation, or publication work; they do not change the Week 1 completion status. The broad windows are event-study contexts only, not monthly positive/negative model labels.

## Evidence boundary

The local files establish source retrieval and the measured coverage described above. They do not establish full FPMA licensing clearance, country-wide weather representativeness, strict vintage-based no-leakage, binary event labels, or model performance. Ukraine is the working Week 1 pilot recommendation, subject to the later gates above.
