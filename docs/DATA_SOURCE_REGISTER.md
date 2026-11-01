# FoodShield Week 1 data source and download register

**Last verified:** 1 November 2026 (Asia/Kolkata)
**Workspace:** `data/raw/` contains local acquisitions plus a small allowlist of reviewed, attributed Ukraine wheat QCL/TCL and NASA POWER snapshots committed for shared work. Other raw acquisitions remain ignored. The Ukraine FPMA response stays local until APK-Inform reuse rights are confirmed. The register records retrieval methods; live APIs and bulk archives can change, so compare SHA-256 hashes before comparing results from different retrieval dates.

To populate missing Week 2 inputs without manual portal steps, run `python scripts/fetch_week2_inputs.py`. The script uses `configs/week2.local.json`, preserves existing files, extracts only the configured FAOSTAT country/item/elements, and writes a local `data/raw/retrieval_manifest.json` with retrieval times and hashes. FAOSTAT bulk archives are downloaded temporarily and removed after filtering; the full archives are not stored in the repository.

## FAO FPMA price observations

API root: `https://fpma.fao.org/giews/v4/global/price_module/api/v1/`
For a known series, retrieve its metadata and observations by UUID. For example (PowerShell):

```powershell
$uuid = 'a9d7b6ee-6a9d-43a7-bca4-aaa419311498' # Ukraine
Invoke-WebRequest "https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSerieDomestic/?uuid__in=$uuid&format=json" -OutFile 'data/raw/fpma_ukraine_national_wheat_grain_wholesale_metadata.json'
Invoke-WebRequest "https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=$uuid&format=json" -OutFile 'data/raw/fpma_ukraine_national_wheat_grain_wholesale.json'
```

| Relative path under `data/raw/` | Series UUID / query | Source URL and retrieval notes |
|---|---|---|
| `fpma_ukraine_national_wheat_grain_wholesale.json`; `fpma_ukraine_national_wheat_grain_wholesale_metadata.json` | `a9d7b6ee-6a9d-43a7-bca4-aaa419311498` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=a9d7b6ee-6a9d-43a7-bca4-aaa419311498&format=json); [metadata API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSerieDomestic/?uuid__in=a9d7b6ee-6a9d-43a7-bca4-aaa419311498&format=json). |
| `fpma_india_national_wheat_wholesale.json`; `fpma_india_national_wheat_wholesale_metadata.json` | `71077031-ad24-44b4-8358-50763b95d6db` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=71077031-ad24-44b4-8358-50763b95d6db&format=json); [metadata API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSerieDomestic/?uuid__in=71077031-ad24-44b4-8358-50763b95d6db&format=json). |
| `fpma_bangladesh_imported_wheat_wholesale.json` | `8e6ae0f8-c70f-4e9c-9206-6039a8ef32c3` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=8e6ae0f8-c70f-4e9c-9206-6039a8ef32c3&format=json). |
| `fpma_egypt_national_wheat_retail.json` | `2c900c0c-a3ce-4ea4-ab96-c7bc9b34b579` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=2c900c0c-a3ce-4ea4-ab96-c7bc9b34b579&format=json). |
| `fpma_ethiopia_addis_ababa_wheat_wholesale.json` | `3098dd37-1010-4de2-bf87-6d126e30f65f` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=3098dd37-1010-4de2-bf87-6d126e30f65f&format=json). |
| `fpma_pakistan_lahore_wheat_retail.json` | `866325ae-5fa0-4e8d-b2f9-ad9a0fa960aa` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=866325ae-5fa0-4e8d-b2f9-ad9a0fa960aa&format=json). |
| `fpma_india_haryana_wheat.json` | `007f8287-ca93-4e11-8968-17abdb049db9` | [Observations API](https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSeriePrice/?uuid__in=007f8287-ca93-4e11-8968-17abdb049db9&format=json). |

For each remaining series, use the same `FpmaSerieDomestic` URL to save metadata. If an observation file contains paginated results, follow its `next` link until null. Recheck response coverage and metadata on every refresh. Ukraine source: APK-Inform; its reuse terms and the metadata's Retail/Wholesale mismatch are still unresolved. FAO FPMA inclusion does not by itself settle third-party reuse rights.

## FAOSTAT production (QCL) and trade (TCL)

| Relative path under `data/raw/` | Retrieval URL / instructions |
|---|---|
| `faostat_qcl_production_bulk_2026-11.zip` | [Official normalized QCL archive](https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip). The acquisition script downloads this to a temporary path only when the configured country extract is missing, filters it, then removes the bulk ZIP. |
| `faostat_tcl_trade_bulk_2026-11.zip` | [Official normalized TCL archive](https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data_(Normalized).zip). The acquisition script downloads this to a temporary path only when the configured country extract is missing, filters it, then removes the bulk ZIP. |
| `faostat_tcli_trade_indicators_2026-11.zip` | [Official normalized TCLI archive](https://bulks-faostat.fao.org/production/Trade_CropsLivestockIndicators_E_All_Data_(Normalized).zip). Small reference download; derived indicators, not the quantity/value source used for M3. |
| `faostat_ukraine_wheat_production_2026-11.csv` and `.metadata.json` | **Versioned shared snapshot.** Derived from QCL archive: filter `Area Code (M49)=804`, `Item Code=15` (Wheat), elements Production, Yield, Area harvested. Keep `Flag` and `Note`; sidecar records output SHA-256 and attribution. |
| `faostat_india_wheat_production_2026-11.csv` and `.metadata.json` | Derived from QCL archive: filter `Area Code (M49)=356`, `Item Code=15` (Wheat), elements Production, Yield, Area harvested. Keep `Flag` and `Note`. |
| `faostat_ukraine_wheat_trade_all_measures_2026-11.csv` and `.metadata.json` | **Versioned shared snapshot.** Derived from TCL archive: filter `Area Code (M49)=804`, `Item Code=15` (Wheat), elements Import quantity, Export quantity, Import value, Export value. Keep units, `Flag`, and `Note`; sidecar records output SHA-256 and attribution. |
| `faostat_india_wheat_trade_all_measures_2026-11.csv` and `.metadata.json` | Derived from TCL archive: same elements, `Area Code (M49)=356`. Keep units, `Flag`, and `Note`. |
| `faostat_ukraine_wheat_trade_quantity_2026-11.csv` and `.metadata.json`; `faostat_india_wheat_trade_quantity_2026-11.csv` and `.metadata.json` | Earlier quantity-only derivations from the same TCL archive; superseded for the current profile by the `all_measures` extracts, retained locally. |

FAOSTAT statistical data are provided under CC BY 4.0 with attribution; FAO terms warn that some datasets may have third-party exceptions, so recheck source metadata before changing or redistributing a refresh ([FAO database terms](https://www.fao.org/contact-us/terms/db-terms-of-use/en)). Use the catalog [datasets_E.json](https://bulks-faostat.fao.org/production/datasets_E.json) to check archive names/update dates. The currently versioned Ukraine extracts were retrieved 1 November 2026 and are the shared working snapshot. Their sidecars include row counts, filters, attribution, and SHA-256. Bulk source archives are intentionally not stored in Git. Future refreshes create a new snapshot and may differ; the archives do not contain historical publication vintages.

## Weather, crop geography, and India mandi reference

| Relative path under `data/raw/` | Retrieval URL / instructions |
|---|---|
| `nasa_power_ukraine_central_daily_2001_2022.json` and `.metadata.json` | **Versioned shared snapshot.** [NASA POWER Daily API request](https://power.larc.nasa.gov/api/temporal/daily/point?parameters=PRECTOTCORR%2CT2M_MAX%2CT2M_MIN&community=AG&longitude=32.0&latitude=49.0&start=20010101&end=20221231&format=JSON&time-standard=UTC). The sidecar records the full request, SHA-256, and citation. Single point only; exploratory, not nationally representative. |
| `nasa_power_haryana_daily_2022.json` | NASA POWER Daily API, AG community, daily 2022 Haryana point sample. Exact request URL/coordinates were not retained in a sidecar; this file is therefore not fully reproducible from the local record alone. Recreate only after confirming the original coordinates/query or replace it with a documented request. |
| `ukraine_state_stats_crop_monthly_area_yield_2025-11.zip` | [Official State Statistics Service of Ukraine dataset page](https://stat.gov.ua/en/datasets/areas-gross-harvests-and-crop-yield-monthly); direct [archive](https://stat.gov.ua/sites/default/files/2025-11/ovuzp_sg.zip). Downloaded archive has 2011–2021 folders containing PDF/XLS bulletins; table content and wheat-area consistency have not yet been profiled. |
| `ogd_variety_daily_market_prices_2026-10-31.csv` | [OGD India resource page](https://www.data.gov.in/resource/variety-wise-daily-market-prices-data-commodity), resource UUID `9ef84268-d588-465a-a308-a864a43d0070`. This file was manually downloaded through the portal after CAPTCHA/details flow and copied from the user's Downloads folder. No stable direct CSV URL is recorded; revisit the resource page, complete its access flow, then use CSV download. Current file is only a single arrival date and not a historical series. |

## Ukraine monthly trade candidate (exploratory; not accepted as wheat target)

| Relative path under `data/raw/` | Retrieval URL / findings |
|---|---|
| `ukraine_monthly_trade_commodity_structure_2025-12.zip` and `.metadata.json` | Official [State Statistics Service of Ukraine dataset page](https://stat.gov.ua/en/datasets/external-trade-goods-monthly-quarterly-operational-data); direct archive: [ZIP](https://stat.gov.ua/sites/default/files/2025-12/shchomisyachni_obsyahy_tovarnoyi_struktury_zovnishnoyi_torhivli_za_utochnenymy_richnymy.zip). Contains 2005–2016 annual structure files and monthly commodity-structure files for 2017–2023. Inspection of 2022 identifies the cereals aggregate (HS chapter 10), values in million USD, and no wheat-specific HS 1001 measure; geographic footnote excludes Crimea, Sevastopol and parts of occupied Donetsk and Luhansk. Therefore this candidate does not meet the wheat-specific independent quantity target requirement. See sidecar for checksum, inspection method and caveats. |

The separate [data.gov.ua trade dataset](https://data.gov.ua/dataset/491d1176-5300-45c1-a9bb-0b207528e04e) states CC BY attribution terms. That does not establish that this exact State Statistics ZIP is covered by the same license. Public download was free; confirm the archive's reuse terms before redistribution. A direct UN Comtrade preview API probe was rate-limited for some periods; the UI preview results and gaps are recorded below and in the archive sidecar.

## UN Comtrade monthly wheat exports (candidate independent target)

Public query template (Ukraine exports, wheat and meslin HS 1001, monthly, partner World):

`https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=YYYYMM`

Access notes: [UN Comtrade API guide](https://uncomtrade.org/docs/un-comtrade-api/) says anonymous preview is rate-limited and intended for quick inspection. The [free Basic Individual tier](https://uncomtrade.org/docs/subscriptions/) requires a free account and enables UI downloads and API requests (up to 100,000 records per call and 500 calls/day); premium is required for bulk files. Anonymous preview is capped at 500 records and one period per call. These are current published limits, not a guarantee against later changes.

| Checked period | Result in official Comtrade preview | Net weight | Query URL |
|---|---|---:|---|
| 2007-01 | No records found | — | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=200701) |
| 2008-12 | No records found | — | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=200812) |
| 2010-07 | One row; trade value USD 36,467,016; net weight 224,629,689 kg; quantity field 0/N/A | 224,629,689 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=201007) |
| 2011-06 | One row; trade value USD 238,539,532; net weight 806,272,743 kg; quantity field 0/N/A | 806,272,743 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=201106) |
| 2022-03 | One row; trade value USD 93,284,637; net weight 306,530,915 kg | 306,530,915 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=202203) |
| 2022-12 | One row; trade value USD 336,778,737; net weight 1,642,993,073 kg | 1,642,993,073 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=202212) |

This confirms a usable wheat-specific quantity field in 2010–11 and 2022 preview records; use `Net Weight (kg)` because the separate quantity field is missing/zero in some rows. It does not confirm full monthly continuity, and the tested 2007/2008 boundary months have no records. Therefore the source is a **partial candidate**, not yet an accepted full-window target: the 2007–08 event cannot be scored on this evidence. Anonymous preview allows one selected period at a time and disables the download button; the page requires login for selecting multiple periods. Before using it, retrieve the full bounded series, measure month coverage/missingness and revisions, verify any 2007–08 gaps, and cross-check the 2010/2022 values against Ukraine's State Customs Service source. UN Comtrade is free to preview; its help page describes a free basic account/key with limited API/download access, while bulk extraction requires premium access. The underlying official statistics are updated/revised over time, so these live values need a dated local snapshot if adopted. No Comtrade series file has been downloaded to the project yet.

## Capture requirements for future downloads

For every new raw file, save a sidecar or update this register with: retrieval UTC date, exact request URL (with secrets removed), method and filters, source/license notes, response/file size and optionally checksum, API pagination or auth/CAPTCHA steps, and known limits. Never store API keys, CAPTCHA values, cookies, or account credentials in the repository. Keep raw responses unchanged; write derived extracts as separate files and document their filter recipe.
