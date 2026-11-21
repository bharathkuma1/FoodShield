# 2. Data: sources, feasibility, events and quality

What data FoodShield uses, why these sources were chosen, how each file was obtained, and how its quality was checked. Project scope is in [`1_PROJECT.md`](1_PROJECT.md); how the data is processed is in [`3_ARCHITECTURE.md`](3_ARCHITECTURE.md); the decisions referred to as D1–D16 are in [`7_DECISIONS.md`](7_DECISIONS.md).

Sections 2.2–2.4 record the Week 1 feasibility work as it stood at the end of Week 1. Items it left open were settled later: the stress label and horizon (D1, D2), the split (D3), wheat-area-weighted weather (D5) and the annual availability lag (D6).

## 2.1 Sources used in the final pipeline

| Signal | Source | Coverage used | File in the repository | Terms |
|---|---|---|---|---|
| Wheat farm-gate producer price (M0 input and future outcome) | FAOSTAT Prices (PP), Ukraine, wheat, element 5530 `Producer Price (LCU/tonne)`, monthly | 2010-01 to 2023-01, 157 months, every value flag `A` (official) | `data/raw/faostat_ukraine_monthly_wheat_producer_prices_2026-11.csv` | CC BY 4.0, cite FAO |
| All-items consumer price index (deflator) | State Statistics Service of Ukraine (SSSU) SDMX, monthly change from previous month | 1991-08 to 2026-09; chained with December 2000 = 100 | `data/raw/ukrstat_ukraine_monthly_all_items_cpi_2026-11.csv` | Law 2524-IX Art. 27: free use and redistribution, naming SSSU |
| Real farm-gate price (derived) | FAOSTAT price × 100 / chained SSSU CPI | 2010-01 to 2023-01 | `data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv` | As its inputs |
| Weather for M1–M4 (D5) | NASA POWER Daily API at 24 oblast centroids, weighted by SSSU wheat area (2015–2020 mean shares) | 2001-01 to 2022-12, 264 months | `data/processed/week4_ukraine_weather_wheat_area_weighted_monthly.csv` | Public API; acknowledge NASA POWER |
| Wheat production, yield, harvested area (M2) | FAOSTAT QCL | 1992–2024, 99 rows | `data/raw/faostat_ukraine_wheat_production_2026-11.csv` | CC BY 4.0 |
| Wheat import and export quantity and value (M3) | FAOSTAT TCL | 1992–2024, 132 rows | `data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv` | CC BY 4.0 |
| Wheat harvested area by oblast (weather weights) | SSSU SDMX | 2015–2021, 25 regions | `data/raw/sssu_ukraine_wheat_harvested_area_by_oblast_2026-11.csv` | As SSSU above |
| Oblast centroids (weather points) | geoBoundaries gbOpen UKR ADM1 | 24 wheat oblasts | `configs/week4_weather_oblasts.json` | Open boundary data; cite geoBoundaries |
| Data at full size (D9) | Full FAOSTAT QCL, TCL and PP archives; NASA POWER at 232 native grid cells | 22.7 million rows; 1,864,120 daily rows | Archives not in Git; grid manifest in `data/raw/nasa_power_grid/` | As above |

Checked but not used for results: FAO FPMA prices (Ukraine series kept local because its APK-Inform reuse terms are unconfirmed); the FAOSTAT CPI extract (replaced by the SSSU primary source); India price sources (§2.4); UN Comtrade monthly exports and the SSSU monthly trade archive (§2.5). Every file, request and hash is in the download register (§2.5).

## 2.2 Week 1 source feasibility and pilot selection

**Decision at the end of Week 1:** Ukraine is the pilot; India is the 2022 comparator. The three proposal periods are global event-study contexts, not country-level binary labels.

### Scope

The approved objective remains wheat supply stress: compare price-only monitoring with progressively richer signals at a country–commodity–month grain, and validate against documented historical cases. The project does not predict wars or other geopolitical causes. The M0–M4 sequence and the local Kafka replay requirement are unchanged.

The proposal discussion deck names three historical cases: 2007–08 global wheat-price stress; the 2010 CIS drought and Russian export-ban shock; and the 2022 Black Sea disruption. The event windows are candidate study intervals, not labels (§2.3).

**Ukraine is the data-fit choice**, replacing India as the leading pilot for this three-case event-study design:

- FPMA has a national-average wholesale wheat-grain series from January 2001 to October 2026, so it spans all three cases. The API also supplies a calculated monthly series with 310 of 310 months present.
- The downloaded Ukraine series shows descriptive local price movement during the 2007–08 and 2010 cases. In 2022, the local series falls after February while FAO documents a sharp rise in international wheat prices. This difference is material: the 2022 label must represent supply and logistics disruption and allow local prices to move in either direction.
- FAOSTAT production and trade extracts for Ukraine cover 1992–2024, including the event years.
- The FPMA series is sourced from APK-Inform. Its metadata has a field mismatch (`market_type=Retail`, `price_type=WHOLESALE`) and does not explain the national-average construction. Source methodology and reuse terms would need confirming before it could become the project contract. *(In Week 4 the project lead selected the FAOSTAT farm-gate price instead; see §2.1.)*

India remains a strong 2022-only candidate, with a complete 2014–2026 national wholesale raw-wheat monthly series. It cannot test 2007–08 or 2010. A FAO review also notes that domestic wheat prices in India were stable during the 2006–08 world food crisis because of export controls, so India is a weaker choice when the validation target is international price-shock transmission.

### Price-source profiles

| Candidate | Observed series | Event coverage / assessment |
|---|---|---|
| **Ukraine** | FPMA National Average; Wheat (3rd class, bid, EXW, processing); `WHOLESALE`; UAH/tonne; weekly source values plus calculated monthly values; Jan 2001–Oct 2026; 310/310 monthly values present | Best event-period coverage among checked series. Metadata labels `market_type` as Retail while `price_type` is Wholesale. Local price response in 2022 is downward, unlike the international benchmark. |
| **India** | FPMA National Average; Wheat raw grain; `WHOLESALE`; INR/100 kg; Jan 2014–Sep 2026; 153/153 months present | Good national price baseline, but only covers the proposal's 2022 event. All 31 India FPMA wheat series checked begin in 2014 or later. |
| Ethiopia | Addis Ababa; white wheat; `WHOLESALE`; ETB/100 kg; Jan 2000–Apr 2023; 278 observations across 280 possible months | Covers event dates, but is city-level and the 2010 local price series does not show the same rise as the international shock. |
| Russia | National Average; milling wheat, 3rd class offer EXW; `WHOLESALE`; RUB/tonne; Jan 2005–Feb 2022 | Covers 2007–08 and 2010 but ends before the 2022 event response. |
| Pakistan | Lahore retail wheat; Jan 2006–Oct 2021, mixed monthly/weekly cadence | Does not cover 2022. |
| Bangladesh | National-average imported-wheat wholesale; Jan 2022–Sep 2026, mixed monthly/weekly cadence | Too short for 2007–08 and 2010; commodity is imported wheat. |
| Egypt | National-average retail wheat; Oct 2020–Sep 2026; 68/72 months present | Covers only the 2022 event and has four missing months. |

**Ukraine FPMA profile:**

- Series UUID: `a9d7b6ee-6a9d-43a7-bca4-aaa419311498`.
- Metadata: Ukraine; National Average; "Wheat (3rd class, bid, EXW, processing)"; `WHOLESALE`; UAH/tonne; source APK-Inform Agency. The `market_info` field is blank and `market_type` is labelled Retail, a source-metadata inconsistency.
- The local response includes 804 data records: 494 weekly and 310 calculated monthly values. Monthly values cover 2001-01 through 2026-10 with no missing month, duplicate month, or null nominal price.
- Descriptive nominal monthly prices: Jan 2007 UAH 740/tonne to Feb 2008 UAH 1,326/tonne (+79.2%); Jun–Sep 2010 UAH 1,162.5 to 1,425/tonne (+22.6%); Feb–Dec 2022 UAH 8,516.67 to 6,425/tonne (−24.6%). These are data checks, not model outcomes or causal estimates.

**India FPMA profile (comparison):** series UUID `71077031-ad24-44b4-8358-50763b95d6db`; National Average; raw wheat; `WHOLESALE`; INR/100 kg; Jan 2014–Sep 2026; 153/153 months; source Ministry of Consumer Affairs. The 31 wheat price series returned for India all begin in 2014 or later.

API links for both series are in §2.5.

### FAOSTAT downloads and profiles

The FAOSTAT API query endpoint required authentication during this check, so filtered extracts were produced from FAO's official normalised bulk files. Archive updates at the time were QCL 2025-12-31 and TCL 2026-07-24, as listed in the official [dataset catalogue](https://bulks-faostat.fao.org/production/datasets_E.json).

| Country/domain | Filtered output | Coverage and profile |
|---|---|---|
| Ukraine QCL | `faostat_ukraine_wheat_production_2026-11.csv` | Production, yield, and harvested area; 1992–2024; 99 rows; units t, kg/ha, ha; 97 `A`, one `E`, one `I` flag. |
| Ukraine TCL | `faostat_ukraine_wheat_trade_all_measures_2026-11.csv` | Import/export quantity and value; 1992–2024; 132 rows (33 per measure); quantity t, value 1000 USD; 112 `A`, 11 `X`, 9 `I` flags. |
| India QCL | `faostat_india_wheat_production_2026-11.csv` | Production, yield, harvested area; 1961–2024; 192 rows; units t, kg/ha, ha; all `A` flags. |
| India TCL | `faostat_india_wheat_trade_all_measures_2026-11.csv` | Import/export quantity and value; 1961–2024; 256 rows (64 per measure); quantity t, value 1000 USD; 235 `A`, 21 `X` flags. |

These TCL aggregate extracts have no partner dimension. FAOSTAT's [Trade Matrix documentation](https://files-faostat.fao.org/production/TM/TM_e.pdf) describes unreconciled bilateral discrepancies from timing, trade-system and classification differences, confidentiality, and valuation (exports FOB versus imports CIF). A bilateral matrix is only needed for a partner-specific feature, which was not chosen.

The archives were about 34 MB (QCL) and 274 MB (TCL). The TCLI indicator archive (about 0.9 MB) was also checked, but it does not contain trade quantities and is not the M3 source.

FAOSTAT's database terms generally apply CC BY 4.0 and require attribution, but warn that third-party-provided records may have additional restrictions ([FAO terms](https://www.fao.org/contact-us/terms/db-terms-of-use/en)). Annual production and trade must use explicit publication/availability lags; same-year values cannot be assumed to have been available during the event year.

### Weather feasibility

- The India sample `nasa_power_haryana_daily_2022.json` is not appropriate for a Ukraine pilot.
- A Ukraine screening sample, `nasa_power_ukraine_central_daily_2001_2022.json`, was downloaded at 49.0°N, 32.0°E. It has 8,035 daily values per parameter from 2001-01-01 to 2022-12-31, with no `-999` sentinels: `PRECTOTCORR` (mm/day), `T2M_MAX` (°C), and `T2M_MIN` (°C). This single point only verifies API availability and historical coverage; it is not national or crop-weighted weather.
- NASA POWER's [Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/) supports daily point and regional requests from 1981 to near real time. A national signal needs grid cells over wheat-growing areas with defined weights. *(Done in Week 4 as D5, 24 wheat oblasts; checked against the full 232-cell grid in D9-B.)*

### Gates left after the Week 1 handoff, and how they were closed

1. Before public redistribution, confirm APK-Inform terms and explain the FPMA Retail/Wholesale discrepancy; until then keep raw values local. *Closed by not using FPMA for shared results (§2.1).*
2. Build wheat-area-weighted weather only for years with verified area weights. The regional archive covers 2011–2021 and cannot supply weights for 2007–08 or 2010. *Closed by D5: fixed 2015–2020 shares.*
3. Keep `data_year <= forecast_year - 2` for feature engineering, without claiming strict historical as-of availability. *Closed by D6, with a 3-year-lag sensitivity run.*
4. Define an independent monthly observed-stress target before computing classification metrics. *Closed by D1 and D2.*

**Evidence boundary.** The Week 1 files establish source retrieval and the measured coverage above. They do not establish FPMA licensing clearance, country-wide weather representativeness, strict vintage-based no-leakage, binary event labels, or model performance.

## 2.3 Historical event cases and outcome framing

The three proposal windows are examples from proposal development, not mandatory cases or country-level ground-truth labels. Cases are contexts for observable market or supply stress; they are not labels for predicting wars, export bans, or their causes. The outcome and its onset are defined independently of future model inputs, and a strict time cutoff lets only earlier data enter each warning.

FAO primary sources support the three as international wheat-market stress episodes:

| Proposal example | Candidate event-study window | Documented context / onset marker |
|---|---|---|
| 2007–08 | 2007-01 through 2008-12; March 2008 is a candidate international peak marker | FAO reports wheat prices rose 118% from January 2007 to March 2008; by October 2008 export prices were nearly 50% below the March peak, while still elevated. This supports the broad window, not a country-level label. |
| 2010 / 2010–11 | 2010-07 through 2011-06 | FAO reports international grain prices surging from July after drought-reduced CIS crops and Russia's export ban; September wheat was 60–80% above July. A separate FAO review describes wheat and maize world prices doubling in 2010 H2 and 2011 H1. |
| 2022 Black Sea disruption | 2022-03 through 2022-12; February 2022 recorded separately as disruption context | FAO reported the Cereal Price Index up 17.1% in March versus February and world wheat prices up 19.7% that month, with the Black Sea war as a major driver. FAO also documented that 20% of Ukraine's planted winter-crop area might not be harvested. |

Primary sources: [FAO Food Outlook, November 2007](https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf), [FAO Food Outlook, November 2008](https://www.fao.org/4/ai474e/ai474e03.htm), [FAO Crop Prospects and Food Situation, No. 3, September 2010](https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/), [FAO Lessons from the world food crisis of 2006–08](https://www.fao.org/4/i2330e/i2330e.pdf), and [FAO March 2022 food-price report](https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en).

### Coverage-first case selection

The pilot target is future Ukraine wheat farm-gate producer-price stress, using the FAOSTAT monthly producer-price series and the SSSU CPI deflator. At prediction month `t`, the target is measured strictly after `t`; it uses the same price source as M0 at a later time, so it is temporally separated but not source-independent.

| Signal | Ukraine files in `data/raw/` | India files in `data/raw/` | Case-selection implication |
|---|---|---|---|
| FAOSTAT monthly wheat producer price | 2010-01 to 2023-01; continuous through 2022-12; all values flagged official | Not profiled as the selected target | Selected source is farm-gate, not wholesale. The extra January 2023 observation supports a three-month future window only through October 2022. |
| FPMA monthly wheat price | 2001-01 to 2026-10; 310 calculated monthly observations; metadata mismatch and third-party rights open | 2014-01 to 2026-09; 153 monthly observations; 2022 is in coverage | Ukraine FPMA remains a local comparator only; it is not used for the shared target. |
| NASA POWER weather | Daily 2001-01-01 to 2022-12-31 at one point (32.0 E, 49.0 N), 8,035 observations per parameter | Daily 2022 only at one Haryana point | With these files alone, the all-input common monthly period is 2001–2022 for Ukraine and 2022 for India. |
| FAOSTAT wheat production | Annual 1992–2024; production, yield, harvested area; flags preserved | Annual 1961–2024; same three measures | Adequate span for both; an availability lag is needed. |
| FAOSTAT wheat trade | Annual import/export quantity and value 1992–2024; flags preserved | Annual 1961–2024 | Structural features, not monthly outcome labels. A two-year lag is only a proxy pending release-vintage evidence. |
| Independent monthly trade outcome candidate | UN Comtrade UI samples: HS 1001 net weight visible in 2010/2011 and 2022; sampled 2007/2008 months return no records | 2022-05 query returned no records | Do not constrain dates to these sparse probes. |

The farm-gate series, the weather and the annual snapshots support a Ukraine M0–M4 feature window from 2010 through 2022 before lookback and feature-completeness rules. Cases are chosen from the final usable intersection, rather than forcing the proposal examples.

### Outcome options against coverage

| Candidate outcome | Fit to research question | Coverage/access evidence | Assessment |
|---|---|---|---|
| Future Ukraine farm-gate wheat producer-price stress | Closest shareable outcome for testing whether added signals warn of future wheat-price stress at the producer level. M0 uses price history through `t`; M1–M4 use the same cutoff plus other signals; the outcome is measured after `t`. | FAOSTAT PP is continuous January 2010–December 2022, with January 2023 available for future outcomes. SSSU CPI covers all price months. | **Selected.** It measures farm-gate producer prices, not wholesale/retail prices or physical shortage. Temporally separated but not source-independent. Stress rule and horizon defined in advance (D1, D2); thresholds fitted on training/validation only. |
| Monthly wheat export-flow stress | An external quantity-based signal of cross-border flow disruption. | Comtrade preview has HS 1001 net weight in sampled 2010-07, 2011-06, 2022-03, 2022-12; sampled 2007-01 and 2008-12 have no rows. No full monthly extract. | **Secondary corroboration candidate only** until full coverage is measured. Exports are not equivalent to domestic availability. |
| Annual wheat production or trade | Slow-moving supply context and M2/M3 feature. | Available for Ukraine and India over the profiled spans. | **Not a monthly outcome.** Used as lagged model inputs with explicit release timing. |

### Fit of the proposal cases to the selected price series

The Week 4 source-only eligibility profile (`scripts/profile_week4_m0_m4_eligibility.py`, with the single-point weather of the time) applied the Week 3 past-only completeness rules. It reported M0 145 feature-eligible origins (2011-01–2023-01), M1 144 (2011-01–2022-12), M2 132 (2011-02–2022-12), and M3/M4 121 (2011-02–2022-12). With a three-month future window, 119 M4 origins remained through 2022-10; only nine fell within February–October 2022. These are availability counts, not labels or model results. (After the D10 feature corrections the common origin set is 117 months; see §2.7.)

| Proposal example | Fit for the Ukraine price target | Use in evaluation |
|---|---|---|
| 2007–08 | Outside the FAOSTAT price series. | Not scoreable; global context only. |
| 2010/11 | Series begins during the event and lacks multi-year pre-event history. | Descriptive context only. |
| 2022 | Inside the price and feature window, with few complete three-month forward windows. | The final test period (D3); event-level metrics are uncertain, so a rolling-origin backtest on earlier years is reported as well. |

### Avoiding leakage and circular validation

1. Set each event's external evidence, onset, end, and severity independently of the model inputs. Do not choose a date because the target price series spikes there and then describe that as M0 detection success.
2. Keep event context (drought, supply shortfall, export disruption) separate from the outcome label (observed wheat-market stress in the chosen geography). A global event does not automatically imply stress in every domestic market.
3. Keep evaluation chronological: train on earlier years, select thresholds and parameters on a separate validation slice, and report final metrics on later held-out periods. Never tune on the final test.
4. For M2/M3, use only production and trade values that could have been observed by each prediction date. The annual year label is not the public release date.
5. A global event does not guarantee that domestic producer prices rise at the same time. Inspect the farm-gate outcome directly.

The calendar proposed at the end of this work (2010 warm-up; training 2011–2017; validation 2018–2020; test 2021–2022, with three-month purges) was adopted as D3. Its results are in [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md).

## 2.4 India price-source check (exploratory, not used)

The approved proposal names FAO FPMA for food prices, FAOSTAT for production and trade, and NASA POWER for weather. The OGD/AGMARKNET mandi-price resource below was explored when India-specific feasibility was raised; it is not part of the proposal's source stack and was not adopted.

- **Dataset:** Current Daily Price of Various Commodities from Various Markets (Mandi), published by the Directorate of Marketing and Inspection, Department of Agriculture and Farmers Welfare, Government of India, on the [Open Government Data Platform](https://www.data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi) (resource `9ef84268-d588-465a-a308-a864a43d0070`, Government Open Data License – India). Daily, by market; distinct from the Department of Consumer Affairs' national/state wholesale averages and from FAO FPMA prices.
- **Downloaded:** one portal export after the CAPTCHA/details flow, kept as `data/raw/ogd_variety_daily_market_prices_2026-10-31.csv` (1,542,006 bytes; SHA-256 `FE3E99421F2398AD1DDB08C67C0A02DA05CF501EF9C3534B6A5DC07A9427C446`; not in Git). It has 15,409 rows and 10 columns, one arrival date only, and 422 wheat rows with numeric modal prices across 10 states and 320 markets. A format and schema check, not a historical series.
- **Historical access attempts:** the OGD historical-resource preview (Maharashtra, Pune, wheat) reported 14,410 records, but downloading redirected to Google account authorisation and was stopped there; the unfiltered preview reported 82,821,587 records. The OGD API accepted a user-supplied key (never written to the repository) but ignores the `arrival_date` filter, so it returns only the live day. The AGMARKNET 2.0 date-wise endpoint advertises prices from January 2021 but returns `TOKEN_OR_CAPTCHA_REQUIRED`. The CEDA Agmarknet API needs its own token, which was not used.
- **Conclusion:** no historical India mandi series was obtained; India stayed a comparator through the FPMA series only, and the pilot is Ukraine.

## 2.5 Download register

**Workspace:** `data/raw/` holds local acquisitions plus an allowlist of reviewed, attributed Ukraine wheat QCL/TCL, FAOSTAT producer-price, SSSU CPI and NASA POWER snapshots committed for shared work. Other raw acquisitions remain ignored. The Ukraine FPMA response stays local until APK-Inform reuse rights are confirmed. Live APIs and bulk archives can change, so compare SHA-256 hashes before comparing results from different retrievals.

To populate missing inputs without manual portal steps, run `python scripts/fetch_week2_inputs.py`. It uses `configs/week2.local.json`, preserves existing files, extracts only the configured FAOSTAT country, item and elements, and writes a local `data/raw/retrieval_manifest.json` with retrieval times and hashes. FAOSTAT bulk archives are downloaded temporarily and removed after filtering.

To reproduce the farm-gate real-price input, run `python scripts/profile_week4_price_deflator.py`. It chains SSSU monthly CPI changes from the explicit December 2000 = 100 anchor, joins monthly FAOSTAT wheat producer prices, and records input/output hashes and method in the output sidecar. It is the M0 and target price input, not an official IFPA score.

To reproduce the source-only event and feature-eligibility profile, run `python scripts/profile_week4_m0_m4_eligibility.py`. It applies the Week 3 completeness rules to the selected price input and other shared snapshots, without producing labels or performance metrics.

### FAO FPMA price observations

API root: `https://fpma.fao.org/giews/v4/global/price_module/api/v1/`. For a known series, retrieve its metadata and observations by UUID. For example (PowerShell):

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

For each remaining series, use the same `FpmaSerieDomestic` URL to save metadata. If an observation file is paginated, follow its `next` link until null. Recheck coverage and metadata on every refresh. FAO FPMA inclusion does not by itself settle third-party reuse rights.

### FAOSTAT production (QCL), trade (TCL), prices (PP) and derived inputs

| Relative path under `data/raw/` | Retrieval URL / instructions |
|---|---|
| `faostat_qcl_production_bulk_2026-11.zip` | [Official normalised QCL archive](https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip). The acquisition script downloads this to a temporary path only when the configured country extract is missing, filters it, then removes the bulk ZIP. |
| `faostat_tcl_trade_bulk_2026-11.zip` | [Official normalised TCL archive](https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data_(Normalized).zip). Same handling as QCL. |
| `faostat_tcli_trade_indicators_2026-11.zip` | [Official normalised TCLI archive](https://bulks-faostat.fao.org/production/Trade_CropsLivestockIndicators_E_All_Data_(Normalized).zip). Small reference download; derived indicators, not the M3 source. |
| `faostat_ukraine_wheat_production_2026-11.csv` and `.metadata.json` | **Versioned shared snapshot.** From the QCL archive: `Area Code (M49)=804`, `Item Code=15` (Wheat), elements Production, Yield, Area harvested. Keeps `Flag` and `Note`; sidecar records output SHA-256 and attribution. |
| `faostat_india_wheat_production_2026-11.csv` and `.metadata.json` | From the QCL archive: `Area Code (M49)=356`, same item and elements. |
| `faostat_ukraine_wheat_trade_all_measures_2026-11.csv` and `.metadata.json` | **Versioned shared snapshot.** From the TCL archive: `Area Code (M49)=804`, `Item Code=15`, elements Import quantity, Export quantity, Import value, Export value. Keeps units, `Flag` and `Note`; sidecar records output SHA-256 and attribution. |
| `faostat_india_wheat_trade_all_measures_2026-11.csv` and `.metadata.json` | From the TCL archive: same elements, `Area Code (M49)=356`. |
| `faostat_ukraine_wheat_trade_quantity_2026-11.csv` and `.metadata.json`; `faostat_india_wheat_trade_quantity_2026-11.csv` and `.metadata.json` | Earlier quantity-only derivations from the same TCL archive; superseded by the `all_measures` extracts, retained locally. |
| `faostat_ukraine_monthly_wheat_producer_prices_2026-11.csv` and `.metadata.json` | **Versioned shared pilot target/input snapshot.** From the official normalised PP archive: M49 `804`, item `15` (Wheat), element code `5530` (`Producer Price (LCU/tonne)`), monthly rows only; source fields and flags kept. 157 observations from January 2010 through January 2023, complete monthly coverage through December 2022, every row flagged `A` (official value). The dataset catalogue states CC BY 4.0; the PP domain describes its main input as official member-country statistics. Farm-gate producer pricing, not a wholesale or retail measure. Archive: [Prices_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/Prices_E_All_Data_(Normalized).zip). Cite FAO and retain the licence and source metadata when redistributing. |
| `faostat_ukraine_monthly_cpi_2026-11.csv` and `.metadata.json` | **Local candidate snapshot; not versioned; superseded by the SSSU CPI below.** From the FAOSTAT CP archive: M49 `804`, item codes `23012` (General CPI) and `23013` (Food CPI), `Element=Value`. 318 monthly records per item, January 2000–June 2026. All 318 General CPI rows have flag `X` (external organisation); Food CPI has 258 `X`, 55 `I` (imputed) and 5 `A` rows. Archive: [ConsumerPriceIndices_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/ConsumerPriceIndices_E_All_Data_(Normalized).zip). Terms for the external values would need confirming before sharing. |
| `ukrstat_ukraine_monthly_all_items_cpi_2026-11.csv` and `.metadata.json` | **Downloaded, profiled and allowlisted for team sharing.** SSSU SDMX v3 dataflow `DF_PRICE_CHANGE_CONSUMER_GOODS_SERVICE`, series `INDEX_CONSUMPRICE.PREV_MONTH.UA00000000000000000.0.M` (Ukraine, all-items CPI, monthly change from previous month). 422 consecutive non-empty observations from August 1991 through September 2026, including the full January 2010–January 2023 producer-price interval. Download SHA-256: `648b85db57bf918d51f57f0653d9342a809db617d136d0b3d32fb01b53ccc96c`. `scripts/fetch_week2_inputs.py` retrieves and filter-writes it with a sidecar (query, response hash, output hash, dimensions, citation). Ukraine Law 2524-IX Article 27(1) gives users access and the right to use, disseminate and store official state statistics; Article 27(3) requires naming SSSU when publishing or distributing. A month-over-month rate, not an index level; the chained deflator uses an explicit anchor. SSSU metadata notes territorial exclusions for occupied and conflict-affected areas. |
| `data/processed/week4_ukraine_wheat_farmgate_realprice_candidate.csv` and `.metadata.json` | **Shared derived price input.** Built by `scripts/profile_week4_price_deflator.py` from the two versioned inputs above: 157 consecutive months, January 2010–January 2023, SSSU all-items CPI chained with December 2000 = 100. Formula: `real price = nominal FAOSTAT producer price × 100 / chained CPI`. A broad-inflation-adjusted farm-gate series, not a wholesale price, wheat-specific affordability indicator, or IFPA score. The sidecar records input/output hashes, formula, anchor and interpretation limits. The stress label built on it is D1. |
| `data/processed/week4_ukraine_m0_m4_eligibility.csv` and `.metadata.json` | **Shared source-only eligibility profile** from `scripts/profile_week4_m0_m4_eligibility.py`: monthly M0–M4 feature completeness and future price-observation availability for sensitivity horizons. Eligible origins, not labels, episodes or model results. |
| `sssu_ukraine_wheat_harvested_area_by_oblast_2026-11.csv` and `.metadata.json` | **Shared (Week 4 V2).** SSSU SDMX v3 dataflow `DF_AREA_HARVESTS_CROP_YIELD_M` 3.0.1, key `AREA_HARVEST_AGRICULTURAL_CROP.*.C_0040.ALL_AGRICULTURAL_HOLDINGS.*` (harvested wheat area, all holdings, Ukraine + 24 oblasts, thousand ha). Keeps the final-season month per region and year, 2015–2021; oblasts sum exactly to the national total. Retrieved in Week 4 through the project member's browser; sidecar records the request, response hash and territorial note. Used only for fixed weather weights (2015–2020 mean shares). |
| `data/processed/week4_ukraine_weather_wheat_area_weighted_monthly.csv` and `.metadata.json` | **Shared derived input for M1–M4 (Week 4 V2).** NASA POWER Daily API (AG, UTC, 2001–2022) at one centroid per wheat oblast (24 points from geoBoundaries ADM1), aggregated with the Week 2 monthly rules and weighted by the SSSU area shares above. The sidecar lists every request URL and raw-response SHA-256. Regenerate with `python scripts/fetch_week4_weighted_weather.py` (raw daily files cache under ignored `data/raw/nasa_power_oblast_daily/`). One grid cell per oblast, not every wheat field. |

FAOSTAT statistical data are provided under CC BY 4.0 with attribution; FAO terms warn that some datasets may have third-party exceptions, so recheck source metadata before changing or redistributing a refresh ([FAO database terms](https://www.fao.org/contact-us/terms/db-terms-of-use/en)). Use the catalogue [datasets_E.json](https://bulks-faostat.fao.org/production/datasets_E.json) to check archive names and update dates. The versioned Ukraine extracts were retrieved in Week 1 and are the shared working snapshot; their sidecars include row counts, filters, attribution and SHA-256. Bulk archives are not stored in Git. Future refreshes create a new snapshot and may differ; the archives do not contain historical publication vintages.

The CP domain metadata notes that some CPI gaps may be statistically estimated. The `X` values in the FAOSTAT Ukraine CPI extract are attributed to an external organisation, so the FAO database's general CC BY statement does not by itself confirm the underlying third-party terms; the SSSU primary source is used instead. See [SSSU API documentation](https://stat.gov.ua/en/development-api/sdmx-api-v3), [Ukraine Official Statistics Law 2524-IX](https://zakon.rada.gov.ua/laws/show/2524-20), the [official CP metadata](https://files-faostat.fao.org/production/CP/CP_e.pdf), and [FAO database terms](https://www.fao.org/contact-us/terms/db-terms-of-use/en).

The PP catalogue reports a [CC BY 4.0 licence](https://data.fao.org/catalog/dataset/47c17894-8ca1-4bd0-ba4d-6078e919e9b1). The extracted Ukraine wheat monthly PP observations are all marked `A` (official value), and no PP-specific third-party restriction was identified. In contrast, FPMA wheat observations are provider-partner data from APK-Inform with no specific redistribution permission identified; those rows and price-derived outputs stay out of the shared repository.

### Weather, crop geography and India mandi reference

| Relative path under `data/raw/` | Retrieval URL / instructions |
|---|---|
| `nasa_power_ukraine_central_daily_2001_2022.json` and `.metadata.json` | **Versioned shared snapshot.** [NASA POWER Daily API request](https://power.larc.nasa.gov/api/temporal/daily/point?parameters=PRECTOTCORR%2CT2M_MAX%2CT2M_MIN&community=AG&longitude=32.0&latitude=49.0&start=20010101&end=20221231&format=JSON&time-standard=UTC). The sidecar records the full request, SHA-256 and citation. Single point; kept only as a sensitivity check (D5). |
| `nasa_power_haryana_daily_2022.json` | NASA POWER Daily API, AG community, daily 2022 Haryana point sample. Exact request URL and coordinates were not retained, so this file is not fully reproducible from the local record alone. |
| `ukraine_state_stats_crop_monthly_area_yield_2025-11.zip` | [SSSU dataset page](https://stat.gov.ua/en/datasets/areas-gross-harvests-and-crop-yield-monthly); direct [archive](https://stat.gov.ua/sites/default/files/2025-11/ovuzp_sg.zip) (9,860,183 bytes). 2011–2021 folders with PDF/XLS bulletins. Superseded for weights by the SSSU SDMX extract above. |
| `ogd_variety_daily_market_prices_2026-10-31.csv` | [OGD India resource page](https://www.data.gov.in/resource/variety-wise-daily-market-prices-data-commodity), resource UUID `9ef84268-d588-465a-a308-a864a43d0070`. Manually downloaded through the portal after the CAPTCHA/details flow; no stable direct CSV URL. A single arrival date, not a historical series (§2.4). |
| `nasa_power_grid/nasa_power_grid_daily_2001_2022.manifest.json` | **D9-B gridded weather (Week 5).** [NASA POWER Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/), AG community, one point request per native grid cell (232 cells, 2001–2022). Every request URL and raw-response SHA-256 is in the committed manifest; the daily CSVs stay local. Outputs in `data/processed/d9b_grid_weather/`. |

### Ukraine monthly trade candidate (exploratory; not accepted)

| Relative path under `data/raw/` | Retrieval URL / findings |
|---|---|
| `ukraine_monthly_trade_commodity_structure_2025-12.zip` and `.metadata.json` | SSSU [external-trade dataset page](https://stat.gov.ua/en/datasets/external-trade-goods-monthly-quarterly-operational-data); direct [ZIP](https://stat.gov.ua/sites/default/files/2025-12/shchomisyachni_obsyahy_tovarnoyi_struktury_zovnishnoyi_torhivli_za_utochnenymy_richnymy.zip). 2005–2016 annual structure files and monthly commodity-structure files for 2017–2023. The 2022 files have the cereals aggregate (HS chapter 10) in million USD and no wheat-specific HS 1001 measure; a footnote excludes Crimea, Sevastopol and parts of occupied Donetsk and Luhansk. It does not meet the wheat-specific quantity requirement. |

The separate [data.gov.ua trade dataset](https://data.gov.ua/dataset/491d1176-5300-45c1-a9bb-0b207528e04e) states CC BY attribution terms; that does not establish that this exact SSSU ZIP is covered by the same licence.

### UN Comtrade monthly wheat exports (secondary corroboration candidate)

Public query template (Ukraine exports, wheat and meslin HS 1001, monthly, partner World):

`https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=YYYYMM`

The [UN Comtrade API guide](https://uncomtrade.org/docs/un-comtrade-api/) says anonymous preview is rate-limited, capped at 500 records and one period per call. The [free Basic Individual tier](https://uncomtrade.org/docs/subscriptions/) requires a free account and enables UI downloads and API requests (up to 100,000 records per call and 500 calls per day); bulk files need premium access. These are published limits at the time of checking.

| Checked period | Result in the Comtrade preview | Net weight | Query URL |
|---|---|---:|---|
| 2007-01 | No records found | — | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=200701) |
| 2008-12 | No records found | — | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=200812) |
| 2010-07 | One row; trade value USD 36,467,016; quantity field 0/N/A | 224,629,689 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=201007) |
| 2011-06 | One row; trade value USD 238,539,532; quantity field 0/N/A | 806,272,743 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=201106) |
| 2022-03 | One row; trade value USD 93,284,637 | 306,530,915 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=202203) |
| 2022-12 | One row; trade value USD 336,778,737 | 1,642,993,073 kg | [query](https://comtradeplus.un.org/TradeFlow?AggregateBy=none&BreakdownMode=plus&CommodityCodes=1001&Flows=X&Frequency=M&Partners=0&Reporters=804&period=202212) |

This confirms a usable wheat-specific `Net Weight (kg)` field in 2010–11 and 2022 (the separate quantity field is missing or zero in some rows). It does not confirm monthly continuity, and the 2007/2008 boundary months have no records, so the 2007–08 event cannot be scored from it. No Comtrade series file was downloaded.

### Evidence links added in Weeks 4 and 5

Machine-readable request URLs and response hashes are also in each file's `.metadata.json` sidecar.

| Purpose | Source and link | Where it is used |
|---|---|---|
| Oblast wheat harvested area (weather weights) | SSSU: [Areas, gross harvests and crop yield (monthly)](https://stat.gov.ua/en/datasets/areas-gross-harvests-and-crop-yield-monthly). SDMX v3 CSV request: `https://stat.gov.ua/sdmx/workspaces/default:integration/registry/sdmx/3.0/data/dataflow/SSSU/DF_AREA_HARVESTS_CROP_YIELD_M/3.0.1/AREA_HARVEST_AGRICULTURAL_CROP.*.C_0040.ALL_AGRICULTURAL_HOLDINGS.*?dimensionAtObservation=TIME_PERIOD` | `data/raw/sssu_ukraine_wheat_harvested_area_by_oblast_2026-11.csv` |
| Oblast centroids (weather points) | geoBoundaries gbOpen UKR ADM1: [GeoJSON download](https://media.githubusercontent.com/media/wmgeolab/geoBoundaries/main/releaseData/gbOpen/UKR/ADM1/geoBoundaries-UKR-ADM1.geojson) (SHA-256 `b2f9f4ad2e9be5888523219c0bb46a24332ead16b46305409098048fd361a2d6`); project page [geoboundaries.org](https://www.geoboundaries.org/). Centroids from Shapely `polygon.centroid`, or `representative_point()` where the centroid falls outside the polygon (Odesa). | `configs/week4_weather_oblasts.json` |
| Daily weather per oblast | [NASA POWER Daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/); all 24 request URLs are in the weighted-weather sidecar | `data/processed/week4_ukraine_weather_wheat_area_weighted_monthly.csv` |
| D9-A full FAOSTAT archives (retrieved in Week 5 through the project member's browser; not in Git) | [Production_Crops_Livestock_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip), [Trade_CropsLivestock_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data_(Normalized).zip), [Prices_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/Prices_E_All_Data_(Normalized).zip); names and update dates in the [catalogue](https://bulks-faostat.fao.org/production/datasets_E.json). Archive SHA-256 values are in the profile. | `data/processed/d9a_faostat_bulk/bulk_profile.json`, [`6_SCALABILITY.md`](6_SCALABILITY.md) §6.4 |
| D9-B gridded daily weather (Week 5) | NASA POWER Daily API, one request per native grid cell (232 cells, 2001–2022) | `data/raw/nasa_power_grid/nasa_power_grid_daily_2001_2022.manifest.json`, `data/processed/d9b_grid_weather/` |
| Annual-data release timing (D6) | FAO: [Production statistics 2010–2023, release 19 Dec 2024](https://www.fao.org/statistics/events/events-detail/agricultural-production-statistics-2010-2023/); [Production statistics 2000–2022, dated 27 Dec 2023](https://www.fao.org/statistics/highlights-archive/highlights-detail/agricultural-production-statistics-(2000-2022)); [Production statistics 2000–2020, dated 01/04/2022](https://www.fao.org/statistics/highlights-archive/highlights-detail/New-FAOSTAT-data-release-Agricultural-production-statistics-(2000-2020)/); [Trade of agricultural commodities 2005–2022, Dec 2023](https://www.fao.org/3/cc9206en/cc9206en.pdf); [Trade of agricultural commodities 2000–2020, 2022](https://openknowledge.fao.org/items/92e85c13-6e9d-476c-a000-b925825b1fa8); Internet Archive copy of the [FAOSTAT bulk catalogue, 28 Dec 2018](https://web.archive.org/web/20181228021557id_/http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.xml) (Production updated 2018-12-20, Trade 2018-06-27) | [`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.3 |

### Capture requirements for future downloads

For every new raw file, save a sidecar or update this register with: retrieval UTC date, exact request URL (secrets removed), method and filters, source and licence notes, response or file size and optionally checksum, API pagination or auth/CAPTCHA steps, and known limits. Never store API keys, CAPTCHA values, cookies, or account credentials in the repository. Keep raw responses unchanged; write derived extracts as separate files and document their filter recipe.

## 2.6 Synthetic data

Synthetic data is used only for pipeline development, the labelled demo and the scaling benchmarks (D7). It is never used in results, labels or coverage claims.

### Week 3 development fixture

The generator creates a small Ukraine wheat panel calibrated from distribution summaries of the local FPMA, NASA POWER, FAOSTAT QCL and FAOSTAT TCL files. It generates new values; it does not copy rows from those sources. It let the Week 3 features be developed without waiting for outstanding source, rights or coverage decisions.

Every generated row has `is_synthetic=true`; output metadata names the calibration inputs, seed, ranges, lags, splits and synthetic scenario. The two Week 3 fixture files under `data/synthetic/week3_ukraine/` are versioned for team development; other synthetic outputs are ignored. Synthetic data stays separate from `data/raw/` and real outputs.

```powershell
python scripts\generate_synthetic_week3_fixture.py
```

The script uses only Python's standard library and the source files in `data/raw/`. It writes `data/synthetic/week3_ukraine/ukraine_wheat_monthly_synthetic.csv` and its `.metadata.json`. The fixture has 96 monthly rows (2015-01 through 2022-12), price columns, point-weather monthly columns, and annual production and trade features with `feature_year = forecast_year - 2`, available on 1 January of the forecast year.

`demo_split` is a code-development split: 2015 warm-up, 2016–2020 training, 2021 validation, 2022 test. Three marked `SIM-*` scenarios create synthetic weather anomalies followed by synthetic price shocks. The `synthetic_demo_future_price_stress` column uses a demo-only rule (a 15% real-price increase within the next three months), not the project's label. The final three labels are blank because the full future horizon is not present. Recipe and seed: `configs/synthetic_week3.local.json`.

### Week 4 synthetic showcase

`python scripts/generate_week4_synthetic_showcase.py` builds 500 synthetic versions of the real 2021–2022 test window and scores them with the frozen Week 4 M0 model. Recipe and seed: `configs/synthetic_week4_showcase.local.json`. It mirrors the real window's structure: the same 20 eligible origins, features, label rule, model and alert cutoff. Prices come from a calendar-aligned half-year block bootstrap of 2011–2020 real-price returns, starting from the real December 2020 price; no real price after December 2020 is read, so the real test labels stayed unopened. Outputs in `data/synthetic/week4_showcase/`: one typical showcase path (real 2020 history rows marked `is_synthetic=false`, synthetic rows `true`), per-scenario metrics and a metadata sidecar. The bootstrap reuses training-period behaviour and contains no 2022-style war shock, so its metrics are not FoodShield results.

### Synthetic scaling workloads

The batch scaling series multiplies the real panel into labelled synthetic workloads (up to 186 million rows); see [`6_SCALABILITY.md`](6_SCALABILITY.md). They measure engineering throughput only.

### Safe use

- Use synthetic data to exercise Spark joins, schema handling, temporal cutoffs, M0–M4 feature code and model and report interfaces.
- Keep `is_synthetic`, `demo_split`, `synthetic_event_id` and `synthetic_demo_future_price_stress` out of predictor columns.
- Do not report accuracy, lead time, recall, false alarms, data coverage or model performance from synthetic data as FoodShield results, and do not combine synthetic rows with real observations in results.

## 2.7 Data quality, joins and as-of checks (Week 7)

**Scripts:** `scripts/run_week7_data_quality.py` → `data/processed/week7_data_quality/report.json` and `feature_completeness_by_year.csv`; tests in `tests/` (`python -m unittest discover -s tests -v`).

### Source coverage

| Input | Coverage | Missing values | Quality flags |
|---|---|---:|---|
| Real farm-gate wheat price (FAOSTAT PP ÷ SSSU CPI) | 2010-01 to 2023-01, 157 months, no duplicates | 0 | All 157 producer prices flag `A` (official); CPI release 14.10.2026 |
| Wheat-area-weighted weather (24 oblast points, D5) | 2001-01 to 2022-12, 264 months | 0 | Full area weight covered in every month |
| Gridded daily weather (D9-B, 232 cells) | 2001-01-01 to 2022-12-31, 1,864,120 rows | 0 | Every request URL and raw SHA-256 in the manifest |
| FAOSTAT production (QCL), Ukraine wheat | 1992–2024, 99 rows (3 measures) | 0 | All flag `A` since 2005 |
| FAOSTAT trade (TCL), Ukraine wheat | 1992–2024, 132 rows (4 measures) | 0 | All flag `A` since 2005 |
| SSSU wheat harvested area by region | 2015–2021, 175 rows, 25 regions | 0 | No observation flags; weights use 2015–2020 (D5) |

The full FAOSTAT archives reproduce the Ukraine extracts value for value ([`6_SCALABILITY.md`](6_SCALABILITY.md) §6.4), so the extracts are a faithful slice of the official release.

### Feature completeness and joins (2010–2022, 156 months)

The panel joins the monthly price (the driving table) with the monthly weather and the annual values available at each month. Every price month from 2010 to 2022 finds its weather month and its annual data year; no join leaves a gap.

| Feature group | Months missing | Why |
|---|---:|---|
| Price change month on month (M0) | 1 (2010-01) | No earlier month in the series |
| Price 12-month anomaly (M0) and the M4 interaction | 12 (all of 2010) | Needs 12 earlier months |
| Weather climatology anomalies (M1) | 0 | Weather from 2001 gives at least 5 prior years from 2006 on |
| Production and trade year-on-year changes (M2, M3) | 0 | Data years 1992 on; the 2-year lag (D6) always has two years available |

So every stage is complete in every month from 2011-01 (`feature_completeness_by_year.csv`). The common origin set is 117 months (2011-01 to 2020-09): every M0–M4 feature is present and the 3-month outcome ends by 2020-12. This matches the Week 5 ablation exactly.

### Temporal (as-of) checks

| Check | How | Result |
|---|---|---|
| No feature uses later data | Replay all 1,864,486 records in availability order. For each of 156 months, compare the features computed when the month closes with the features recomputed after everything has arrived (`tests/test_integration.py`). | **Identical**, exactly, in every month and feature |
| Streaming features = batch features | All 13 corrected features from the streaming monitor against the Spark batch pipeline the frozen models were trained on, every month | **Identical** (tolerance 1×10⁻⁹) |
| Annual availability lag (D6) | Every annual value becomes available on 1 January two years after its data year | Holds for all 210 annual records |
| Labels stay before the test | Backtest labels need the 3-month outcome to end by 2020-12; the last labelled origin is 2020-09 | Holds |
| Frozen protocol unchanged, test opened once | SHA-256 of `configs/week5_frozen_protocol.json` starts `c6fc95aa`; `OPENED.json` present | Holds (`tests/test_core.py`) |

**Fast unit tests (17, all passing, under 1 second, in `tests/test_core.py`):**

- labels, including the exact-threshold and missing-future cases and the holdout guard;
- price features;
- confusion counts and the F1 cutoff;
- the streaming monitor: month closing, duplicate-day protection, weight renormalisation, climatology window, bounded year-on-year change, annual lag, scoring, state round-trip;
- replay availability rules and the frozen-protocol check;
- the dashboard's live view (it follows the newest replay run).

### Support for the M0–M4 comparison

| Split | Months | Months with stress ahead |
|---|---:|---:|
| Train 2011–2017 (purged) | 81 | 23 |
| Validation 2018–2020 (purged) | 33 | 10 |
| Backtest 2014–2020 | 81 | 24 |
| Test 2021–2022 (opened once) | 22 | 5 |

Stress months ahead by year (common origins): 2011 3 · 2012 3 · 2013 3 · 2014 6 · 2015 5 · 2016 3 · 2017 0 · 2018 2 · 2019 2 · 2020 6.

Every stage is scored on exactly the same months, so differences between stages come from the added signals alone. The support is thin: 8 backtest episodes and 3 test episodes ([`4_MODELS_AND_RESULTS.md`](4_MODELS_AND_RESULTS.md) §4.7).

### Known data limitations

- **Annual release timing.** FAO release dates are known only for recent data years, so D6 is an approximation; the 3-year sensitivity run covers it.
- **Revisions.** FAOSTAT and SSSU revise past values, and the archives are updated in place. Recorded SHA-256 hashes identify the exact release used.
- **Fixed area weights.** Regional wheat area is only available from 2015, so weights are held at 2015–2020 shares. Within-oblast cropland is not mapped, although the 232-cell grid agrees closely with the 24 points.
- **2022.** Official statistics exclude occupied territories from March 2022; NASA POWER weather is unaffected.
- **Same source for features and outcome.** The outcome comes from the same price series as the M0 features, strictly after each origin.
