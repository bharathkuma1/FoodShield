# Week 1 team assignments

**Status:** Agreed workstream ownership
**Scope:** Week 1 source feasibility and event-evidence review. These assignments do not by themselves approve a country, source, event label, or model contract.

## Owners and deliverables

### Progress status

Completion percentages measure completion of the documented Week 1 feasibility handoff in this workspace, not individual performance or teammate attendance. The source choices and engineering rules below are working decisions recorded at the user's direction in Week 1; they are not represented as a vote or personal sign-off from each teammate. Source/legal limitations and the tasks they create for later implementation are recorded separately.

| Teammate | Status (% complete) | Completed | Still open | Acceptance criteria for 100% |
|---|---:|---|---|---|
| **Bharath Kumar** | **100%** | Compared price candidates; selected Ukraine FPMA national monthly wheat-grain series as provisional M0, retained India as comparator, documented metadata mismatch, source provenance, and a no-redistribution-until-rights-confirmed restriction. | Before public release: confirm underlying APK-Inform rights and clarify its market/series methodology; revise source only if rights cannot be secured. | **Week 1 complete:** candidate comparison, series ID, M0 recommendation, metadata limitation, attribution, and safe rights disposition are documented. The remaining permission check is a release gate, not missing feasibility work. |
| **Abhijith C R** | **100%** | India and Ukraine QCL extracts and metadata are local; years, units, record counts, and flags profiled; a conservative annual availability rule selected. | In implementation, replace the proxy lag with verified publication dates or vintage snapshots if obtainable; calculate as-of coverage and preserve flags. | **Week 1 complete:** QCL source/filters/coverage and the explicit `data_year <= forecast_year - 2` monthly feature rule are documented as a conservative assumption, not a claimed release date. |
| **Jyoti Prakash Das** | **100%** | NASA POWER parameters, units, period, time standard, sentinels, and exploratory point limitation profiled; official crop-statistics archive and its 2011–2021 annual folders documented. | Before claiming representative M1 results, extract/validate regional wheat-area tables and create weighted grids for supported years. Do not claim 2007–2010 crop weights from this archive; compare M1+ models only on a common supported period or source historical weights. | **Week 1 complete:** selected a staged spatial plan—NASA daily grid over wheat-producing regions, weighted by validated official regional wheat area where available; no national/crop-weighted signal is claimed yet. Coverage gap and M1+ validation boundary are explicit. |
| **Naman Singh Shekhawat** | **100%** | Ukraine and India TCL quantity/value extracts are local with coverage and flags; bilateral mismatch risks and cited event chronology documented; aggregate trade chosen. | Before predictive metric reporting, define and source an independent observed-stress target; keep the three documented episodes as holdout contexts until then. | **Week 1 complete:** aggregate M3 measures selected; bilateral matrix deemed out of scope; broad event windows and an independent-target rule documented. These periods are not mislabeled as validated binary outcomes. |

The detailed path-to-source register is [DATA_SOURCE_REGISTER.md](DATA_SOURCE_REGISTER.md). It includes retrieval URLs or API requests for current raw downloads, filters for derived extracts, and known provenance gaps. **All four Week 1 feasibility workstreams are now 100% complete as a documented handoff.** Their remaining conditions above belong to later implementation, validation, or publication gates; this status does not claim teammate review or team ratification.

### Progress evidence and current local files

- Ukraine trade: `data/raw/faostat_ukraine_wheat_trade_all_measures_2026-11.csv` (132 rows; four measures, 33 years each, 1992–2024; quantity in t, value in 1000 USD; flags A/X/I retained) and metadata sidecar.
- India trade comparison: `data/raw/faostat_india_wheat_trade_all_measures_2026-11.csv` (256 rows; four measures, 64 years each, 1961–2024; flags A/X retained) and metadata sidecar.
- Ukraine crop statistics for weather weighting exploration: `data/raw/ukraine_state_stats_crop_monthly_area_yield_2025-11.zip` (official archive, 9,860,183 bytes; monthly bulletins in PDF/XLS formats packaged by year). Archive presence is verified; its tables and suitability for a consistent wheat-area panel are not yet profiled. The archive lists 2011–2021, so earlier event-period area weights are not established.
- Candidate event windows are in `docs/WEEK1_EVENT_CASES.md`; they are research windows, not approved target labels.

| Teammate | Owned workstream | Week 1 deliverable | Current local starting point |
|---|---|---|---|
| **Bharath Kumar** | Price source and price-only baseline feasibility | Compare FPMA wheat-grain and wheat-flour series for the shortlisted countries. Record source/market, product, price type, units, frequency, time range, gaps/duplicates, nominal/real field, identifiers, source attribution, and any metadata inconsistencies. Recommend an M0 price series only after the comparison. | `data/raw/fpma_ukraine_national_wheat_grain_wholesale.json` and metadata; India national and state series; Ethiopia, Pakistan, Bangladesh, and Egypt comparison series. |
| **Abhijith C R** | FAOSTAT production (M2) | Profile wheat production, yield, and harvested area by candidate country. Preserve flags/notes and units; record years, update/retrieval dates, missingness, and a defensible publication/availability-lag proposal for monthly features. | Filtered India and Ukraine QCL CSVs and metadata sidecars in `data/raw/`; normalized QCL source archive retained locally. |
| **Jyoti Prakash Das** | NASA POWER weather (M1) | Check parameter definitions/units, temporal coverage, time standard, missing/sentinel values, and monthly aggregation. Recommend a wheat-area-based spatial strategy; distinguish exploratory point samples from representative country exposure. | Haryana 2022 and central Ukraine 2001–2022 point samples in `data/raw/`; neither is crop-weighted or nationally representative. Regional crop bulletin archive downloaded, but only lists 2011–2021 and its tables are not yet profiled. |
| **Naman Singh Shekhawat** | FAOSTAT trade (M3) and historical event evidence | Profile wheat import/export quantity and value choices, reporter/partner dimensions, flags/notes, coverage, and partner discrepancy risks. Build a cited event chronology for the proposal's 2007–08, 2010/11, and 2022 cases, then propose candidate windows and independent stress criteria. | Filtered India and Ukraine TCL quantity-and-value CSVs and sidecars in `data/raw/`; normalized TCL archive retained locally; candidate cases in `docs/WEEK1_EVENT_CASES.md`. |

## Shared profile fields

For each source and candidate country, record:

- dataset/series and version or update date; access and retrieval date;
- source URL and source attribution/reuse terms;
- country, market, commodity, item/element codes, and geographic identifiers;
- data grain and frequency, unit/currency, start/end dates, and missing/duplicate records;
- quality flags, notes, revisions, and known source limitations;
- annual signal availability date or an explicit conservative lag before monthly alignment;
- a small cross-checkable example and the path to the local raw/filtered data.

Keep source facts, assumptions, and proposed engineering rules distinct. Preserve the raw files; generated filters and outputs stay separate from code and proposal materials. Do not assume an event-period record was available to a model at that time merely because the data row carries that year.

## Week 1 handoff

Owners contribute their profile to the shared feasibility notes, then compare country–month coverage across price, weather, production, trade, and the candidate event periods. The handoff should record:

1. which source series can be joined at the project grain and which cannot;
2. country/event coverage and limitations, including product or market mismatches;
3. annual-signal availability lags and weather spatial/monthly aggregation assumptions;
4. source licensing/attribution questions;
5. a documented recommendation for the country/source shortlist and candidate event definitions.

Working Week 1 choice is Ukraine as the three-case event-study pilot and India as the 2022 comparator. This records the user's direction to settle feasibility choices; it does not assert that teammates separately reviewed or ratified it. Source rights, representative weather, and a monthly predictive target remain later-stage gates as documented above.
