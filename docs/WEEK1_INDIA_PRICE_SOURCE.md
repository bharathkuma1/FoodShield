# Week 1 India wheat-price source check

**Checked:** 1 November 2026 (Asia/Kolkata)
**Status:** Exploratory India-specific alternative only; one-day extract profiled; not a selected proposal source.

## Scope note

The approved proposal names FAO FPMA for food prices, FAOSTAT for crop production and agrifood trade, and NASA POWER for weather. This OGD/AGMARKNET mandi-price resource was explored after India-specific feasibility was raised. It is not part of the named proposal source stack and should not replace FAO FPMA unless profiling shows the proposal source is unsuitable and the team explicitly agrees to the substitution. India is a candidate geography to assess after cross-source profiling, not a preselected requirement.

## Candidate source

- **Dataset:** Current Daily Price of Various Commodities from Various Markets (Mandi)
- **Publisher:** Directorate of Marketing and Inspection (DMI), Department of Agriculture and Farmers Welfare, Government of India
- **Portal:** [Open Government Data Platform India resource](https://www.data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi)
- **Dataset catalog:** [OGD catalog entry](https://www.data.gov.in/catalog/current-daily-price-various-commodities-various-markets-mandi)
- **Resource last updated on the portal:** 31 October 2026
- **Available formats shown:** CSV, XML, JSON
- **Granularity:** Daily, by market
- **Data domain:** Mandi market prices, including wheat. These are distinct from the Department of Consumer Affairs' monitored national/state wholesale averages and from FAO FPMA prices.
- **License shown by OGD:** Government Open Data License - India; check the dataset's terms and source attribution before redistribution.
- **Resource API label:** Variety-wise Daily Market Price (resource ID `9ef84268-d588-465a-a308-a864a43d0070`)

## Access and download result

The CSV was found in the user's Downloads folder after they completed the portal's CAPTCHA/details flow. Two copies were present and had the same size and SHA-256; one untouched copy was placed in the project's ignored raw-data folder:

- **Original download(s):** `C:\Users\kbbha.KBBHARATHKUMAR\Downloads\9ef84268-d588-465a-a308-a864a43d0070.csv` and `9ef84268-d588-465a-a308-a864a43d0070 (1).csv`
- **Downloads-folder modified times:** 1 November 2026 at 00:20:38 and 00:25:10 IST
- **Project copy:** `data/raw/ogd_variety_daily_market_prices_2026-10-31.csv`
- **Retrieved/portal-updated date:** 31 October 2026 (portal's latest data date)
- **File size:** 1,542,006 bytes
- **SHA-256:** `FE3E99421F2398AD1DDB08C67C0A02DA05CF501EF9C3534B6A5DC07A9427C446`
- **Duplicate check:** the two Downloads copies have identical SHA-256; only one project copy is retained.

No credentials were entered by the assistant. The raw project copy is ignored by Git under the existing `data/raw/` rule.

The Department of Consumer Affairs' [PMS report menu](https://fcainfoweb.nic.in/Reports/Report_Menu_web.aspx) separately offers Price Report, Average/Month End Report, and Variation Report options, but its page requires a CAPTCHA before report generation. The [daily summary](https://fcainfoweb.nic.in/Reports/DB/DBprices_W.aspx) is public and includes all-India wheat wholesale prices with comparison dates, but it is not a historical daily dataset download.

## Feasibility observations

- India has a plausible monthly wholesale wheat price baseline. FAO GIEWS' [India country brief](https://cb.apps.fao.org/country.jsp?code=IND) publishes national-average monthly wholesale wheat prices and discusses recent price movements.
- The downloaded CSV has 15,409 data rows and 10 columns: `State`, `District`, `Market`, `Commodity`, `Variety`, `Grade`, `Arrival_Date`, `Min_x0020_Price`, `Max_x0020_Price`, and `Modal_x0020_Price`. It contains one `Arrival_Date` only: 31/10/2026. There are 422 wheat rows with numeric modal prices, covering 10 states and 320 distinct market names.
- Wheat rows have numeric `Min_x0020_Price`, `Max_x0020_Price`, and `Modal_x0020_Price`; values span 2,260–5,700. The dataset/API metadata should be checked for the price unit before using these numbers in analysis.
- This is a successful format and schema check, but it does **not** establish historical coverage and is not yet sufficient for monthly M0. A time series or month-end export is still needed. The market-day grain requires explicit aggregation to country–commodity–month or a consistent market panel.
- A future dataset profile should record price type, market, date, unit, source revision, missingness, and how multiple markets/days are aggregated. Do not combine mandi prices, DoCA wholesale averages, FPMA retail prices, and wheat-flour/atta prices as if they were one measure.

## Next access step

### Historical-resource preview and export

The OGD [Variety-wise Daily Market Prices Data of Commodity](https://www.data.gov.in/resource/variety-wise-daily-market-prices-data-commodity) page was inspected as a historical export path. With **State = Maharashtra**, **District = Pune**, and **Commodity = Wheat** (date bounds left blank), **Preview & Download** reported 14,410 matching records and displayed 10 rows. The preview included dates from 3 September through 26 October 2026. This is only a portal preview: the full available date range, completeness, and export size have not been verified.

Selecting **Download** in that preview redirected to Google account authorization. The assistant stopped before account selection or consent. No historical CSV was downloaded in that attempt.

On 1 November 2026, the OGD sourced-webservice page was retried. Its unfiltered preview reported **82,821,587 records** (first 10 shown, beginning in 2009). The page's direct CSV control did not produce a download during the retry. No new file appeared in Downloads. Because this unfiltered export is extremely large and the filtered 14,410-row preview is not currently exposed on that page, no full export was initiated.

After the user created a Janparichay account, the OGD login handoff succeeded in the active browser session: the OGD portal displayed its signed-in dashboard state. The resource's API tab offers **Generate API Key**; no key was created. Authentication is therefore working, but it has not exposed a filtered file export on the resource page or changed the 82,821,587-row unfiltered preview.

The user subsequently supplied an OGD API key. It was used only for bounded read-only requests to the official OGD resource API and was not written to this document. The API accepted it. `filters[state]=Maharashtra` plus `filters[commodity]=Wheat` returned 55 records; the sampled records were dated 31/10/2026. Adding `filters[district]=Pune` returned zero for that date. An attempted old-date filter also returned zero, but `arrival_date` is not listed among the API's exposed filter fields, so this does not establish that historical data is unavailable. This OGD API test confirms access to the live daily feed, not a historical wheat series.

The `arrival_date` filter was then tested directly with `filters[state]=Maharashtra`, `filters[commodity]=Wheat`, and `filters[arrival_date]=03/09/2026`, using a two-row limit. The API returned the same total (55) and records dated 31/10/2026 as the query without an arrival-date filter. The date parameter is therefore ignored/not supported by this resource API; changing it does not retrieve historical dates.

## Historical access paths to verify

- The official [AGMARKNET 2.0 Price Trend Reports](https://agmarknet.gov.in/pricetrendmainmenu) include a **State/UT-wise Wholesale Prices Monthly Analysis** report. Its report form requests commodity, year, and month and does not show a CAPTCHA on the form page. This may support monthly benchmarking, but a wheat result and full coverage range have not yet been verified.
- The official AGMARKNET 2.0 [filter service](https://api.agmarknet.gov.in/v1/daily-price-arrival/filters) lists Wheat (commodity ID `1`), Maharashtra (state ID `20`), and Pune (district ID `364`). Its filter metadata advertises selectable **price** dates from 1 January 2021 through 31 October 2026, with a one-year window per query. A direct request to the date-wise price endpoint returned `TOKEN_OR_CAPTCHA_REQUIRED`; the endpoint needs the portal's CAPTCHA key/value before returning data. This is the strongest verified historical route so far, but a successful wheat response and export are still pending.
- The [CEDA Agmarknet API documentation](https://api.ceda.ashoka.edu.in/documentation/) documents a `POST /agmarknet/prices` endpoint with `commodity_id`, `state_id`, `district_id`, `market_id`, `from_date`, and `to_date`. Its response documentation includes a 401 for a missing or invalid API token. No CEDA token was supplied or used; the OGD key is for the separate OGD API and must not be reused there.
- The next feasibility check should use the official AGMARKNET monthly report for a small wheat sample or have the user complete the CAPTCHA on the date-wise report, then retrieve a bounded period and profile its dates, grain, aggregation, price unit, and missingness. Do not treat the OGD live API sample as a historical series.

### Clear navigation steps

1. Open the [OGD variety-wise daily market prices resource](https://www.data.gov.in/resource/variety-wise-daily-market-prices-data-commodity).
2. Set **State: Maharashtra**, **District: Pune**, and **Commodity: Wheat**. Leave date bounds blank to request the portal's available range, or set a bounded period for a smaller export.
3. Select **Preview & Download**, then select **Download** in the preview.
4. If Google account authorization appears, complete the browser sign-in/authorization yourself. Then allow the CSV export to finish; keep the downloaded original in Downloads or provide it here for profiling and copying to `data/raw/`.
5. Record the exported filename, date range, size, and checksum, then profile schema, row counts, missing dates, price units, and coverage before treating it as a usable time series.

The existing project copy remains a single-day mandi snapshot, not a historical price series. The historical resource is distinct from FAO FPMA domestic prices and the Department of Consumer Affairs' monitored national/state wholesale averages.
