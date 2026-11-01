# Week 1 historical event cases: candidate windows and data fit

**Checked:** Week 1
**Status:** Candidate cases only. The proposal's three examples are not fixed requirements. Select final evaluation instances after choosing an explicit outcome definition and confirming source overlap, quality, and usable coverage. They are not country-level ground-truth labels or a binary classification target by themselves.

## Purpose

The proposal uses historical wheat-market stress periods to compare M0–M4 warning timing and quality. The three windows below are examples from proposal development, not mandatory cases. Final instances may be different and should be selected where the required signals and a defensible outcome overlap. Cases are contexts for observable market/supply stress; they are not labels for predicting wars, export bans, or their causes. Define the outcome and onset independently of future model inputs, and apply a strict time cutoff so only earlier data enter each warning.

The discussion deck names 2007–08, 2010, and 2022. FAO primary sources support these as international wheat-market stress episodes:

| Proposal example | Candidate event-study window | Documented context / onset marker |
|---|---|---|
| 2007–08 | 2007-01 through 2008-12; March 2008 is a candidate international peak marker | FAO reports wheat prices rose 118% from January 2007 to March 2008; by October 2008 export prices were nearly 50% below the March peak, while still elevated. This supports the broad candidate window, not a country-level label. |
| 2010 / 2010–11 | 2010-07 through 2011-06 | FAO reports international grain prices surging from July after drought-reduced CIS crops and Russia's export ban; September wheat was 60–80% above July. A separate FAO review describes wheat/maize world prices doubling in 2010 H2 and 2011 H1. |
| 2022 Black Sea disruption | 2022-03 through 2022-12; record February 2022 separately as disruption context, not a full spike month | FAO reported the Cereal Price Index up 17.1% in March versus February and world wheat prices up 19.7% that month, with the Black Sea war as a major driver. FAO also documented that 20% of Ukraine's planted winter-crop area might not be harvested. |

Primary sources: [FAO Food Outlook, November 2007](https://www.fao.org/docrep/fao/010/ah876e/ah876e00.pdf), [FAO Food Outlook, November 2008](https://www.fao.org/4/ai474e/ai474e03.htm), [FAO Crop Prospects and Food Situation, No. 3, September 2010](https://www.fao.org/markets-and-trade/publications/crop-prospects-and-food-situation/7/), [FAO Lessons from the world food crisis of 2006–08](https://www.fao.org/4/i2330e/i2330e.pdf), and [FAO March 2022 food-price report](https://www.fao.org/newsroom/detail/fao-food-price-index-posts-significant-leap-in-march/en).

## Coverage-first case selection

The locally profiled sources show a wide historical overlap for Ukraine's current **input signals**, but not a fully verified independent monthly target. India has strong annual production/trade history, but its current weather file covers only 2022.

| Signal | Ukraine files in `data/raw/` | India files in `data/raw/` | Case-selection implication |
|---|---|---|---|
| FPMA monthly wheat price | 2001-01 to 2026-10; 310 calculated monthly observations, no missing months in span; metadata has a market-type/price-type mismatch and third-party rights open | 2014-01 to 2026-09; 153 monthly observations, no missing months; 2022 is in coverage | Ukraine supports longer retrospective history; India supports 2014 onward. |
| NASA POWER weather | Daily 2001-01-01 to 2022-12-31 at one point (32.0 E, 49.0 N), 8,035 observations per parameter; not crop-area weighted or nationally representative | Daily 2022 only at one Haryana point; not representative of all India wheat area | Using current files alone, the all-input common monthly period is 2001–2022 for Ukraine and 2022 for India, before fixing stricter spatial requirements. |
| FAOSTAT wheat production | Annual 1992–2024; production, yield, harvested area; flags preserved | Annual 1961–2024; same three measures; flags preserved | Adequate time span for both; choose availability lag and preserve flags. |
| FAOSTAT wheat trade | Annual import/export quantity and value 1992–2024; flags preserved | Annual import/export quantity and value 1961–2024; flags preserved | These are structural features, not monthly outcome labels. A two-year lag is only a proxy pending release-vintage evidence. |
| Independent monthly trade outcome candidate | UN Comtrade UI samples: HS 1001 net weight visible in 2010/2011 and 2022; sampled 2007/2008 months return no records; full coverage not checked | 2022-05 query returned no records; full monthly availability not checked | Do not constrain dates to these sparse probes. Profile availability before using trade as an outcome. |

Under the current files and the proposed two-year lag, Ukraine is the stronger starting geography for aligned M0–M4 **feature inputs**, with a provisional 2001–2022 common window. That is not yet a nationally representative weather dataset or proof of a valid label/benchmark. India could become a serious alternative after collecting a consistent wheat-area weather series across more years/states and checking monthly target availability. Choose cases from the final usable intersection, rather than forcing the three proposal examples.

## Outcome options against current coverage

| Candidate outcome | Fit to research question | Coverage/access evidence | Assessment |
|---|---|---|---|
| Future domestic wheat-price stress | Closest measurable version of “emerging wheat-market stress earlier than price-only monitoring.” M0 can use price history through month `t`; M1–M4 use the same cutoff plus other signals; the label describes a price-stress condition after `t`. This is time-disjoint even though price is both a lagged feature and the later outcome. | FPMA Ukraine monthly series has 310 observations from 2001-01 to 2026-10 with no missing months in the span. Current Ukraine weather caps the common input window at 2022-12. India FPMA has 2014-01 to 2026-09, but its local weather extract is 2022 only. | **Preferred primary outcome candidate**, named “future wheat-market price stress,” not physical shortage or complete supply stress. Define real-price/anomaly rule and forecast horizon in advance; fit thresholds on training data only; ensure all features end at or before prediction time. FPMA source reuse rights and metadata mismatch remain gates. |
| Monthly wheat export-flow stress | Provides an external quantity-based signal of cross-border flow disruption; useful as an additional outcome/context in an exporting country. | Ukraine HS 1001 Comtrade preview has net weight in sampled 2010-07, 2011-06, 2022-03, 2022-12; sampled 2007-01/2008-12 have no rows. India 2022-05 query has no row. No full monthly extract or month-completeness profile is available. Anonymous preview permits one period per call; free basic account enables bounded downloads/API use. | **Secondary outcome/corroboration candidate only** until full period coverage and missingness are measured. Exports are not equivalent to domestic availability; zeros, missing reports, trade policy, and transport changes need separate treatment. Do not substitute annual FAOSTAT trade as a monthly label. |
| Annual wheat production or trade | Relevant slow-moving supply context and M2/M3 feature. | QCL/TCL annual values are available for both Ukraine and India across their profiled historical spans. | **Not a monthly outcome.** Retain as lagged model inputs and keep their release timing explicit. |

### Recommended evaluation framing (proposed, not yet team-approved)

Use a future domestic wheat-price-stress outcome as the primary benchmark if rights are cleared and the team approves a threshold/horizon. At prediction time `t`, all M0–M4 inputs must be cut off at `t`; the outcome is assessed only in a later window. Report the result as prediction of **market price stress**, and do not infer physical supply shortages or causation. Evaluate export quantity separately only after acquiring and profiling its full monthly series. Select illustrative case periods from actual target episodes inside the verified common coverage; the original proposal windows can be retained where they fit but are not mandatory. This framing preserves the proposal's price-only comparison while giving the models a measurable forward outcome.

## Country-series feasibility

### Ukraine: strongest provisional match

The downloaded FPMA national-average wholesale wheat-grain series covers January 2001–October 2026 and supplies a complete calculated monthly series. It therefore has price observations for every candidate event period. Descriptive nominal prices show an increase through the 2007–08 and 2010 episodes, but the local Ukraine price series declines from February to December 2022 while international wheat prices rise. This is evidence that the 2022 local response cannot be defined only as a rising price. It may reflect a different local market response during export/logistics disruption; this explanation requires independent evidence and must not be inferred as causal from price data alone.

FAOSTAT filtered Ukraine wheat production and trade data cover 1992–2024. The weather sample at one central point covers 2001–2022. These dates overlap the event windows, but the current weather file is not representative of the wheat area and annual production/trade must be lagged to when it was actually available.

### India: 2022-only alternative

The India national-average raw-wheat wholesale series begins in January 2014, so it covers 2022 but not 2007–08 or 2010. FAO's retrospective on the 2006–08 crisis says domestic wheat prices were relatively stable in India due to trade controls, which further weakens India as a single-country validation case for a global price shock. India remains a useful comparison if the team chooses a 2022-focused pilot.

### Other checked candidates

- Ethiopia's Addis Ababa white-wheat wholesale series covers all event dates but is city-level; its 2010 local price series does not track the international increase in the same way.
- Russia's national-average milling-wheat wholesale series starts in 2005 but ends in February 2022, before the Black Sea event's price response.
- Pakistan, Bangladesh, and Egypt series checked do not span all three cases.

## Avoiding leakage and circular validation

1. Set each event's external evidence, onset, end, and severity independently of the model inputs. Do not choose a date because the same FPMA input series spikes there and then describe that as M0 detection success.
2. Keep event context (for example, drought, supply shortfall, export disruption) separate from an outcome label (observed wheat-market stress in the chosen geography). The deck's global event descriptions do not automatically imply stress in every country's domestic market.
3. Keep evaluation chronological: train/calibrate on earlier years, select thresholds and parameters on a separate validation slice, and report final metrics on later held-out periods/events. The proposal's 2007–08, 2010–11, and 2022 examples are eligible candidates only; a different documented event can be used when it has better verified coverage. Never tune on the final test event.
4. For M2/M3, use only the production/trade values that could have been observed by each prediction date. The annual year label is not the public release date.
5. A downward local price movement can accompany supply/logistics disruption. The 2022 Ukraine price decline is retained as a model-input observation, not used to reject the independently documented global event context or to define a rise-only target.

## Training, test, and comparison plan

Use the first 3–5 years before a candidate test event as an initial training/calibration span, then test on a later held-out event and compare every alert with the observed outcome in that later period. At monthly grain this gives only 36–60 training rows, so keep the first detector simple. If the profiled series supports more consistent pre-event history, prefer using more prior years or rolling-origin backtests instead of discarding useful observations. Reserve a distinct validation slice for threshold/model selection; do not choose thresholds, windows, or parameters after inspecting the final event's outcome.

At each forecast month `t`, restrict all M0–M4 features to observations available by `t`; compare predicted/warning times with an outcome defined strictly after `t`. The primary candidate is future domestic wheat-market price stress (not a claim of physical shortage), contingent on price-source reuse rights. Monthly wheat exports can be an additional corroborating outcome only after full coverage and reporting gaps are profiled. A documented event window may explain context, but it is not a substitute for actual observed outcomes. Report lead time, misses, false alarms, and classification metrics only after the outcome rule and forecast horizon are fixed.

Current Ukraine files make 2022 a plausible held-out candidate after training on earlier common-coverage years, but weather remains a single-point sample and annual values use an unverified release-lag proxy. India has only a 2022 weather extract at present. Neither example is locked as the test set. Remaining gates: resolve FPMA rights/metadata, improve weather geography, confirm annual feature availability, define a future outcome rule/horizon, and profile the full candidate period before adopting any test event. See `docs/DECISIONS.md`.
