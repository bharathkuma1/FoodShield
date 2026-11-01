# Week 3 team assignments

**Status:** Agreed default owner mapping based on the Week 1/2 signal workstreams; revise it when a change is explicitly called out.
**Scope:** Spark analytics and temporal feature engineering at the proposal's country–commodity–month grain. Week 3 produces reusable feature tables and quality evidence; it does not define or claim the final stress label, train a detector, or report model performance.

## Shared objective

Build the common monthly analytical panel from Week 2 Silver datasets, preserving country, commodity, month, source provenance, units, and feature availability. Implement and document the proposed signal progression as far as source coverage permits: price-only M0 inputs; weather M1 additions; production M2 additions; trade M3 additions; and M4 temporal/lagged/cross-signal features. Report coverage and missingness at every join. Keep the point-weather and annual-lag limitations visible. Development fixtures may exercise the pipeline, but synthetic data must remain marked and excluded from historical evaluation.

## Agreed default owners and deliverables

| Teammate | Status | Proposed Week 3 workstream | Acceptance evidence |
|---|---:|---|---|
| **Bharath Kumar** | **0%** | Price/M0 feature contract: establish the monthly country–commodity–time key; retain price basis, currency, unit, and provenance; derive price lags, rolling summaries, and normalized/anomaly candidates using only values available through each feature month. | Documented schema and feature formulas; bounded Spark output; hand-reviewed dates show no future price rows enter a feature. |
| **Abhijith C R** | **0%** | Production signal: align annual wheat production, yield, and harvested area to monthly rows; retain flags/units; implement the provisional availability rule (`data_year <= forecast_year - 2`) and expose feature year/availability date. | Coverage and join report; preserved source metadata; hand-reviewed as-of examples; assumption clearly distinguished from verified publication vintages. |
| **Jyoti Prakash Das** | **0%** | Weather signal: document monthly aggregation from daily data and spatial basis; implement available monthly weather summaries and lag/anomaly candidates; keep screening-point results distinct from any later representative weighted geography. | Parameter/unit and aggregation definitions; month coverage/missingness; sample aggregation checked against daily inputs; no national/crop-weighted claim from the point sample. |
| **Naman Singh Shekhawat** | **0%** | Trade signal and integration: align aggregate wheat import/export quantities and values with explicit annual availability; preserve flags/units; produce join completeness and missingness summaries across the panel and signal stages. | Trade feature schema and lag treatment documented; M0–M3 row/coverage comparison; key uniqueness and missingness summaries; no event labels inferred from trade alone. |

## Shared deliverables and acceptance boundary

- A reproducible Spark DataFrame/Spark SQL feature build with a documented country–commodity–month key and explicit schemas.
- Feature definitions for lagged values, rolling statistics, anomalies, and normalized indicators, including window direction and minimum-history behavior.
- Temporal rules that prohibit future rows or unavailable annual observations from entering a prediction-month feature.
- Join cardinality, completeness, missingness, and coverage summaries for M0 through each available signal stage.
- A bounded local run and a small hand-reviewed sample. If using synthetic fixtures, preserve `is_synthetic` and exclude those rows from real-data claims and final evaluation.

Week 3 does not settle the observed outcome, forecast horizon, alert threshold, final event labels, or historical train/validation/test periods. Those are Week 4 evaluation-protocol decisions and must be based on verified source rights, coverage, and availability timing. The planned feature workspace is `src/foodshield/features/`; the actual implementation status should be updated when work is present and reviewed.
