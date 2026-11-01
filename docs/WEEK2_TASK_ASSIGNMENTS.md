# Week 2 team assignments

**Status:** Agreed owner mapping, carried forward from the Week 1 workstreams.
**Scope:** Local Spark Bronze/Silver/Gold ETL for the Ukraine wheat pilot. This documents implementation evidence and handoff status; it does not change the proposal scope or claim that every source is cleared for publication.

## Owners and deliverables

Completion percentages measure whether the Week 2 implementation deliverable is present and inspectable in this workspace. They do not measure individual effort or attendance. The team agreed to carry forward the Week 1 owners for Week 2.

| Teammate | Status (% implementation documented) | Week 2 responsibility and evidence | Remaining boundary | Acceptance criteria for 100% |
|---|---:|---|---|---|
| **Bharath Kumar** | **100%** | FPMA price Bronze/Silver transforms and Gold M0 monthly price output. The local run contains 804 Bronze and 804 Silver price rows and 310 Gold monthly rows. Source-series identifier, price currency/unit, grain, and restriction are documented in `docs/WEEK2_ETL.md`. | FPMA underlying source reuse rights and metadata mismatch remain a release gate; do not redistribute source values. | Reproducible FPMA read and typed outputs; declared keys and grain pass checks; monthly M0 table and source limitations are inspectable. |
| **Abhijith C R** | **100%** | FAOSTAT QCL production Bronze/Silver transform, retaining units, flags, and notes, with the configured provisional two-year availability lag. The run contains 99 rows in each layer. | The two-year lag is an explicit proxy, not observed publication timing or historical vintage evidence. | Reproducible QCL read; typed output retains source metadata and availability fields; declared key checks pass; lag assumption is documented. |
| **Jyoti Prakash Das** | **100%** | NASA POWER daily weather Bronze/Silver screening transform, sentinel filtering, and monthly screening aggregation. The run contains 24,105 daily rows and 264 monthly rows in each applicable layer. | This remains a single-point screening sample, not a crop-weighted or nationally representative wheat-weather signal. | Reproducible daily and monthly outputs; `-999` sentinel excluded from Silver; units and spatial basis retained; monthly aggregation and limitation are documented. |
| **Naman Singh Shekhawat** | **100%** | FAOSTAT TCL trade Bronze/Silver transform, retaining units, flags, and notes. The run contains 132 rows in each layer. | Current extract is reporter aggregate and has no partner dimension. Week 2 does not encode event windows as validated binary labels. | Reproducible TCL read; declared grain and key checks pass; retained metadata and partner/event-label limitations are documented. |

## Shared Week 2 completion evidence

- Runner: `scripts/run_week2_etl.py`; transformation code: `src/foodshield/etl/week2.py`; local configuration: `configs/week2.local.json`.
- Implementation notes and reproducible setup: `docs/WEEK2_ETL.md`.
- Profile and schemas: `data/processed/week2_ukraine/profile.json` (generated output, locally ignored).
- Successful local run wrote Bronze, Silver, and Gold Parquet outputs under `data/processed/week2_ukraine/`. It checks non-empty tables, null key values, and duplicate keys at declared grains. Raw inputs under `data/raw/` are left unchanged.
- Counts are from the recorded pilot run: Bronze prices 804, production 99, trade 132, daily weather 24,105; Silver prices 804, production 99, trade 132, daily weather 24,105, monthly weather 264; Gold M0 monthly prices 310.
- These are implementation/run facts for the local Ukraine wheat extracts. They do not establish national representativeness, source rights, model performance, individual teammate sign-off, or general support for arbitrary country/commodity substitutions.

## Week 2 handoff to Week 3

Before feature engineering, review and record team acceptance of this owner mapping and contracts, then address the following as Week 3 inputs:

1. Define the country–commodity–month join keys and report coverage/completeness for each signal.
2. Review missing months and duplicate-resolution behavior; do not fill absent observations with zero without a source-based rule.
3. Validate the annual `feature_available_from` rule against release/vintage evidence or retain it as a sensitivity assumption.
4. Keep the weather sample labeled as point screening until crop-area weighting and historical coverage are established.
5. Carry forward FPMA reuse restrictions and the separate event-context versus validated-target distinction.
6. Add targeted tests/contracts for schemas, temporal availability, and joins as the Week 3 feature pipeline is implemented.

The Week 2 code and successful local run satisfy the documented pipeline milestone. The follow-up items are downstream validation and integration work, not uncompleted Week 2 source-transform deliverables.
