# FoodShield decisions, assumptions, and open questions

This log separates requirements established by the submitted proposal and TA feedback from proposed implementation choices. Update it when the team makes a decision; retain the rationale and date.

## Confirmed requirements

| Decision | Basis |
|---|---|
| Research wheat supply stress and whether multi-source temporal signals give earlier warning than price-only monitoring. | Submitted proposal PDF and updated DOCX. |
| Do not claim prediction of geopolitical events or their causes. | Submitted proposal problem definition. |
| Use a country–commodity–month analytical grain as the working alignment. | Submitted proposal's example grain and data model. |
| Compare M0 price-only, M1 + weather, M2 + production, M3 + trade, M4 all signals plus temporal/lag/cross-signal features. | Submitted proposal and supplied project context. |
| Use Spark DataFrames/SQL as the primary distributed processing approach; use historical validation, ablation, data-quality and scalability evaluation. | Submitted proposal. |
| Add local Kafka replay of historical/processed records through Spark Structured Streaming and produce timely stress alerts. | TA feedback. No live feed required. |
| Establish numeric throughput and latency targets and report actual outcomes only after measurement. | TA feedback. The TA did not prescribe exact values. |
| Work within Windows 11, 8 GB RAM, and prefer D: drive for data; no cloud resources or paid billing without explicit approval. | Team working constraints. |
| Preserve proposal/presentation files and archive; no relocation or overwrite. | Team working constraints. |

## Proposed engineering choices (not yet team-approved)

| Proposal | Rationale / boundary |
|---|---|
| Use bronze/silver/gold Parquet layers for local data organization. | Fits the requested ETL stages and technology direction; define schemas and retention after profiling. |
| Start local streaming performance targets at ≥100 events/s sustained for 10 minutes, ≥250 events/s for 60 seconds, and p95 end-to-end alert latency ≤10 seconds. | Laptop-conscious targets proposed in `docs/PLAN.md`; measure in a fixed workload before treating as commitment. These are not TA-prescribed and are not achieved results. |
| Measure replay completeness by reconciling valid input records with processed accounting; do not promise exactly-once behavior. | Avoids expanding the task into delivery semantics beyond the TA request. |
| Use local Spark mode and a single-node Kafka setup for the initial replay demonstration. | Lightweight starting point for an 8 GB laptop; exact runtime/deployment method remains open. |
| Keep source code in a `src/foodshield/` package with configs, scripts, tests, local data, and generated artifacts separated. | Recommended project organization; it keeps implementation, local datasets, and outputs distinct. |

## Assumptions to validate

- The candidate FAO food-price, FAOSTAT production/trade, and NASA POWER data can support a common wheat/country/month analysis with adequate overlap. This is not established yet.
- Historical event periods and usable event labels can be defined consistently from documented evidence and available data.
- Kafka and Spark Structured Streaming can run together within laptop memory limits for a bounded replay workload.
- Proposed numeric streaming targets are appropriate after a baseline measurement on the actual laptop.

## Unresolved questions

1. Which countries or regions have sufficient overlapping price, weather, production, and trade coverage, and which will be selected?
2. Which exact dataset editions/files, access methods, and licensing/attribution terms will be used?
3. What operational definition, onset date, and severity criteria will define each historical stress event and a valid warning?
4. What event-time and publication/availability lag rules will apply to annual production and trade records?
5. Which detector/thresholding method will be used for M0–M4, and how will thresholds be selected without holdout leakage?
6. What replay event schema, ordering, pacing, checkpoint/output behavior, and duplicate/restart semantics will the streaming demo support?
7. What Kafka local runtime is practical on the Windows laptop (for example, native process or container), given memory and setup constraints?
8. After a small benchmark run, should the initial 100/250 events/s targets be retained or adjusted with a documented rationale?
9. Which dashboard technology and final output format best fit the remaining time and laptop constraints?

## Source materials reviewed

- `Dataflux - FoodShield_Project_Proposal.pdf` (submitted proposal)
- `FoodShield_Project_Proposal_Updated_2.docx` (updated proposal; same central scope)
- `Revised with TA comments.docx` (streaming replay and target feedback)
- `FoodShield_Proposal_Discussion_Deck_v5.pptx` (scope context, candidate events/sources, and architecture)
- Earlier proposal and deck versions under `archive/` (historical context; preserved)

The current discussion deck describes streaming as an extension/stretch item, while the later TA feedback makes local replay a required project improvement. The working plan incorporates replay while retaining the proposal's core batch research and M0–M4 comparisons.
