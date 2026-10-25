# FoodShield development conventions

Read `docs/PROJECT_BRIEF.md`, `docs/PLAN.md`, and `docs/DECISIONS.md` before changing project code or data contracts.

## Scope and evidence

- Preserve the approved wheat supply stress objective, the M0–M4 model progression, and the TA requirement for local Kafka replay with Spark Structured Streaming.
- Do not silently change project objectives or methodology. Keep confirmed requirements, assumptions, and proposed engineering choices distinct.
- Do not claim data availability, completed work, benchmark outcomes, or model performance without evidence. Label benchmark values as targets until measured.
- Profile source coverage, quality, licensing, identifiers, units, and time ranges before relying on a dataset. Apply explicit availability lags to annual signals to avoid leakage.

## Implementation practices

- Use Python and the agreed Spark DataFrame/Spark SQL direction. Keep transformations reproducible and make schemas, data grain, and temporal rules explicit.
- Prefer small, bounded local runs suitable for Windows 11 and 8 GB RAM. Keep datasets on D: where practical; avoid unnecessary copies and memory-heavy operations.
- Preserve all submitted proposal, presentation, and archive materials. Never overwrite, move, or delete them.
- Do not create cloud resources or incur paid service costs without explicit approval.
- Make incremental, reviewable changes and use targeted checks for affected behavior. Avoid unrelated refactoring and dependency additions.

## Project layout

Follow the recommended source, configuration, data, and artifact layout in `docs/PROJECT_BRIEF.md`. Keep generated data and outputs separate from source code and documentation.
