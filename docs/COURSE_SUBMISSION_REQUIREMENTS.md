# DA231o final submission requirements

**Source:** professor's final-project instruction slide shared by the team.

**Purpose:** internal checklist for preparing one consistent team submission. This file tracks course logistics; it is not part of the professor-facing research manuscript.

## Team report

| Requirement | Expected content or format | Current status |
|---|---|---|
| One report per team | Avoid duplicate submissions from team members. | Pending final submission. |
| Length and format | 6–8 A4 pages, 10-point font, 2.5 cm margins, PDF. | Manuscript draft is maintained in `PROJECT_SUBMISSION.md`; final page layout and PDF export remain pending. |
| Heading | Project title, authors, and email address. | Title and author names are present; team email address(es) need to be supplied/confirmed. |
| Problem and goals | Problem definition, motivation, design goals, required features, scalability/performance goals. | Core problem and wheat-only M0–M4 scope are in the manuscript; batch scaling protocol and proposed local replay goals are now explicit. Confirm goals after baseline measurement and review against the proposal before PDF export. |
| Approach and methods | High-level design, architecture/data model, big-data platforms, and ML methods. | Architecture and Spark/Kafka direction are documented; model methods remain subject to Week 4 target/protocol decisions. |
| Evaluation | Experiment design, scalability/performance targets, evaluation metrics, plots, and analysis. | Metrics and proposed targets are documented; batch/streaming measurements and plots remain future evidence. Targets must not be presented as achieved. |
| Summary | Assess progress toward the proposal's design/performance goals and describe future extensions. | Manuscript conclusion exists; add a final evidence-based assessment and concise future-extension discussion after experiments. |

The manuscript keeps local source-extract counts out of its data description. The final evaluation should include only the benchmark workload sizes and measured performance details needed to make scalability results reproducible; those are experimental settings, not claims about the amount or completeness of the source data.

## Other team submissions and participation

| Requirement | Expected content or format | Current status |
|---|---|---|
| Presentation slides | One slide deck per team; avoid duplicate submissions. | Pending. |
| Public GitHub repository link | One link per team; verify public accessibility before submission. | The working branch is shared remotely; public accessibility has not been confirmed. |
| Contribution summary | Each team member submits the MS Form entry describing tasks performed and estimated effort percentage. | The repository records assigned workstream ownership and artifact status; this is not an individual-contribution declaration. Each member must provide their own form response. |
| Demo/presentation session | Use the session-selection MS Form when posted. All team members attend the full session and all presentations in that session. | Session details not yet available. |
| Peer evaluation | Each student completes the circulated evaluation form for presentations in the session. | Pending session and form. |

## Assessment dimensions

Use these dimensions for the final internal review: **novelty, technical effort, technical correctness, scalability, and impact**. The manuscript and demo should provide evidence for each dimension without claiming results that have not been measured.
