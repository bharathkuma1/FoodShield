# Week 8: report and presentation plan

**Format required:** one report per team, 6–8 A4 pages, 10-point font, 2.5 cm margins, PDF, with title, authors and email addresses (`COURSE_SUBMISSION_REQUIREMENTS.md`). The source text is the manuscript `PROJECT_SUBMISSION.md` (v0.2). This plan fixes the page budget, figures and tables, so the final PDF is assembled rather than written from scratch.

**Graded on:** novelty, technical effort, technical correctness, scalability, impact.

**Status (Week 8):** both are drafted as shared Claude artifacts, so the team can comment and edit in place:

- **Report draft** (Claude doc): abstract and Sections 1–8, with an architecture diagram, the F1 chart and the shuffle-partition chart. It exports to PDF or Word. The four streaming cells now hold the laptop runs.
- **Slide deck** (Claude slides): the 12 slides below, each with speaker notes (about 11 minutes in total) and the demo script on slide 11. It exports to PowerPoint or PDF.

The links are in the project chat; share them from each artifact's Share menu before the joint review.

## 1. Report: page budget (target 7.5 pages; about 3,600 words plus 6 figures and 4 tables)

| § | Section (rubric item) | Pages | Content | Figures and tables (existing files) |
|---|---|---:|---|---|
| — | Title, authors, emails, abstract | 0.4 | Question, approach, the two-sided result, scale numbers | — |
| 1 | Introduction and goals (*problem and goals*) | 0.6 | Why wheat price stress; research question; design goals (leakage-safe, reproducible, Spark batch + Kafka streaming); scalability goals and targets | — |
| 2 | Data and engineering design (*approach*) | 1.4 | Sources and terms; layered Spark pipeline; as-of rules (D6, replay availability); D9 data at full size; Parquet layout | Architecture diagram (Mermaid source in `ARCHITECTURE.md`); Table: sources and volumes |
| 3 | Methods (*approach*) | 1.0 | Label (D1/D2); split and backtest (D3); M0–M4 with one estimator (D4); corrected features (D10); frozen protocol and one-time test (D8) | Table: feature sets per stage |
| 4 | Model results (*evaluation*) | 1.6 | Backtest vs test; episodes and lead time; threshold trade-off; probability quality; robustness; label sensitivity | `figures/week7_model_f1_intervals.png`, `figures/week7_model_lead_times.png`, Table: headline (from `WEEK7_MODEL_EVALUATION.md` §1) |
| 5 | Scalability (*evaluation*) | 1.3 | Batch size series; one factor at a time; CSV vs Parquet; streaming replay correctness, throughput and latency against targets | `figures/week7_batch_size_scaling.png`, `figures/week7_batch_resources.png`, streaming chart from §2, Table: streaming runs |
| 6 | Discussion and limitations | 0.6 | One country, 11 episodes; same source for features and outcome; 2022 context; single-machine scaling | — |
| 7 | Summary and future work (*summary*) | 0.4 | Assessment against the goals; multi-country panel (D13); live feeds | — |
| — | References | 0.2 | FAOSTAT, SSSU, NASA POWER, geoBoundaries, Spark, Kafka | — |

**Writing rules carried over from the project:**

- measured values only, with targets labelled as targets;
- synthetic data only in the labelled scaling benchmark;
- test results stated as a single pre-registered opening;
- no causal claims.

**Still needed before the PDF:**

- [ ] Team email addresses for the heading.
- [x] Laptop Week 6 numbers: in the report (abstract, Section 5.2, Section 8) and on slides 10–12.
- [ ] Final team read-through (joint review, D14).
- [ ] Export: A4, 10 pt, 2.5 cm margins. The Docs or PDF tooling can produce this from the final text.

## 2. Presentation: 12 slides, about 12 minutes plus live demo

| # | Slide | Message | Visual |
|---:|---|---|---|
| 1 | Title | FoodShield: can multi-signal data warn of wheat price stress earlier? | Team names |
| 2 | Problem | Price spikes hurt; does weather/production/trade add warning value over prices? | Price timeline (dashboard section 1) |
| 3 | Goals | Leakage-safe models; Spark at real data size; Kafka → Structured Streaming alerts | Three goal tiles |
| 4 | Data | Four public sources; full FAOSTAT archives (22.7 M rows); 232-cell weather (1.86 M daily rows) | Sources and volumes table |
| 5 | Pipeline | Layered Spark batch path and the streaming path | Architecture diagram |
| 6 | Method | Label, split, M0–M4 ladder, one estimator, frozen protocol, test opened once | Timeline of splits |
| 7 | Result 1 | Backtest: added signals did not help | F1 intervals (blue) |
| 8 | Result 2 | Test: M4 beat M0 with half the false alarms, so too few events to decide | F1 intervals (orange), lead times |
| 9 | Batch scaling | 186 M rows in 43 s; shuffle partitions matter most; Parquet up to 14× faster | Size and resource charts |
| 10 | Streaming | 1.86 M records replayed; exact-once processing of unique records; p95 under 2 s at the target rates | Streaming table |
| 11 | **Live demo** | Double-click `RUN_DEMO_LIVE.bat`: the dashboard opens on its Streaming tab and the replay runs (2022 window at 1,000 events/s); each 2022 alert pops up as Spark raises it; then the report's checks | Browser (live dashboard) + terminal |
| 12 | Conclusions | What holds, what doesn't, next steps (multi-country panel) | Three takeaways |

**Demo command (about 4 minutes on 2 cores).** The 250 events/s run from earlier drafts takes about 8 minutes: the 2016–2021 fast-forward is about 100 s, and 2022 at 250/s is 340 s. So the demo sends 2022 at 1,000 events/s, four times the burst target:

```powershell
py -3.12 scripts\run_week6_replay.py --name demo --work-root C:\fs6 --replay-from 2016-01-01 --measure-from 2022-01-01 --rate 1000
```

**Demo script** (also in slide 11's speaker notes):

| Time | On screen | Say |
|---|---|---|
| Before | Project folder open in File Explorer; sound on if you want the alert chime. The replay clears `C:\fs6\demo` itself. | — |
| 0:00 | Double-click `RUN_DEMO_LIVE.bat` | The dashboard opens on its Streaming tab. One command starts a local Kafka broker, creates the topics and launches the Spark Structured Streaming consumer. |
| 0:30 | Producer output | Records go out in the order they would have become known: daily weather for 232 grid cells, monthly prices, annual statistics two years late. 2016–2021 goes fast so the features have history. |
| ~1:45 | Paced window starts | All of 2022, about 85,000 records, at 1,000 events/s. Each month closes when all 232 cells and the price have arrived; the frozen M0 and M4 then score it. |
| meanwhile | Dashboard, Streaming tab | Months appear as Spark scores them, with score, alert line, latency and the main reasons. From January 2022 each stress alert pops up on screen, on any tab; click once on the page first so the chime can play. |
| ~3:30 | Printed report | Achieved rate; alert latency p50 and p95; the checks block (every unique record processed once, alerts equal the offline replay, M0 equals the one-time test), all true. M4 agrees with the frozen test on 20 of 22 months because this replay starts in 2016; that is expected and not a check. |
| If it fails | Dashboard, Streaming tab, "Replay a saved run" (the recorded laptop demo, with the same pop-ups) | Say so plainly; play the recorded run and walk through the same fields. |

The timings are from the 2-core workspace (the producer took 185 s for this run there); time the laptop at the rehearsal.

**Rehearsal check:** the demo needs the laptop prerequisites (Java 17+, Hadoop native files) and one rehearsal run beforehand.
