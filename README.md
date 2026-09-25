# Productionizing NYC Mobility Pipeline

We created this mirror cloned repo carrying over the previous history of Week 8 to show how the pipeline has underwent changes of versioning and deploying, orchestrating, recovering, monitoring, and governing.  

For architecture, the data model, and how to run the full pipeline end to end, see the main [`README.md`](../README.md); this file is the production layer on top of it.

## The checklist

| Requirement | Answered in |
|---|---|
| Version & Deploy — show a change moving through Change → Test → Commit → Push → Deploy → Run → Verify | [§1](#1-version--deploy) |
| Orchestrate — show how the pipeline should execute (Ingest → Bronze → Silver → DQ → Gold) | [§2](#2-orchestrate) |
| Recover — what happens when a task fails, and can it be safely rerun | [§3](#3-recover) |
| Monitor — execution, failures, freshness, data quality | [§4](#4-monitor) |
| Govern — naming conventions, ownership, permissions/access, lineage, key DQ rules | [§5](#5-govern) |


---

## 1. Version & Deploy

**The cycle:** `Change → Test → Commit → Push → Deploy → Run → Verify`, applied per dataset and per layer so a single bad change can't take down the whole pipeline.

**Branching.** One feature branch per dataset-and-layer, all merging through a shared `testing` branch before `main`:

| Branch | Scope |
|---|---|
| `feature/raw-green-taxi-production` | Green taxi, raw layer |
| `feature/clean-green-taxi-production` | Green taxi, clean layer |
| `feature/raw-taxi-zones-production` | Taxi zones, raw layer |
| `feature/clean-taxi-zones-production` | Taxi zones, clean layer |
| `feature/raw-weather` | Weather, raw layer |
| `feature/clean-weather-production` | Weather, clean layer |
| `feature/traffic-advisory-production` | Traffic advisory — raw, clean, validation, and the scheduled job (new source this week) |
| `testing` | Integration branch. Every feature branch merges here first |
| `main` | Production. Only reached via reviewed PR |

**Deploy path:**
1. **Change** — work happens on the dataset's feature branch, in the owner's own Databricks Git folder.
2. **Test** — notebook run in isolation against a scoped date/month range; idempotency re-run before moving on.
3. **Commit** — small, scoped commits 
4. **Push** — to the feature branch, then **merge into `testing` first** — this is what surfaces conflicts between datasets before they reach production.
5. **Deploy** — once `testing` is green, a **PR is opened from the feature branch to `main`** (not from `testing`). `testing` is never merged into `main` directly — it's a proving ground, not a source.
6. **Run** — the notebook or job executes against `nyc_mobility`.
7. **Verify** — row counts, idempotency re-run, and the relevant validation table all confirm the change is safe before the PR is approved.


---

## 2. Orchestrate

**Pipeline order, all sources:** `Ingest → Bronze (raw) → Silver (clean) → DQ (validation) → Gold (mart)`, each stage reading only from the one before it — unchanged from the Week 8 shape, now hardened per layer (see [§6](#6-what-changed-since-week-8)).

**Scheduling is source-specific, not blanket.** Only one of the four sources gets an automated daily job:

| Source | Storage shape | Scheduled? | Why |
|---|---|---|---|
| Green taxi | Static monthly Parquet | No — run on demand | Historical files don't change once published |
| Taxi zones | Static single CSV | No — run on demand | Reference data, rarely changes |
| Weather | REST API with history | No — run on demand | Open-Meteo's archive means a missed day can always be re-fetched later |
| **Traffic advisory** | **Live HTML page, current week only, no archive** | **Yes — daily job** | **A day not scraped is gone forever — this is the only source where missing a run causes permanent data loss** |

**The `traffic_advisory_daily` job** (job id `359007123675808`, defined in [`resources/traffic_advisory_daily.job.yml`](../resources/traffic_advisory_daily.job.yml), versioned in the repo rather than click-configured):

- **Three tasks, strictly ordered:** `raw → clean → validation`, each depending on the one before, so validation never runs against a clean table that failed to build.
- **Trigger:** daily at 01:00 Asia/Manila (`0 0 1 * * ?`). Currently **paused** — the definition exists and is versioned so it can be resumed without rebuilding anything; runs are triggered on demand from the job page in the meantime.
- **Retries:** 2 retries with backoff on the raw task, to absorb a longer `nyc.gov` outage (the notebook's own fetch already retries 3 times for short blips).
- **Alerting:** email on task failure.
- **Smoke test:** run `709584871288042` on 23 Sep went green end to end — raw 1m 26s, clean 29s, validation 25s, 184 rows written across the three layers.

The other three sources are run manually in the order listed in the main README's [Execution table](../README.md#execution) — no job is needed until they, too, gain a hard freshness requirement.

---

## 3. Recover

*What happens when a task fails, and can it be safely rerun?* Answered per dataset.


| Dataset | Failure mode | What happens | Rerun-safe? |
|---|---|---|---|
| **Green taxi** | A requested month's file is missing | Notebook prints "skipped, file not found" and continues — doesn't crash the whole run | Yes |
| **Taxi zones (raw)** | Wrong or unexpected file picked up | File discovery now filters to known extensions and requires exactly one match, instead of blindly taking whatever the OS lists first | Yes |
| **Taxi zones (raw)** | Empty file or a renamed/reordered column | Row count + schema assertion catches it before the write | Yes |
| **Weather (raw)** | Same file loaded again | Switched from `overwrite` to a Delta `MERGE` keyed on `observation_time + latitude + longitude` — matches update in place, non-matches insert | Yes |
| **Traffic advisory** | A day missed entirely | `days_behind` > 1 on the freshness query | **No** — restore the schedule and record the gap; nothing recovers a day that was never scraped |

**Common thread:** every raw loader now fails loudly (an assertion or a raised error) rather than silently writing a broken or empty result. That single design rule is what makes "can I safely rerun this" a yes for almost every failure mode above.

For full documentation on this: [recover](docs/recover.md)


---

## 4. Monitor

**Traffic advisory** is the only source with a running scheduled job, so it's the only one with full job-level monitoring today:

- **Execution** — trigger (daily), task order (raw → clean → validation), and per-task status/duration/retries, visible on the job page.
- **Failures** — email alert on task failure, with fetch failures (nyc.gov unreachable) kept distinct from parse failures (page structure changed) so an investigation starts in the right place.
- **Freshness, two separate signals** (conflating them wastes an investigation — they look identical in the data):
  - *Ours* — `days_behind` from `MAX(scrape_date)` vs. today. Above 1 means we're losing data.
  - *Theirs* — `MAX(advisory_week_start)` in clean. Stale here with a current scrape just means DOT hasn't republished; nothing is lost, and the Type 2 table already handles a repeated advisory by updating `last_seen_scrape` instead of duplicating it.
- **Data quality** — 10 checks writing one row each to `validation.traffic_advisory_validation`. `PASS` at zero failures, `WARN` under 5%, `FAIL` at or above — except checks marked *strict* (duplicate keys, null business keys, an advisory missing from clean), which go straight to `FAIL` on any occurrence regardless of percentage, because those specifically break downstream joins.

**Green taxi, taxi zones, and weather** are monitored at the notebook level rather than the job level, since they aren't on a schedule yet: every raw and clean load now carries its own row-count assertion, schema check, and (for green taxi and weather) an explicit idempotency check that must pass before the run is considered good. There's no dashboard or alert for these three yet — that's the natural next step once any of them gains a hard freshness requirement.

---

## 5. Govern

**Naming** — unchanged from Week 8 and still enforced: `nyc_mobility` catalog, layer-named schemas (`raw`, `clean`, `mart`, `validation`), same table name across layers, `snake_case` columns with `_bk` (business key) / `_sk` (surrogate key) / `is_` (boolean) prefixes/suffixes where relevant (e.g. `closure_bk`, `advisory_sk`, `is_current` on traffic advisory).

**Ownership** — per dataset, See: [Who owns what](../README.md#7-collaboration)

**Permissions** — the pattern established (pipeline job: write on raw/clean/validation; engineers: read on raw, read+write on clean, read on validation; analysts: read on clean and validation only) is the target model for every source. Raw is never hand-edited for any dataset — it's the only untouched record of what a source said at ingestion time, and for traffic advisory specifically, once it's wrong there's nothing to re-fetch it from.

**Lineage** — same shape across sources, source → landing volume → raw → clean → (mart/validation):
```
nyc.gov weektraf.shtml
  → ftw-b12-r2 volume, weektraf_YYYY-MM-DD.html
    → nyc_mobility.raw.traffic_advisory        (MERGE on advisory_sk)
      → nyc_mobility.clean.traffic_advisory     (Type 2 MERGE on closure_bk)
        → validation.traffic_advisory_validation → dq_visualization
        → dim_advisory → Q4
```
Green taxi, taxi zones, and weather follow the equivalent raw → clean → mart path documented in the main README's [architecture diagram](../README.md#2-architecture-and-data-flow).

**Key data-quality rules that would block a release**, one set per dataset. The same principle underlies all four — fail loud, never merge or ship bad data over good history — and pairs with the rerun behavior in [§3](#3-recover).

*Green taxi*
1. No pickup/dropoff timestamp from before 2026 — corrupted-year records (`pre2026_timestamp_corruption`) fail outright rather than being scored by percentage.
2. Dropoff must never be earlier than pickup (`dropoff_before_pickup`) — breaks duration math and the date-key join downstream.
3. `ehail_fee` must be null in every row — the one case where a 100%-null column is the *expected* state; anything else means the source layout changed.

*Taxi zones*
1. `location_id` must be non-null and the correct type in every row — it's the primary key everything downstream joins on, and it's never allowed to fail.
2. Duplicate zone *names* are expected and allowed (large neighborhoods legitimately share a name) — the rule is join on `LocationID`, never on name.
3. `Unknown` / `N/A` borough and service-zone sentinels are retained by design, not dropped — surfaced as warnings so they stay visible rather than silently disappearing.

*Weather*
1. `latitude`/`longitude`/`observation_time` must be non-null and form a unique grain (`duplicate_grain`) — this triple is the merge key in [§3](#3-recover); a duplicate here breaks the raw `MERGE`'s idempotency the same way a duplicate `advisory_sk` would for traffic advisory.
2. Physical-range checks are non-negotiable, not just warnings: `temperature_2m` within −90 °C to 60 °C, `latitude`/`longitude` within ±90/±180, `elevation` within −500 m to 9000 m — a value outside these ranges means a parsing or unit bug, not real weather.
3. `observation_time` must never be in the future — a timeliness check that catches a clock or load bug the same way traffic advisory's `scrape_date` check does.

*Traffic advisory*
1. A zero-row scrape must fail the load, never merge an empty result over good history.
2. `closure_bk` must be unique and non-null in clean — it's the join key for Q4.
3. `times_seen` must never exceed the number of scrapes raw holds; if it does, the merge lost its idempotency and the history can't be trusted.


## Group F ##
Barroga . Crapatanta . Legaspi . Marquez . Orogo . Tolentino
