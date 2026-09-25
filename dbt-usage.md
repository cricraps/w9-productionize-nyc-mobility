# Using dbt 

This is a documentation of how the team explored the usage of dbt in the existing pipeline, what was tested, the verdict, and the recommendation. 

## Verdict

**Not now, and not for everything.** The team has decided to keep the pipeline we have. dbt is solid at building and testing tables, but it can't pull data in, and rewriting every notebook into SQL costs more right now than it gives back. It needs more exploration, and was agreed to work better once we're building new reporting tables on top of the clean layer.

## What was tested

dbt was pointed at two of our four sources. It reproduced the same numbers as the current pipeline and ran its data-quality checks automatically — 17 of 17 passing. It can't ingest data on its own, so adopting it fully would mean running two tools side by side or rewriting the ingestion layer too.

| | |
|---|---|
| **Replaces** | The clean and check steps only — not scraping, not file loads |
| **Stays the same** | The data, the warehouse, the dashboard — only how clean tables get built changes |

## The proof

| Test | Result | What it means |
|---|---|---|
| Did the scrape run? | WARN, correctly — schedule was paused, data was a day old | dbt correctly flags stale data |
| Build everything | 17/17 checks passed, ~25 seconds | Every table built, every check passed |
| Run it again | 66 rows, 66 current, 66 total sightings — twice | Re-running doesn't create duplicates |
| Compare with our notebook | 66 of 66 rows match, 0 differences | Same answer as the pipeline we already trust |
| Docs and lineage | Generated straight from the project files | Free map of where every table comes from |

Full run screenshots are in the DBT subtab, section 11 of the write-up.

## Gains vs. costs

| Gains | Costs |
|---|---|
| Checks run on every build | Can't collect data — still need a second tool for that |
| Build order derived automatically from the code | Rewriting existing logic from Python to SQL |
| One lineage map across all tables | Our one-row-per-check summary table has no dbt equivalent yet |
| Private dev schema per engineer | Setup overhead — connection, token, GitHub app per person |

## Recommendation

| | |
|---|---|
| **Now** | Keep Databricks Jobs for collect, clean, and check. Keep this dbt branch as a working reference. |
| **Later** | Build new reporting tables in dbt off the clean layer, run as a step inside the Databricks job. |
| **Skip** | Moving data collection into dbt — it isn't built for that. |

## Project structure

```
nyc_mobility_dbt/
├── analyses/
├── macros/
├── models/
├── seeds/
├── snapshots/
├── tests/
├── dbt_project.yml
└── .gitignore
```
