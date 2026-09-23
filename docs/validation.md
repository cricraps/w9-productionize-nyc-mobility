# Validation and data quality

One section per source: the rules that run against each set of tables, how they are
recovered when they fail, what is monitored, and who owns the result.

Status values are shared across all sources: PASS at zero failures, WARN under 5 percent,
FAIL at or above. Checks marked strict skip the percentage and go straight to FAIL on any
failure at all.

---

## Traffic advisory

Owner: Kinah, Engineer 4.
Tables: `nyc_mobility.raw.traffic_advisory`, `nyc_mobility.clean.traffic_advisory`,
`nyc_mobility.validation.traffic_advisory_validation`.
Job: `traffic_advisory_daily`, three tasks in order, raw then clean then validation.

### Why this source is scheduled

The DOT page publishes the current week only and keeps no archive. Green taxi is static
parquet, zones are static, weather has history behind the API. This one has none.

Retry works. Rerun works. **Backfill does not exist for this source.** Any day we scraped
can be rebuilt from the landed HTML on `ftw-b12-r2` without re-fetching. Any day we did not
scrape is gone. That is why the load is scheduled daily and alerts on failure rather than
being run by hand.

### Recovery

| Failure | How it shows | Response | Rerun safe |
| --- | --- | --- | --- |
| Timeout or non-200 | Fetch raises after three tries | Retry, nothing was written | Yes |
| Zero rows parsed | Raw load raises before writing | Investigate first, an empty merge over good history is what this guard prevents | After fix |
| Parser regression | Run succeeds, location count drops from 26 to about 11, Validity flags | Fix parser, re-parse from landed HTML, rerun all three | Yes |
| Clean merge fails part way | Task fails | Rerun, Delta MERGE is atomic | Yes |
| Same scrape loaded twice | Nothing visible, by design | None | Yes |
| A day missed entirely | `days_behind` above 1 | Restore the schedule, record the gap, nothing recovers the day | No |

#### Why a rerun is safe

`advisory_sk` is SHA-256 over the entry fields plus `scrape_date`, and the raw load is a
MERGE with `WHEN NOT MATCHED THEN INSERT`. Rerunning a scrape date inserts zero rows.

The clean Type 2 MERGE has two MATCHED branches and the order matters. The first fires only
on `src.scrape_dt > tgt.last_seen_scrape` and is the only branch that increments
`times_seen`. The second handles the same scrape seen again and touches nothing but
`is_current`. Without that guard a rerun would inflate `times_seen` every time.

Proof, identical before and after a deliberate rerun:

```sql
SELECT scrape_date,
       COUNT(*)                    AS rows_landed,
       COUNT(DISTINCT advisory_sk) AS distinct_keys
FROM   nyc_mobility.raw.traffic_advisory
GROUP  BY scrape_date
ORDER  BY scrape_date;
```

### Monitoring

**Execution.** Daily time trigger. Tasks run raw, clean, validation, each depending on the
previous. Roughly 26 locations and 72 entries per publish, 66 rows on the 14 Sep baseline.

**Failures.** Email on task failure. A fetch failure means nyc.gov was unreachable, which is
not our bug. A parse failure means the page structure changed, which is.

**Freshness, two signals.** They look identical in the data and only one is a problem.

```sql
-- ours: did the scrape run?
SELECT MAX(scrape_date) AS last_scrape,
       DATEDIFF(current_date(), MAX(scrape_date)) AS days_behind
FROM   nyc_mobility.raw.traffic_advisory;

-- theirs: did DOT republish? stale here with a current scrape is not a fault
SELECT MAX(advisory_week_start) AS last_published_week
FROM   nyc_mobility.clean.traffic_advisory;
```

**Data quality.** Ten checks write one row each to
`nyc_mobility.validation.traffic_advisory_validation`. PASS at zero failures, WARN under
5 percent, FAIL at or above. Strict checks skip the percentage and go straight to FAIL.

| Column | Dimension | Catches | Strict |
| --- | --- | --- | --- |
| `ALL_COLUMNS (raw, latest scrape)` | Completeness | A silent zero-row scrape | yes |
| `location_name (raw)` | Validity | Parser regression, floor 20 against a baseline of 26 | no |
| `advisory_sk (raw)` | Uniqueness | Duplicate keys, which break raw MERGE idempotency | yes |
| `scrape_date (raw)` | Timeliness | A scrape dated in the future | yes |
| `closure_bk` | Uniqueness | More than one row per advisory in the Type 2 table | yes |
| `closure_bk` | Completeness | Null business key, the join key for everything downstream | yes |
| `effective_from, effective_to` | Validity | Dates parsed the wrong way round, row still usable | no |
| `borough` | Validity | A borough string DOT renamed, or the parser mangled | no |
| `closure_bk` | Accuracy | An advisory in the newest raw scrape missing from clean | yes |
| `times_seen` | Consistency | The clean-layer idempotency guard | yes |

The location floor of 20 is a regression alarm, not a statistical outlier rule, so the IQR
method used for trip distance does not apply. A parser that stops recognising bare `strong`
headings returns about 11 locations where the page has 26. It does not error, it quietly
returns less. 20 sits between the broken state and the healthy one.

### Governance

**Naming.** Catalog `nyc_mobility`. Schemas by layer, not owner: `raw`, `clean`,
`validation`. Same table name across layers. Columns snake_case, `_bk` business key,
`_sk` surrogate key, `is_` boolean.

**Ownership.** Kinah, Engineer 4. Upstream is NYC DOT, a public page with no contract, no
SLA and no notice before a layout change. Consumers are the DQ compilation and Q4. The
parser is ours to fix, and the location count check is what tells us it broke.

**Permissions.** Least privilege by layer.

| Role | raw | clean | validation |
| --- | --- | --- | --- |
| Pipeline job | write | write | write |
| Engineers | read | read and write | read |
| Analysts | none | read | read |

Nobody edits `raw` by hand. It is the only record of what the page said on a given day.

**Lineage.**

```
nyc.gov weektraf.shtml
  -> ftw-b12-r2 volume, weektraf_YYYY-MM-DD.html   (landed byte for byte)
    -> nyc_mobility.raw.traffic_advisory           (MERGE on advisory_sk)
      -> nyc_mobility.clean.traffic_advisory       (Type 2 MERGE on closure_bk)
        -> nyc_mobility.validation.traffic_advisory_validation
        -> dim_advisory -> Q4
```

The landed HTML is why a parser bug is recoverable without re-fetching, and the only reason
any part of this source can be rebuilt.

**Rules that stop a release.**

1. A zero-row scrape must fail the load, never merge an empty result over good history.
2. `closure_bk` must be unique and not null in clean. It is the join key for Q4.
3. `times_seen` must never exceed the number of scrapes raw holds.
