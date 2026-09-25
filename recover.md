## Recover

This is the complete documentation of what are the modes for a run to be considered a failure, what happens, and if they are rerun-safe.

| Dataset | Failure mode | What happens | Rerun-safe? |
|---|---|---|---|
| **Green taxi** | A requested month's file is missing | Notebook prints "skipped, file not found" and continues — doesn't crash the whole run | Yes |
| **Green taxi** | Source file is empty | Row count check stops the load with an error before it's saved | Yes (nothing written) |
| **Green taxi** | Same month loaded again | Anti-join compares vendor, pickup/dropoff time, location, distance, and fare against what's already there; only genuinely new rows are added | Yes — verified by deliberately reloading May a second time and confirming the row count doesn't move |
| **Taxi zones (raw)** | Wrong or unexpected file picked up | File discovery now filters to known extensions and requires exactly one match, instead of blindly taking whatever the OS lists first | Yes |
| **Taxi zones (raw)** | Empty file or a renamed/reordered column | Row count + schema assertion catches it before the write | Yes |
| **Taxi zones (raw)** | Any rerun | `overwrite` mode is naturally idempotent — one file, one table, no accumulation. Idempotency check compares row count before and after | Yes |
| **Taxi zones (clean)** | Raw table empty or missing | Source validation (Cell 1) stops the transform before it runs | Yes |
| **Taxi zones (clean)** | A bad cast or blank borough/zone | Hard excludes (Cell 2) filter these out explicitly, rather than letting a silent `NULL` from a failed `CAST` pass through | Yes |
| **Taxi zones (clean)** | Any rerun | `CREATE OR REPLACE TABLE ... AS SELECT` is a pure function of raw — same input always gives the same output. Write validation (Cell 4) checks row count parity plus uniqueness/non-null on `location_id` | Yes, no separate rerun-and-compare step needed |
| **Weather (raw)** | Same file loaded again | Switched from `overwrite` to a Delta `MERGE` keyed on `observation_time + latitude + longitude` — matches update in place, non-matches insert | Yes |
| **Weather (raw)** | Subsequent runs | Looks up `MAX(observation_time)` already in the table and only merges source rows past that watermark (first run still does a full write since the table doesn't exist yet) | Yes |
| **Weather (raw)** | Source JSON has overlapping timestamps | `dropDuplicates` on the natural key guards against this before it ever reaches the merge | Yes |
| **Weather (raw)** | Zero records parsed | Guard raises before any write; a post-write count of 0 also raises `RuntimeError`, failing the cell | Yes (nothing written) |
| **Traffic advisory (raw)** | Timeout / non-200 from `nyc.gov` | Fetch raises after 3 tries; nothing was written | Yes |
| **Traffic advisory (raw)** | Zero rows parsed | Load raises *before* writing — an empty merge over good history is exactly what this guards against | After the parser is fixed |
| **Traffic advisory (raw)** | Parser regression (location count drops from ~26 to ~11) | Run "succeeds" but the validity check flags it | Fix the parser, re-parse from the already-landed HTML (no re-fetch needed), rerun all three tasks |
| **Traffic advisory (clean)** | Merge fails partway | Task fails; Delta `MERGE` is atomic, so there's no half-written state to clean up | Yes — just rerun |
| **Traffic advisory** | Same scrape loaded twice | Nothing visible changes, by design (`advisory_sk` MERGE + `times_seen` guard) | Yes |
| **Traffic advisory** | A day missed entirely | `days_behind` > 1 on the freshness query | **No** — restore the schedule and record the gap; nothing recovers a day that was never scraped |

**Common thread:** every raw loader now fails loudly (an assertion or a raised error) rather than silently writing a broken or empty result. That single design rule is what makes "can I safely rerun this" a yes for almost every failure mode above.
