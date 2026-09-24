-- Consistency, strict. The clean-layer idempotency guard. times_seen can never exceed
-- the number of distinct scrapes raw holds. If it does, a rerun inflated it.

with scrapes as (
    select count(distinct scrape_date) as scrapes_held
    from {{ source('raw', 'traffic_advisory') }}
)

select c.closure_bk, c.times_seen, s.scrapes_held
from {{ ref('traffic_advisory_clean') }} c
cross join scrapes s
where c.times_seen > s.scrapes_held
