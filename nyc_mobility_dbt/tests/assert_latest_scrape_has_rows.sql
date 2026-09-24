-- Completeness, strict. A silent zero-row scrape must fail.
-- Returns one row when the newest scrape landed nothing.

select 'latest scrape has zero rows' as failure
from (
    select count(*) as n
    from {{ ref('stg_traffic_advisory_current') }}
)
where n = 0
