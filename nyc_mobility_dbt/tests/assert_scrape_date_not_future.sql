-- Timeliness, strict. A scrape dated in the future is a clock or load bug.

select scrape_date, count(*) as rows_affected
from {{ source('raw', 'traffic_advisory') }}
where scrape_date > current_date()
group by scrape_date
