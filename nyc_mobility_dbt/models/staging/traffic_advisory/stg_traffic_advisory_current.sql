-- Newest scrape only. This is the same CTE the clean notebook builds as a temp view,
-- expressed as a dbt view so the incremental model and the tests can ref() it.

with latest as (

    select max(scrape_date) as scrape_date
    from {{ source('raw', 'traffic_advisory') }}

)

select
    sha2(concat_ws('|', r.section_id, r.location_name,
         coalesce(r.detail_label, '~'), r.advisory_text), 256)   as closure_bk,
    r.section_id,
    r.section_name,
    case when r.section_name like '%Crossings%' then 'Crossings'
         else split(r.section_name, '/')[0] end                 as borough,
    r.location_name,
    r.entry_type,
    r.detail_label,
    r.street_name,
    r.from_street,
    r.to_street,
    r.advisory_text,
    cast(r.date_first_mentioned as date)                        as effective_from,
    cast(r.date_last_mentioned  as date)                        as effective_to,
    cast(r.advisory_week_start  as date)                        as advisory_week_start,
    cast(r.advisory_week_end    as date)                        as advisory_week_end,
    cast(r.scrape_date          as date)                        as scrape_dt,
    r.source_system,
    r.source_url,
    r.batch_id
from {{ source('raw', 'traffic_advisory') }} r
join latest l
  on r.scrape_date = l.scrape_date
