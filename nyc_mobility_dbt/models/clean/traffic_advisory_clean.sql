-- Type 2 history of advisories, one row per closure_bk, accumulated one scrape at a time.
-- Reproduces 04_traffic_advisory_clean: the two MATCHED branches and the close-out UPDATE
-- become one SELECT, and dbt's merge on closure_bk does the write.
--
-- Team decision 23 Sep: incremental with merge, not a snapshot, so the column shape
-- downstream (first_seen_scrape, last_seen_scrape, is_current, times_seen) stays the same.

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'closure_bk',
    on_schema_change = 'append_new_columns'
) }}

with src as (

    select * from {{ ref('stg_traffic_advisory_current') }}

)

{% if is_incremental() %}

, tgt as (

    select * from {{ this }}

)

-- Advisories on the page today. Guard: times_seen only increments when this scrape is
-- newer than what the row already records, so a rerun of the same date changes nothing.
, seen as (

    select
        s.closure_bk,
        s.section_id,
        s.section_name,
        s.borough,
        s.location_name,
        s.entry_type,
        s.detail_label,
        s.street_name,
        s.from_street,
        s.to_street,
        s.advisory_text,
        coalesce(t.effective_from, s.effective_from)                 as effective_from,
        coalesce(s.effective_to, t.effective_to)                     as effective_to,
        s.advisory_week_start,
        s.advisory_week_end,
        coalesce(t.first_seen_scrape, s.scrape_dt)                   as first_seen_scrape,
        greatest(coalesce(t.last_seen_scrape, s.scrape_dt), s.scrape_dt) as last_seen_scrape,
        true                                                         as is_current,
        coalesce(t.times_seen, 0)
          + case when t.closure_bk is null or s.scrape_dt > t.last_seen_scrape
                 then 1 else 0 end                                   as times_seen,
        s.source_system,
        s.source_url,
        current_timestamp()                                          as ingested_at,
        s.batch_id
    from src s
    left join tgt t
      on s.closure_bk = t.closure_bk

)

-- Advisories that were current but are missing from the newest scrape have come off
-- the page. Close them, never delete them, so the history stays readable.
, gone as (

    select
        t.closure_bk,
        t.section_id,
        t.section_name,
        t.borough,
        t.location_name,
        t.entry_type,
        t.detail_label,
        t.street_name,
        t.from_street,
        t.to_street,
        t.advisory_text,
        t.effective_from,
        t.effective_to,
        t.advisory_week_start,
        t.advisory_week_end,
        t.first_seen_scrape,
        t.last_seen_scrape,
        false                                                        as is_current,
        t.times_seen,
        t.source_system,
        t.source_url,
        current_timestamp()                                          as ingested_at,
        t.batch_id
    from tgt t
    left anti join src s
      on s.closure_bk = t.closure_bk
    where t.is_current

)

select * from seen
union all
select * from gone

{% else %}

-- First build. Every advisory in the newest scrape is seen for the first time.
select
    closure_bk,
    section_id,
    section_name,
    borough,
    location_name,
    entry_type,
    detail_label,
    street_name,
    from_street,
    to_street,
    advisory_text,
    effective_from,
    effective_to,
    advisory_week_start,
    advisory_week_end,
    scrape_dt                as first_seen_scrape,
    scrape_dt                as last_seen_scrape,
    true                     as is_current,
    1                        as times_seen,
    source_system,
    source_url,
    current_timestamp()      as ingested_at,
    batch_id
from src

{% endif %}
