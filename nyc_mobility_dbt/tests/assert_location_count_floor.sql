-- Validity, not strict. Parser regression alarm, floor 20 against a baseline of 26.
-- A parser that stops recognising bare strong headings returns about 11 locations.

{{ config(severity = 'warn') }}

select count(distinct location_name) as distinct_locations
from {{ ref('stg_traffic_advisory_current') }}
having count(distinct location_name) < 20
