-- Validity, not strict. Dates parsed the wrong way round. The row is still usable.

{{ config(severity = 'warn') }}

select closure_bk, effective_from, effective_to
from {{ ref('traffic_advisory_clean') }}
where effective_from is not null
  and effective_to   is not null
  and effective_from > effective_to
