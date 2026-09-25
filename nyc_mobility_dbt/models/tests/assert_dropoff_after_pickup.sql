-- tests/assert_dropoff_after_pickup.sql
select * from {{ ref('stg_green_taxi') }}
where lpep_dropoff_datetime < lpep_pickup_datetime