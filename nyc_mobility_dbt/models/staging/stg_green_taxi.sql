{{ config(materialized='table', alias='green_taxi') }}

with unioned as (
    select * from {{ source('raw', 'green_03_2026') }}
    union all
    select * from {{ source('raw', 'green_04_2026') }}
    union all
    select * from {{ source('raw', 'green_05_2026') }}
),

with_duration as (
    select *,
        (unix_timestamp(lpep_dropoff_datetime) - unix_timestamp(lpep_pickup_datetime)) / 60.0
            as trip_duration_min
    from unioned
),

excluded as (
    select * from with_duration
    where lpep_pickup_datetime >= '2026-03-01'
      and lpep_pickup_datetime < '2026-06-01'
      and lpep_dropoff_datetime >= lpep_pickup_datetime
      and trip_distance <= 100000
)

select
    cast(VendorID as int)          as VendorID,
    cast(RatecodeID as int)        as RatecodeID,
    cast(PULocationID as int)      as PULocationID,
    cast(DOLocationID as int)      as DOLocationID,
    cast(passenger_count as int)   as passenger_count,
    cast(payment_type as int)      as payment_type,
    cast(trip_type as int)         as trip_type,
    cast(lpep_pickup_datetime as timestamp)  as lpep_pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as lpep_dropoff_datetime,
    round(cast(trip_distance as double), 2)  as trip_distance,
    round(cast(fare_amount as double), 2)    as fare_amount,
    round(cast(extra as double), 2)          as extra,
    round(cast(mta_tax as double), 2)        as mta_tax,
    round(cast(tip_amount as double), 2)     as tip_amount,
    round(cast(tolls_amount as double), 2)   as tolls_amount,
    round(cast(ehail_fee as double), 2)      as ehail_fee,
    round(cast(improvement_surcharge as double), 2) as improvement_surcharge,
    round(cast(total_amount as double), 2)   as total_amount,
    round(cast(congestion_surcharge as double), 2)  as congestion_surcharge,
    round(cast(cbd_congestion_fee as double), 2)    as cbd_congestion_fee,
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(trip_duration_min as decimal(10,2)) as trip_duration_min
from excluded