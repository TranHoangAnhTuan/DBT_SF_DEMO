{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by "VendorID", "tpep_pickup_datetime" ORDER BY "VendorID") as rn
  from {{ source('staging','GREEN_TAXI_DATA') }}
  where "VendorID" is not null 
)
select
    cast("VendorID" as integer) as "VendorID",

    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["'VendorID'", "'tpep_pickup_datetime'"]) }} as "tripid",
    cast("RatecodeID" as integer) as "RatecodeID",
    cast("PULocationID" as integer) as "PULocationID",
    cast("DOLocationID" as integer) as "DOLocationID",

    -- timestamps
    cast("tpep_pickup_datetime" as timestamp) as "pickup_datetime",
    cast("tpep_dropoff_datetime" as timestamp) as "dropoff_datetime",

    -- trip info
    cast("passenger_count" as integer) as "passenger_count",
    cast("trip_distance" as numeric) as "trip_distance",

    -- payment info
    cast("fare_amount" as numeric) as "fare_amount",
    cast("extra" as numeric) as "extra",
    cast("mta_tax" as numeric) as "mta_tax",
    cast("tip_amount" as numeric) as "tip_amount",
    cast("tolls_amount" as numeric) as "tolls_amount",
    cast("improvement_surcharge" as numeric) as "improvement_surcharge",
    cast("total_amount" as numeric) as "total_amount",
    coalesce(
        try_cast("payment_type" as integer)
    ,0) as "payment_type",
    {{ get_payment_type_description("'payment_type'") }} as "payment_type_description"
from tripdata
where rn = 1

