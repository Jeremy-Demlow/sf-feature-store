with source as (
    select * from {{ source('instacart_raw', 'AISLES') }}
),

renamed as (
    select
        AISLE_ID as aisle_id,
        AISLE as aisle_name
    from source
)

select * from renamed