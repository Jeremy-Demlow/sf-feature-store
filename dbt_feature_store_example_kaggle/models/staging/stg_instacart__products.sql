with source as (
    select * from {{ source('instacart_raw', 'PRODUCTS') }}
),

renamed as (
    select
        PRODUCT_ID as product_id,
        PRODUCT_NAME as product_name,
        AISLE_ID as aisle_id,
        DEPARTMENT_ID as department_id
    from source
)

select * from renamed