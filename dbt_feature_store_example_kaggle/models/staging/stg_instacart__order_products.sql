with source as (
    select * from {{ source('instacart_raw', 'ORDER_PRODUCTS') }}
),

renamed as (
    select
        ORDER_ID as order_id,
        PRODUCT_ID as product_id,
        ADD_TO_CART_ORDER as cart_position,
        REORDERED as is_reordered
    from source
)

select * from renamed