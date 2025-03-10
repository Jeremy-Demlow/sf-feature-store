with source as (
    select * from {{ source('instacart_raw', 'ORDERS') }}
),

renamed as (
    select
        ORDER_ID as order_id,
        USER_ID as user_id,
        ORDER_NUMBER as order_number,
        ORDER_DOW as order_dow,
        ORDER_HOUR_OF_DAY as order_hour_of_day,
        DAYS_SINCE_PRIOR_ORDER as days_since_prior_order,
        EVAL_SET as eval_set
    from source
)

select * from renamed