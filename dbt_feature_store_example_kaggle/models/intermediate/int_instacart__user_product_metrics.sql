with orders as (
    select * from {{ ref('stg_instacart__orders') }}
    where eval_set = '{{ var("eval_set", "prior") }}'
),

order_products as (
    select * from {{ ref('stg_instacart__order_products') }}
),

user_orders as (
    select
        o.user_id,
        o.order_id,
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        op.product_id,
        op.cart_position,
        op.is_reordered
    from orders o
    inner join order_products op on o.order_id = op.order_id
),

user_product_metrics as (
    select
        user_id,
        product_id,
        count(distinct order_id) as up_orders,
        max(order_number) as up_last_order_id,
        sum(cart_position) as up_sum_pos_in_cart,
        avg(cart_position) as up_avg_cart_position,
        max(order_hour_of_day) as up_last_order_hour,
        count(distinct order_id) / nullif(max(order_number), 0) as up_orders_ratio,
        
        -- Add a derived feature for recency (assuming higher order_number is more recent)
        max(order_number) as up_orders_since_last,
        
        -- Hour difference (absolute value for time of day patterns)
        abs(max(order_hour_of_day) - min(order_hour_of_day)) as up_delta_hour_vs_last
    from user_orders
    group by 1, 2
)

select * from user_product_metrics