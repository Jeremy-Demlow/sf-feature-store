with orders as (
    select * from {{ ref('stg_instacart__orders') }}
    where eval_set = '{{ var("eval_set", "prior") }}'
),

order_products as (
    select * from {{ ref('stg_instacart__order_products') }}
),

orders_joined as (
    select
        o.order_id,
        o.user_id,
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order,
        count(op.product_id) as basket_size,
        sum(case when op.is_reordered = 1 then 1 else 0 end) as reordered_items
    from orders o
    left join order_products op on o.order_id = op.order_id
    group by 1, 2, 3, 4, 5, 6
),

user_metrics as (
    select
        user_id,
        max(order_number) as user_total_orders,
        avg(days_since_prior_order) as avg_days_between_orders,
        stddev(days_since_prior_order) as std_days_between_orders,
        median(order_hour_of_day) as typical_order_hour,
        mode(order_dow) as preferred_order_day,
        avg(order_hour_of_day) as avg_order_hour,
        avg(basket_size) as avg_basket_size,
        sum(basket_size) as total_items_ordered,
        count(distinct order_id) as total_orders,
        sum(reordered_items) / sum(basket_size) as overall_reorder_rate
    from orders_joined
    where order_number >= {{ var("min_orders", 4) }}
    group by 1
)

select * from user_metrics