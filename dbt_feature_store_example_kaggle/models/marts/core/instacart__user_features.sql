{{
    config(
        materialized='table',
        tags=['core', 'feature-store']
    )
}}

with user_metrics as (
    select * from {{ ref('int_instacart__user_order_metrics') }}
),

orders as (
    select * from {{ ref('stg_instacart__orders') }}
),

order_products as (
    select * from {{ ref('stg_instacart__order_products') }}
),

-- Get last order details for each user
last_orders as (
    select
        o.user_id,
        o.order_id,
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order,
        row_number() over (
            partition by o.user_id 
            order by o.order_number desc
        ) as rn
    from orders o
    where o.eval_set = '{{ var("eval_set", "prior") }}'
),

last_order_per_user as (
    select *
    from last_orders
    where rn = 1
),

-- Count distinct products purchased per user
distinct_products as (
    select
        o.user_id,
        count(distinct op.product_id) as distinct_products_count
    from orders o
    inner join order_products op on o.order_id = op.order_id
    where o.eval_set = '{{ var("eval_set", "prior") }}'
    group by 1
),

-- Calculate reordered product statistics per user
reorder_stats as (
    select
        o.user_id,
        sum(op.is_reordered) as total_reordered_products,
        sum(op.is_reordered) / nullif(count(op.product_id), 0) as reorder_rate
    from orders o
    inner join order_products op on o.order_id = op.order_id
    where o.eval_set = '{{ var("eval_set", "prior") }}'
    group by 1
),

final as (
    select
        um.user_id,
        
        -- Order recency features
        um.user_total_orders,
        um.avg_days_between_orders,
        um.std_days_between_orders,
        
        -- Order timing features
        um.typical_order_hour,
        um.preferred_order_day,
        um.avg_order_hour,
        
        -- Basket features
        um.avg_basket_size,
        um.total_items_ordered,
        dp.distinct_products_count,
        dp.distinct_products_count / nullif(um.total_orders, 0) as avg_unique_products_per_order,
        
        -- Reorder behavior
        rs.total_reordered_products,
        rs.reorder_rate,
        
        -- Last order information
        lo.order_dow as last_order_dow,
        lo.order_hour_of_day as last_order_hour,
        lo.days_since_prior_order as last_order_days_since_prior
        
    from user_metrics um
    inner join distinct_products dp on um.user_id = dp.user_id
    inner join reorder_stats rs on um.user_id = rs.user_id
    inner join last_order_per_user lo on um.user_id = lo.user_id
)

select * from final