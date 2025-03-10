{{
    config(
        materialized='table',
        tags=['core', 'feature-store']
    )
}}

with orders as (
    select * from {{ ref('stg_instacart__orders') }}
    where eval_set = '{{ var("eval_set", "prior") }}'
),

order_products as (
    select * from {{ ref('stg_instacart__order_products') }}
),

-- Basic order metrics
order_metrics as (
    select
        o.order_id,
        o.user_id,
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order,
        count(op.product_id) as basket_size,
        sum(case when op.is_reordered = 1 then 1 else 0 end) as num_reordered_items,
        sum(case when op.is_reordered = 1 then 1 else 0 end) / nullif(count(op.product_id), 0) as reorder_ratio,
        avg(op.cart_position) as avg_cart_position,
        min(op.cart_position) as min_cart_position,
        max(op.cart_position) as max_cart_position
    from orders o
    left join order_products op on o.order_id = op.order_id
    group by 1, 2, 3, 4, 5, 6
),

-- Previous order metrics for calculating deltas
prev_order_metrics as (
    select
        o.user_id,
        o.order_number,
        o.order_id,
        lag(om.basket_size) over (
            partition by o.user_id 
            order by o.order_number
        ) as prev_basket_size,
        lag(om.reorder_ratio) over (
            partition by o.user_id 
            order by o.order_number
        ) as prev_reorder_ratio,
        lag(o.order_dow) over (
            partition by o.user_id 
            order by o.order_number
        ) as prev_order_dow,
        lag(o.order_hour_of_day) over (
            partition by o.user_id 
            order by o.order_number
        ) as prev_order_hour
    from orders o
    inner join order_metrics om on o.order_id = om.order_id
),

-- Product aisle and department diversity
product_diversity as (
    select
        o.order_id,
        count(distinct p.aisle_id) as unique_aisles,
        count(distinct p.department_id) as unique_departments
    from orders o
    inner join order_products op on o.order_id = op.order_id
    inner join {{ ref('stg_instacart__products') }} p on op.product_id = p.product_id
    group by 1
),

final as (
    select
        om.order_id,
        om.user_id,
        om.order_number,
        om.order_dow,
        om.order_hour_of_day,
        om.days_since_prior_order,
        
        -- Basic metrics
        om.basket_size,
        om.num_reordered_items,
        om.reorder_ratio,
        om.avg_cart_position,
        om.min_cart_position,
        om.max_cart_position,
        
        -- Product diversity
        pd.unique_aisles,
        pd.unique_departments,
        pd.unique_aisles / nullif(om.basket_size, 0) as aisle_diversity_ratio,
        pd.unique_departments / nullif(om.basket_size, 0) as department_diversity_ratio,
        
        -- Deltas from previous order
        om.basket_size - pom.prev_basket_size as basket_size_delta,
        om.reorder_ratio - pom.prev_reorder_ratio as reorder_ratio_delta,
        
        -- Time pattern features
        case
            when abs(om.order_dow - pom.prev_order_dow) = 0 then 1
            else 0
        end as same_day_as_last_order,
        
        case
            when abs(om.order_hour_of_day - pom.prev_order_hour) <= 3 then 1
            else 0
        end as similar_hour_as_last_order,
        
        -- Day part indicators
        case
            when om.order_hour_of_day between 5 and 9 then 'morning'
            when om.order_hour_of_day between 10 and 14 then 'midday'
            when om.order_hour_of_day between 15 and 19 then 'evening'
            else 'night'
        end as day_part
        
    from order_metrics om
    left join prev_order_metrics pom on om.order_id = pom.order_id
    left join product_diversity pd on om.order_id = pd.order_id
)

select * from final