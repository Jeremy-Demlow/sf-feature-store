{{
    config(
        materialized='table',
        tags=['core', 'feature-store']
    )
}}

with product_metrics as (
    select * from {{ ref('int_instacart__product_metrics') }}
),

products as (
    select * from {{ ref('stg_instacart__products') }}
),

aisles as (
    select * from {{ ref('stg_instacart__aisles') }}
),

departments as (
    select * from {{ ref('stg_instacart__departments') }}
),

-- Get product order popularity within department
department_rankings as (
    select
        pm.product_id,
        pm.department_id,
        pm.product_orders,
        row_number() over (
            partition by pm.department_id 
            order by pm.product_orders desc
        ) as department_popularity_rank
    from product_metrics pm
),

-- Get product order popularity within aisle
aisle_rankings as (
    select
        pm.product_id,
        pm.aisle_id,
        pm.product_orders,
        row_number() over (
            partition by pm.aisle_id 
            order by pm.product_orders desc
        ) as aisle_popularity_rank
    from product_metrics pm
),

-- Calculate peak hours for each product
product_peak_hours as (
    select
        op.product_id,
        o.order_hour_of_day as peak_hour,
        count(*) as orders_in_hour,
        row_number() over (
            partition by op.product_id 
            order by count(*) desc
        ) as rn
    from {{ ref('stg_instacart__order_products') }} op
    inner join {{ ref('stg_instacart__orders') }} o on op.order_id = o.order_id
    where o.eval_set = '{{ var("eval_set", "prior") }}'
    group by 1, 2
    qualify rn = 1
),

-- Calculate peak days for each product
product_peak_days as (
    select
        op.product_id,
        o.order_dow as peak_day,
        count(*) as orders_in_day,
        row_number() over (
            partition by op.product_id 
            order by count(*) desc
        ) as rn
    from {{ ref('stg_instacart__order_products') }} op
    inner join {{ ref('stg_instacart__orders') }} o on op.order_id = o.order_id
    where o.eval_set = '{{ var("eval_set", "prior") }}'
    group by 1, 2
    qualify rn = 1
),

-- Calculate department averages for normalization
department_stats as (
    select
        pm.department_id,
        avg(pm.product_orders) as avg_dept_orders,
        avg(pm.product_reorder_rate) as avg_dept_reorder_rate
    from product_metrics pm
    group by 1
),

final as (
    select
        p.product_id,
        p.product_name,
        a.aisle_id,
        a.aisle_name,
        d.department_id,
        d.department_name,
        
        -- Product metrics
        pm.product_orders,
        pm.product_reorders,
        pm.product_reorder_rate,
        pm.product_avg_cart_position,
        
        -- Derived features
        dr.department_popularity_rank,
        ar.aisle_popularity_rank,
        
        -- Temporal patterns
        pph.peak_hour,
        ppd.peak_day,
        
        -- Normalized metrics within department
        pm.product_orders / nullif(ds.avg_dept_orders, 0) as department_order_ratio,
        pm.product_reorder_rate / nullif(ds.avg_dept_reorder_rate, 0) as department_reorder_ratio
        
    from products p
    inner join product_metrics pm on p.product_id = pm.product_id
    inner join aisles a on p.aisle_id = a.aisle_id
    inner join departments d on p.department_id = d.department_id
    inner join department_rankings dr on p.product_id = dr.product_id
    inner join aisle_rankings ar on p.product_id = ar.product_id
    inner join department_stats ds on p.department_id = ds.department_id
    left join product_peak_hours pph on p.product_id = pph.product_id
    left join product_peak_days ppd on p.product_id = ppd.product_id
)

select * from final