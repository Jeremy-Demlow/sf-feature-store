with orders as (
    select * from {{ ref('stg_instacart__orders') }}
    where eval_set = '{{ var("eval_set", "prior") }}'
),

order_products as (
    select * from {{ ref('stg_instacart__order_products') }}
),

products as (
    select * from {{ ref('stg_instacart__products') }}
),

product_metrics as (
    select
        op.product_id,
        p.aisle_id,
        p.department_id,
        count(distinct op.order_id) as product_orders,
        sum(case when op.is_reordered = 1 then 1 else 0 end) as product_reorders,
        sum(case when op.is_reordered = 1 then 1 else 0 end) / count(distinct op.order_id) as product_reorder_rate,
        avg(op.cart_position) as product_avg_cart_position
    from order_products op
    inner join orders o on op.order_id = o.order_id
    inner join products p on op.product_id = p.product_id
    group by 1, 2, 3
)

select * from product_metrics