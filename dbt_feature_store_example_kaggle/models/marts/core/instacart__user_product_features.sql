{{
    config(
        materialized='table',
        tags=['core', 'feature-store']
    )
}}

with user_product_metrics as (
    select * from {{ ref('int_instacart__user_product_metrics') }}
),

orders as (
    select * from {{ ref('stg_instacart__orders') }}
    where eval_set = '{{ var("eval_set", "prior") }}'
),

user_features as (
    select * from {{ ref('instacart__user_features') }}
),

product_features as (
    select 
        product_id,
        product_orders,
        product_reorder_rate
    from {{ ref('instacart__product_features') }}
),

-- Track recency of last purchase of each product by user
user_product_recency as (
    select
        o.user_id,
        op.product_id,
        o.order_number,
        row_number() over (
            partition by o.user_id, op.product_id 
            order by o.order_number desc
        ) as rn
    from orders o
    inner join {{ ref('stg_instacart__order_products') }} op 
        on o.order_id = op.order_id
),

last_purchase as (
    select
        user_id,
        product_id,
        order_number as last_purchase_order_number
    from user_product_recency
    where rn = 1
),

-- Calculate user's purchase frequency for products in specific time periods
user_product_time_patterns as (
    select
        o.user_id,
        op.product_id,
        
        -- Count purchases by day of week
        sum(case when o.order_dow = 0 then 1 else 0 end) as dow_0_purchases,
        sum(case when o.order_dow = 1 then 1 else 0 end) as dow_1_purchases,
        sum(case when o.order_dow = 2 then 1 else 0 end) as dow_2_purchases,
        sum(case when o.order_dow = 3 then 1 else 0 end) as dow_3_purchases,
        sum(case when o.order_dow = 4 then 1 else 0 end) as dow_4_purchases,
        sum(case when o.order_dow = 5 then 1 else 0 end) as dow_5_purchases,
        sum(case when o.order_dow = 6 then 1 else 0 end) as dow_6_purchases,
        
        -- Count purchases by time of day
        sum(case when o.order_hour_of_day between 5 and 9 then 1 else 0 end) as morning_purchases,
        sum(case when o.order_hour_of_day between 10 and 14 then 1 else 0 end) as midday_purchases,
        sum(case when o.order_hour_of_day between 15 and 19 then 1 else 0 end) as evening_purchases,
        sum(case when o.order_hour_of_day >= 20 or o.order_hour_of_day < 5 then 1 else 0 end) as night_purchases
    from orders o
    inner join {{ ref('stg_instacart__order_products') }} op 
        on o.order_id = op.order_id
    group by 1, 2
),

-- Pre-calculate user averages
user_avg_stats as (
    select
        user_id,
        avg_basket_size
    from user_features
),

-- Deviation from average behavior
user_product_normalized as (
    select
        upm.user_id,
        upm.product_id,
        upm.up_orders,
        
        -- Normalize user-product behavior against user average
        upm.up_orders / nullif(uas.avg_basket_size, 0) as user_relative_frequency,
        
        -- Normalize user-product behavior against product average
        upm.up_orders / nullif(pf.product_orders, 0) as product_relative_frequency,
        
        -- Normalize cart position
        upm.up_avg_cart_position / nullif(uas.avg_basket_size, 0) as relative_cart_position
    from user_product_metrics upm
    inner join user_avg_stats uas on upm.user_id = uas.user_id
    inner join product_features pf on upm.product_id = pf.product_id
),

-- Find dominant day part
dominant_day_parts as (
    select
        user_id,
        product_id,
        case greatest(
            morning_purchases, 
            midday_purchases, 
            evening_purchases, 
            night_purchases
        )
            when morning_purchases then 'morning'
            when midday_purchases then 'midday'
            when evening_purchases then 'evening'
            else 'night'
        end as dominant_day_part
    from user_product_time_patterns
),

-- Find dominant day of week
dominant_dows as (
    select
        user_id,
        product_id,
        case greatest(
            dow_0_purchases, 
            dow_1_purchases, 
            dow_2_purchases, 
            dow_3_purchases, 
            dow_4_purchases, 
            dow_5_purchases, 
            dow_6_purchases
        )
            when dow_0_purchases then 0
            when dow_1_purchases then 1
            when dow_2_purchases then 2
            when dow_3_purchases then 3
            when dow_4_purchases then 4
            when dow_5_purchases then 5
            else 6
        end as dominant_dow
    from user_product_time_patterns
),

final as (
    select
        upm.user_id,
        upm.product_id,
        concat(upm.user_id, '_', upm.product_id) as user_product_id,
        
        -- Basic metrics
        upm.up_orders,
        upm.up_last_order_id,
        upm.up_sum_pos_in_cart,
        upm.up_avg_cart_position,
        upm.up_last_order_hour,
        upm.up_orders_ratio,
        upm.up_orders_since_last,
        upm.up_delta_hour_vs_last,
        
        -- Recency features
        lp.last_purchase_order_number,
        uf.user_total_orders - lp.last_purchase_order_number as orders_since_last_purchase,
        
        -- Time patterns
        uptp.dow_0_purchases,
        uptp.dow_1_purchases,
        uptp.dow_2_purchases,
        uptp.dow_3_purchases,
        uptp.dow_4_purchases,
        uptp.dow_5_purchases,
        uptp.dow_6_purchases,
        uptp.morning_purchases,
        uptp.midday_purchases,
        uptp.evening_purchases,
        uptp.night_purchases,
        
        -- Dominant time patterns
        ddp.dominant_day_part,
        dd.dominant_dow,
        
        -- Normalized metrics
        upn.user_relative_frequency,
        upn.product_relative_frequency,
        upn.relative_cart_position
        
    from user_product_metrics upm
    inner join user_features uf on upm.user_id = uf.user_id
    inner join product_features pf on upm.product_id = pf.product_id
    left join last_purchase lp on upm.user_id = lp.user_id and upm.product_id = lp.product_id
    left join user_product_time_patterns uptp on upm.user_id = uptp.user_id and upm.product_id = uptp.product_id
    left join user_product_normalized upn on upm.user_id = upn.user_id and upm.product_id = upn.product_id
    left join dominant_day_parts ddp on upm.user_id = ddp.user_id and upm.product_id = ddp.product_id
    left join dominant_dows dd on upm.user_id = dd.user_id and upm.product_id = dd.product_id
)

select * from final