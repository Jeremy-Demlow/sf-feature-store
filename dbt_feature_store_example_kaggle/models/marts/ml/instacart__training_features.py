import snowflake.snowpark.functions as F

def model(dbt, session):
    """
    Combine features from multiple models to create training dataset for ML
    """
    dbt.config(materialized = "table")
    # Reference the source tables/models
    train_orders = dbt.ref("stg_instacart__orders").filter(F.col("eval_set") == 'train')
    train_products = dbt.ref("stg_instacart__order_products")
    user_features = dbt.ref("instacart__user_features")
    product_features = dbt.ref("instacart__product_features")
    order_features = dbt.ref("instacart__order_features")
    user_product_features = dbt.ref("instacart__user_product_features")
    
    # Create training dataset spine
    train_dataset = train_orders.join(
        train_products,
        "order_id"
    ).select(
        train_orders["order_id"],
        train_orders["user_id"],
        train_products["product_id"],
        train_orders["order_number"],
        train_orders["order_dow"],
        train_orders["order_hour_of_day"],
        train_products["is_reordered"].alias("reordered")
    )
    
    # Join all the feature tables
    result = train_dataset.join(
        user_features.select(
            "user_id",
            "user_total_orders",
            "avg_days_between_orders",
            "typical_order_hour",
            "preferred_order_day",
            "avg_basket_size",
            "distinct_products_count",
            F.col("reorder_rate").alias("user_reorder_rate")
        ),
        "user_id",
        "left"
    ).join(
        product_features.select(
            "product_id",
            "aisle_id",
            "department_id",
            "product_orders",
            "product_reorders",
            "product_reorder_rate",
            "product_avg_cart_position",
            "department_popularity_rank",
            "aisle_popularity_rank"
        ),
        "product_id",
        "left"
    ).join(
        order_features.select(
            "order_id",
            "basket_size",
            "reorder_ratio",
            "unique_aisles",
            "unique_departments",
            "day_part"
        ),
        "order_id",
        "left"
    ).join(
        user_product_features.select(
            "user_id",
            "product_id",
            "up_orders",
            "up_avg_cart_position",
            "orders_since_last_purchase",
            "up_orders_ratio",
            "dominant_day_part",
            "dominant_dow",
            "user_relative_frequency",
            "product_relative_frequency"
        ),
        ["user_id", "product_id"],
        "left"
    )
    
    # Add derived features - is preferred day of week
    result = result.withColumn(
        "is_preferred_dow",
        F.when(
            result["order_dow"] == result["dominant_dow"], 
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    
    # Is preferred time of day
    morning_condition = (result["dominant_day_part"] == "morning") & (result["order_hour_of_day"].between(5, 9))
    midday_condition = (result["dominant_day_part"] == "midday") & (result["order_hour_of_day"].between(10, 14))
    evening_condition = (result["dominant_day_part"] == "evening") & (result["order_hour_of_day"].between(15, 19))
    night_condition1 = (result["dominant_day_part"] == "night") & (result["order_hour_of_day"] >= 20)
    night_condition2 = (result["dominant_day_part"] == "night") & (result["order_hour_of_day"] < 5)
    
    result = result.withColumn(
        "is_preferred_time",
        F.when(morning_condition | midday_condition | evening_condition | night_condition1 | night_condition2, 
               F.lit(1)).otherwise(F.lit(0))
    )
    
    # Purchase recency bucket
    result = result.withColumn(
        "purchase_recency_bucket",
        F.when(result["orders_since_last_purchase"] <= 1, F.lit("recent"))
         .when(result["orders_since_last_purchase"].between(2, 3), F.lit("medium"))
         .otherwise(F.lit("old"))
    )
    
    return result