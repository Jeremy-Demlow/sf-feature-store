# Instacart Feature Store dbt Project

This dbt project transforms Instacart data into feature sets for machine learning, focusing on predicting product reorders. It implements a comprehensive feature engineering pipeline inspired by Instacart's 2017 Kaggle competition.

## Project Overview

This dbt project transforms raw Instacart data into production-ready feature sets following a structured, modular approach. The project focuses on creating analytical features that predict whether a customer will reorder a previously purchased product.

### Project Structure

```
instacart_dbt/
│
├── models/
│   ├── sources.yml              # Data source definitions
│   ├── staging/                 # Staging models (minimal transformations)
│   ├── intermediate/            # Intermediate models with business logic
│   └── marts/                   # Final feature sets
│       ├── core/                # Core entity models
│       └── ml/                  # ML-specific models
│
├── macros/                      # Custom macros
└── analyses/                    # Ad-hoc analyses
```

### Feature Sets

The project creates the following feature groups:

1. **User Features** (`instacart__user_features`)
   - Order recency and frequency metrics
   - Typical ordering patterns (day/time preferences)
   - Product diversity metrics
   - Reorder behavior statistics

2. **Product Features** (`instacart__product_features`)
   - Popularity metrics
   - Reorder rates
   - Department and aisle rankings
   - Temporal patterns

3. **Order Features** (`instacart__order_features`)
   - Basket composition metrics
   - Product diversity
   - Timing patterns
   - Reorder ratios

4. **User-Product Features** (`instacart__user_product_features`)
   - Interaction frequency
   - Product reorder patterns
   - Time and day preferences for specific products
   - Normalized metrics

5. **ML Training Features** (`instacart__training_features`)
   - Combined features from all entities
   - Target variable (reordered flag)
   - Additional derived features for ML

## Getting Started

### Prerequisites

- dbt (v1.3.0 or newer)
- A Snowflake account with the Instacart dataset loaded
- For Python models: Snowflake with Python UDF support enabled

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/instacart-dbt.git
   cd instacart-dbt
   ```

2. Install dependencies:
   ```bash
   dbt deps
   ```

3. Update your dbt profile in `~/.dbt/profiles.yml`:
   ```yaml
   dbt_feature_store_example_kaggle:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: [your-account]
         user: [username]
         password: [password]
         role: [role]
         database: [database]
         warehouse: [warehouse]
         schema: transform
         threads: 4
   ```

4. Verify the connection:
   ```bash
   dbt debug
   ```

5. Build the models:
   ```bash
   dbt build
   ```

### Usage Examples

Run specific model groups:
```bash
# Run staging models
dbt run --select staging

# Run core feature models
dbt run --select marts.core

# Run ML training features
dbt run --select marts.ml
```

Generate documentation:
```bash
dbt docs generate
dbt docs serve
```

## Features in Detail

### User Features

Key features include:
- `user_total_orders`: Total number of orders placed
- `avg_days_between_orders`: Average time between orders
- `typical_order_hour`: Median hour when orders are placed
- `preferred_order_day`: Most common day of week for ordering
- `avg_basket_size`: Average number of items per order
- `reorder_rate`: Percentage of items that are reorders

### Product Features

Key features include:
- `product_orders`: Number of times product was ordered
- `product_reorder_rate`: Percentage of orders that are reorders
- `product_avg_cart_position`: Average position in cart
- `department_popularity_rank`: Popularity ranking within department
- `peak_hour`: Hour with most orders for this product

### User-Product Features

Key features include:
- `up_orders`: Number of times user ordered this product
- `orders_since_last_purchase`: Orders since user last bought this product
- `up_orders_ratio`: Percentage of user's orders containing this product
- `dominant_day_part`: Preferred time of day for this product (morning, midday, evening, night)
- `dominant_dow`: Preferred day of week for this product

### ML Training Features

The training dataset combines features from all entities with derived features including:
- `is_preferred_dow`: Flag if order is on user's preferred day
- `is_preferred_time`: Flag if order is during user's preferred time period
- `purchase_recency_bucket`: Categorization of recency (recent, medium, old)

## Python Models

This project includes a Python-based model (`instacart__training_features`) that leverages Snowpark for more complex feature engineering. To use this model:

1. Ensure your dbt_project.yml has proper Python model configuration:
   ```yaml
   python-models:
     instacart:
       marts:
         ml:
           +materialized: table
   ```

2. Install required packages:
   ```bash
   pip install dbt-snowflake>=1.3.0 snowflake-snowpark-python
   ```

## Data Lineage

Raw data flows through the project as follows:

1. **Source data**: `INSTACART_RAW` schema with tables from the Kaggle dataset
2. **Staging**: Clean, typed models of raw tables
3. **Intermediate**: Business logic and metrics by entity
4. **Marts**: Feature-rich tables ready for analytics and ML

## Contributions

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Based on the Instacart Market Basket Analysis dataset
- Inspired by feature engineering approaches from the Kaggle competition