#!/bin/bash

# Create main project directory
mkdir -p .

# Create config files
touch ./dbt_project.yml
touch ./packages.yml

# Create model directories and files
mkdir -p ./models
touch ./models/sources.yml

# Staging models
mkdir -p ./models/staging
touch ./models/staging/schema.yml
touch ./models/staging/stg_instacart__aisles.sql
touch ./models/staging/stg_instacart__departments.sql
touch ./models/staging/stg_instacart__orders.sql
touch ./models/staging/stg_instacart__products.sql
touch ./models/staging/stg_instacart__order_products.sql

# Intermediate models
mkdir -p ./models/intermediate
touch ./models/intermediate/schema.yml
touch ./models/intermediate/int_instacart__user_order_metrics.sql
touch ./models/intermediate/int_instacart__product_metrics.sql
touch ./models/intermediate/int_instacart__user_product_metrics.sql

# Mart models
mkdir -p ./models/marts
touch ./models/marts/schema.yml

# Core mart models
mkdir -p ./models/marts/core
touch ./models/marts/core/instacart__user_features.sql
touch ./models/marts/core/instacart__product_features.sql
touch ./models/marts/core/instacart__order_features.sql
touch ./models/marts/core/instacart__user_product_features.sql

# ML mart models
mkdir -p ./models/marts/ml
touch ./models/marts/ml/instacart__training_features.sql

# Macros
mkdir -p ./macros
touch ./macros/time_windows.sql

# Analyses
mkdir -p ./analyses
touch ./analyses/feature_importance.sql

echo "Directory structure created successfully."