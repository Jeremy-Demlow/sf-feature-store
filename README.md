# Snowflake Feature Store


<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->

# Snowflake Feature Store

> A comprehensive toolkit for building and managing ML features in
> Snowflake, with focus on LTV prediction.

## What is this?

This library provides a production-ready interface to Snowflake’s
Feature Store, enabling you to:

1.  Create and manage feature transformations with validation
2.  Handle temporal features and windowed aggregations
3.  Generate point-in-time correct training datasets
4.  Monitor feature quality and detect drift
5.  Maintain feature documentation and versioning

## Architecture

The Snowflake Feature Store library is designed with a modular
architecture to provide a comprehensive feature engineering workflow:

    ┌─────────────────────────────────────────────────────────────────┐
    │                     Snowflake Feature Store                     │
    └─────────────────────────────────────────────────────────────────┘
                                      │
                   ┌─────────────────┴─────────────────┐
                   ▼                                   ▼
    ┌────────────────────────────┐        ┌─────────────────────────┐
    │    Connection Management   │        │    Feature Definitions  │
    │  (Snowflake Session Setup) │        │   (Configs, Validation) │
    └────────────────────────────┘        └─────────────────────────┘
                   │                                   │
                   └─────────────────┬─────────────────┘
                                     ▼
                      ┌─────────────────────────────┐
                      │     Feature Engineering     │
                      │  (Transforms, Aggregations) │
                      └─────────────────────────────┘
                                     │
                    ┌───────────────┴────────────────┐
                    ▼                                ▼
    ┌─────────────────────────────┐      ┌────────────────────────────┐
    │    Feature Views & Store    │      │     Feature Monitoring     │
    │ (Registration, Management)  │      │  (Drift Detection, Stats)  │
    └─────────────────────────────┘      └────────────────────────────┘
                    │                                 │
                    └───────────────┬─────────────────┘
                                    ▼
                      ┌─────────────────────────────┐
                      │    Training Data Generation │
                      │  (Point-in-time Correctness)│
                      └─────────────────────────────┘

## Prerequisites

Before using this library, you’ll need:

1.  **Snowflake Account**
    - Access to a Snowflake account with sufficient privileges
    - Ability to create databases and schemas
2.  **Required Snowflake Privileges**
    - `CREATE DATABASE` or access to an existing database
    - `CREATE SCHEMA` on target database
    - `CREATE TABLE`, `CREATE VIEW` privileges in target schema
    - `USAGE` on a warehouse with adequate compute resources
3.  **Python Environment**
    - Python 3.7 or higher
    - Administrative access to install Python packages
4.  **Snowflake Configurations**
    - Connection parameters (account, username, password)
    - Optionally, Snowflake key pair authentication setup
    - Environment variables or configuration file for credentials
5.  **Data Access**
    - Access to source data in Snowflake tables or external sources
    - Permissions to read from source data locations

## Comparison with Alternatives

### vs. Direct Snowflake Feature Store API

| Feature                | This Library                                 |
|------------------------|----------------------------------------------|
| Learning Curve         | Simplified, higher-level abstractions        |
| Feature Validation     | Built-in validation rules, drift detection   |
| Transformations        | Pre-built, composable transformation library |
| Point-in-time Training | Simplified API with safeguards               |
| Monitoring             | Built-in drift detection and metrics         |
| Documentation          | Auto-generated feature documentation         |

## Install

``` bash
pip install snowflake-feature-store
```

## How to use

### Complete LTV Example

Here’s a full example of setting up a feature store for LTV prediction:

``` python
from snowflake_feature_store.connection import get_connection
from snowflake_feature_store.manager import feature_store_session 
from snowflake_feature_store.config import (
    FeatureViewConfig, FeatureConfig, FeatureValidationConfig
)

# Connect to Snowflake
conn = get_connection()

# Create feature store session
with feature_store_session(conn, cleanup=False) as manager:
    # 1. Create Customer Entity
    manager.add_entity(
        name="CUSTOMER", 
        join_keys=["CUSTOMER_ID"],
        description="Customer entity for LTV prediction"
    )

    # 2. Configure Features
    feature_configs = {
        "LIFE_TIME_VALUE": FeatureConfig(
            name="LIFE_TIME_VALUE",
            description="Current customer value",
            validation=FeatureValidationConfig(
                null_threshold=0.1,
                range_check=True,
                min_value=0
            )
        ),
        "SESSION_LENGTH": FeatureConfig(
            name="SESSION_LENGTH",
            description="Session duration in minutes",
            validation=FeatureValidationConfig(
                null_threshold=0.3,
                range_check=True,
                min_value=0
            )
        )
    }

    # 3. Create Feature View
    config = FeatureViewConfig(
        name="customer_behavior",
        domain="RETAIL",
        entity="CUSTOMER",
        feature_type="BEHAVIOR",
        features=feature_configs,
        refresh=RefreshConfig(frequency="1 day")
    )

    # 4. Add Transformations
    transforms = [
        fill_na(['SESSION_LENGTH'], 0),
        moving_agg(
            cols=['TRANSACTIONS'],
            window_sizes=[7, 30],
            agg_funcs=['SUM', 'AVG'],
            partition_by=['CUSTOMER_ID'],
            order_by=['DATE']
        )
    ]

    # 5. Create Feature View
    feature_view = manager.add_feature_view(
        config=config,
        df=source_df,
        entity_name="CUSTOMER",
        transforms=transforms
    )

    # 6. Generate Training Data
    training_data = generate_ltv_training_data(
        manager=manager,
        feature_view=feature_view,
        training_start_date='2024-01-01',
        training_end_date='2024-03-01',
        prediction_window=90
    )
```

### Key Components

1.  **Feature Configuration**
    - Validation rules
    - Data quality checks
    - Documentation
    - Dependencies
2.  **Transformations**
    - Missing value handling
    - Window aggregations
    - Custom transforms
    - Feature combination
3.  **Monitoring**
    - Feature statistics
    - Drift detection
    - Quality metrics
    - Alert configuration
4.  **Training Data**
    - Point-in-time correctness
    - Label generation
    - Feature selection
    - Data validation

## Documentation

The library includes detailed documentation for each component:

1.  [Connection Management](./01_connection.ipynb): Snowflake connection
    setup and management
2.  [Feature Transforms](./02_transforms.ipynb): Feature engineering and
    validation
3.  [Feature Views](./03_feature_view.ipynb): Feature organization and
    versioning
4.  [Feature Store](./04_manager.ipynb): Feature store operations and
    monitoring
5.  [End-to-End Example](./06_simple_example.ipynb): Complete LTV
    prediction workflow

## Advanced Features

1.  **Feature Monitoring**

``` python
# Monitor feature drift
monitor = LTVMonitor(manager, feature_view.name) 
monitor.set_baseline(feature_view.feature_df) 
drift_metrics = monitor.check_feature_health(new_data)
```

2.  **Custom Transformations**

``` python
# Create custom transform
@transform_config(name="engagement_score")
def calculate_engagement(df): 
    return df.with_column(
        'ENGAGEMENT_SCORE', 
        (F.col('SESSION_LENGTH') + F.col('TIME_ON_APP')) / 2.0
    )
```

3.  **Point-in-Time Training**

``` python
# Generate training data
training_data = generate_ltv_training_data(
    manager=manager,
    feature_view=feature_view,
    training_start_date='2024-01-01',
    prediction_window=90
)
```

## Troubleshooting

### Common Issues

1.  **Connection problems**
    - Check Snowflake credentials and account information
    - Ensure network connectivity to Snowflake
    - Verify proper role and warehouse permissions
2.  **Missing dependencies**
    - Ensure all required libraries are installed:
      `pip install -r requirements.txt`
    - Check for version conflicts, especially with Pydantic
3.  **Schema creation failures**
    - Verify you have CREATE SCHEMA privileges
    - Check for schema name conflicts
4.  **Performance issues**
    - Consider using a larger warehouse for heavy transformations
    - Optimize window functions and partition keys
    - Review data volumes and aggregation periods

## Contributing

Contributions are welcome! Please see our [Contributing
Guide](CONTRIBUTING.md) for more information.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Installation

Install latest from the GitHub
[repository](https://github.com/Jeremy-Demlow/sf-feature-store):

``` sh
$ pip install git+https://github.com/Jeremy-Demlow/sf-feature-store.git
```

or from [pypi](https://pypi.org/project/sf-feature-store/)

``` sh
$ pip install sf_feature_store
```
