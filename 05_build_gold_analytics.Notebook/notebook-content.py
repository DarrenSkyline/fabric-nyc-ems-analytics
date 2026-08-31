# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "62afa944-791d-4816-8ef6-98a724c248a9",
# META       "default_lakehouse_name": "nyc_ems_lakehouse",
# META       "default_lakehouse_workspace_id": "e72e0909-21e6-43a8-9809-2f28d2090b05",
# META       "known_lakehouses": [
# META         {
# META           "id": "62afa944-791d-4816-8ef6-98a724c248a9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NYC EMS Gold Statistical Analytics
# 
# ## Purpose
# 
# This notebook builds daily EMS performance aggregates for statistical
# analysis, control-limit monitoring, anomaly detection and Power BI
# forecasting.
# 
# ## Grain
# 
# One row represents one incident date, borough and initial severity level.
# 
# ## Inputs
# 
# - `gold_fact_ems_incident`
# - `gold_dim_geography`
# - `gold_dim_severity`
# - `silver_ems_data_quality_audit`
# 
# ## Planned Outputs
# 
# - `gold_agg_daily_performance`
# - `gold_ems_data_quality_audit`
# 
# ## Statistical Scope
# 
# - Daily incident demand
# - Average response performance
# - Population standard deviation
# - Response-time percentiles
# - Upper and lower statistical control limits
# - Anomaly detection
# - Time-series and forecasting support

# CELL ********************

# Import PySpark component
from pyspark.sql import functions as F
from pyspark.sql import Window

# Convert seconds to minutes using decimal arithmetic
# to avoid binary floating-point rounding errors
def seconds_to_minutes(column_name):
    return (
        F.round(
            F.col(column_name)
            .cast("decimal(24, 6)")
            / F.lit(60),
            2
        )
        .cast("double")
    )

# Calculate a reporting percentage from integer counts
# using decimal arithmetic
def count_to_percentage(
    count_column,
    total_column
):
    return (
        F.when(
            F.col(total_column) > 0,
            F.round(
                (
                    F.col(count_column)
                    .cast("decimal(20, 0)")
                    * F.lit(100)
                )
                /
                F.col(total_column)
                .cast("decimal(20, 0)"),
                2
            ).cast("double")
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define table names
silver_audit_table_name = (
    "silver_ems_data_quality_audit"
)

gold_fact_table_name = (
    "gold_fact_ems_incident"
)

gold_geography_table_name = (
    "gold_dim_geography"
)

gold_severity_table_name = (
    "gold_dim_severity"
)

gold_daily_table_name = (
    "gold_agg_daily_performance"
)

gold_audit_table_name = (
    "gold_ems_data_quality_audit"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Gold tables
df_gold_fact = spark.table(
    gold_fact_table_name
)

df_dim_geography = spark.table(
    gold_geography_table_name
)

df_dim_severity = spark.table(
    gold_severity_table_name
)

print(
    f"Gold Fact columns: "
    f"{len(df_gold_fact.columns)}"
)

print(
    f"Geography rows: "
    f"{df_dim_geography.count():,}"
)

print(
    f"Severity rows: "
    f"{df_dim_severity.count():,}"
)


assert len(df_gold_fact.columns) == 29, (
    f"Expected 29 Gold Fact columns, "
    f"found {len(df_gold_fact.columns)}"
)

print("Gold analytical inputs loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get total expected incident
latest_row = (
    spark.table(silver_audit_table_name)
    .orderBy(F.col("audit_timestamp").desc())
    .first()
)

latest_silver_audit = (
    latest_row.asDict()
    if latest_row is not None
    else {}
)

assert (
    latest_silver_audit["validation_status"]
    == "PASS"
), "The latest Silver audit did not pass."

expected_incident_rows = (
    latest_silver_audit[
        "distinct_incident_ids"
    ]
)

print(
    f"Expected incident rows: "
    f"{expected_incident_rows:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare Geography and Severity Lookup
df_borough_lookup = (
    df_dim_geography
    .select(
        "geography_key",
        "borough"
    )
)

df_initial_severity_lookup = (
    df_dim_severity
    .select(
        F.col("severity_key").alias(
            "initial_severity_key"
        ),
        F.col(
            "severity_level_code"
        ).alias(
            "initial_severity_level_code"
        ),
        F.col("severity_label").alias(
            "initial_severity_label"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add analytical attributes to the Fact table
df_analytics_source = (
    df_gold_fact
    .join(
        F.broadcast(
            df_borough_lookup
        ),
        on="geography_key",
        how="left"
    )
    .join(
        F.broadcast(
            df_initial_severity_lookup
        ),
        on="initial_severity_key",
        how="left"
    )
)

# Validate the result of the connection
analytics_lookup_validation = (
    df_analytics_source
    .agg(
        F.sum(
            F.when(
                F.col("borough").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_boroughs"),

        F.sum(
            F.when(
                F.col(
                    "initial_severity_level_code"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("missing_severity_codes")
    )
    .collect()[0]
)

print(
    f"Missing borough lookups: "
    f"{analytics_lookup_validation['missing_boroughs']:,}"
)

print(
    f"Missing severity lookups: "
    f"{analytics_lookup_validation['missing_severity_codes']:,}"
)


assert (
    analytics_lookup_validation[
        "missing_boroughs"
    ] == 0
), "One or more Borough lookups failed."

assert (
    analytics_lookup_validation[
        "missing_severity_codes"
    ] == 0
), "One or more Severity lookups failed."

print(
    "Analytics dimension lookups passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Daily Performance Aggregation
# 
# Daily EMS performance is aggregated at the following grain:
# 
# > One row = one incident date × borough × initial severity level
# 
# Response-time sums, averages, percentiles and population standard
# deviations use only the validated response-time measures created in
# the Silver layer.
# 
# Response-time sums and valid-record counts are retained so Power BI
# can calculate correctly weighted averages when multiple daily groups
# are combined.
# 
# The analytical measures include:
# 
# - Valid-response rate
# - Average dispatch, incident-response and travel time
# - Population standard deviation
# - Coefficient of variation
# - P50, P75, P90 and P95 response times
# - Minimum and maximum response times
# - Held, special-event and transfer incident rates
# 
# Statistical control limits are calculated separately for each borough
# and initial severity level using the previous 30 eligible daily
# observations. The current observation is excluded from its own
# historical baseline.
# 
# A daily group is eligible for reporting when it contains at least 30
# valid incident-response records. At least 20 eligible historical
# observations are required before control limits and reportable anomaly
# flags are produced.
# 
# The upper and lower control limits use three population standard
# deviations around the historical centre line. The lower control limit
# is restricted to zero because response time cannot be negative.
# 
# Low-sample groups and insufficient historical baselines are retained
# for traceability but are not reported as formal response-time
# anomalies.


# CELL ********************

# Create daily base aggregation table
df_daily_base = (
    df_analytics_source
    .groupBy(
        "date_key",
        "incident_year",
        "borough",
        "initial_severity_key",
        "initial_severity_level_code",
        "initial_severity_label"
    )
    .agg(
        # Incident demand
        F.sum("incident_count").alias(
            "total_incidents"
        ),

        # Valid-value counts
        F.count(
            "valid_dispatch_response_seconds"
        ).alias(
            "valid_dispatch_response_count"
        ),

        F.count(
            "valid_incident_response_seconds"
        ).alias(
            "valid_incident_response_count"
        ),

        F.count(
            "valid_travel_time_seconds"
        ).alias(
            "valid_travel_time_count"
        ),

        # Additive response-time totals
        F.sum(
            "valid_dispatch_response_seconds"
        ).alias(
            "total_dispatch_response_seconds"
        ),

        F.sum(
            "valid_incident_response_seconds"
        ).alias(
            "total_incident_response_seconds"
        ),

        F.sum(
            "valid_travel_time_seconds"
        ).alias(
            "total_travel_time_seconds"
        ),

        # Average response times
        F.avg(
            "valid_dispatch_response_seconds"
        ).alias(
            "average_dispatch_response_seconds"
        ),

        F.avg(
            "valid_incident_response_seconds"
        ).alias(
            "average_incident_response_seconds"
        ),

        F.avg(
            "valid_travel_time_seconds"
        ).alias(
            "average_travel_time_seconds"
        ),

        # Daily incident-response variability
        F.stddev_pop(
            "valid_incident_response_seconds"
        ).alias(
            "population_stddev_response_seconds"
        ),

        # Range
        F.min(
            "valid_incident_response_seconds"
        ).alias(
            "minimum_response_seconds"
        ),

        F.max(
            "valid_incident_response_seconds"
        ).alias(
            "maximum_response_seconds"
        ),

        # Percentiles
        F.percentile_approx(
            "valid_incident_response_seconds",
            0.50,
            10000
        ).alias("p50_response_seconds"),

        F.percentile_approx(
            "valid_incident_response_seconds",
            0.75,
            10000
        ).alias("p75_response_seconds"),

        F.percentile_approx(
            "valid_incident_response_seconds",
            0.90,
            10000
        ).alias("p90_response_seconds"),

        F.percentile_approx(
            "valid_incident_response_seconds",
            0.95,
            10000
        ).alias("p95_response_seconds"),

        # Operational indicators
        F.sum(
            F.when(
                F.col("is_held") == True,
                1
            ).otherwise(0)
        ).alias("held_incident_count"),

        F.sum(
            F.when(
                F.col("is_special_event") == True,
                1
            ).otherwise(0)
        ).alias("special_event_count"),

        F.sum(
            F.when(
                F.col("is_transfer") == True,
                1
            ).otherwise(0)
        ).alias("transfer_incident_count")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add a date field
df_daily_base = (
    df_daily_base
    .withColumn(
        "incident_date",
        F.to_date(
            F.col("date_key").cast("string"),
            "yyyyMMdd"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add valid response rate
df_daily_base = (
    df_daily_base
    .withColumn(
        "valid_response_rate",
        F.when(
            F.col("total_incidents") > 0,
            (
                F.col(
                    "valid_incident_response_count"
                )
                / F.col("total_incidents")
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reorder fields
df_daily_base = (
    df_daily_base
    .select(
        # Grain
        "date_key",
        "incident_date",
        "incident_year",
        "borough",
        "initial_severity_key",
        "initial_severity_level_code",
        "initial_severity_label",

        # Demand
        "total_incidents",

        # Validity
        "valid_dispatch_response_count",
        "valid_incident_response_count",
        "valid_travel_time_count",
        "valid_response_rate",

        # Additive totals
        "total_dispatch_response_seconds",
        "total_incident_response_seconds",
        "total_travel_time_seconds",

        # Averages
        "average_dispatch_response_seconds",
        "average_incident_response_seconds",
        "average_travel_time_seconds",

        # Variability and range
        "population_stddev_response_seconds",
        "minimum_response_seconds",
        "maximum_response_seconds",

        # Percentiles
        "p50_response_seconds",
        "p75_response_seconds",
        "p90_response_seconds",
        "p95_response_seconds",

        # Operational counts
        "held_incident_count",
        "special_event_count",
        "transfer_incident_count"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View basic aggregation results
print(
    f"Daily base column count: "
    f"{len(df_daily_base.columns)}"
)

display(
    df_daily_base
    .orderBy(
        "incident_date",
        "borough",
        "initial_severity_level_code"
    )
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that no events were lost during daily aggregation
daily_base_validation = (
    df_daily_base
    .agg(
        F.count("*").alias(
            "daily_group_rows"
        ),

        F.sum("total_incidents").alias(
            "aggregated_incident_count"
        ),

        F.sum(
            F.when(
                F.col("date_key").isNull()
                | F.col("borough").isNull()
                | F.col(
                    "initial_severity_key"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_grain_rows"),

        F.sum(
            F.when(
                F.col("total_incidents") <= 0,
                1
            ).otherwise(0)
        ).alias("non_positive_incident_groups"),

        F.sum(
            F.when(
                (F.col("valid_response_rate") < 0)
                | (F.col("valid_response_rate") > 1),
                1
            ).otherwise(0)
        ).alias("invalid_response_rates")
    )
    .collect()[0]
)

for field_name, field_value in (
    daily_base_validation.asDict().items()
):
    print(
        f"{field_name}: "
        f"{field_value:,}"
    )


assert (
    daily_base_validation[
        "aggregated_incident_count"
    ] == expected_incident_rows
), (
    "Daily aggregate incident count does not "
    "match the validated incident count."
)

assert (
    daily_base_validation[
        "invalid_grain_rows"
    ] == 0
), "Daily aggregate contains invalid grain values."

assert (
    daily_base_validation[
        "non_positive_incident_groups"
    ] == 0
), "Daily aggregate contains empty groups."

assert (
    daily_base_validation[
        "invalid_response_rates"
    ] == 0
), "Daily aggregate contains invalid response rates."

print(
    "Daily performance base validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Confirm basic fields
print(df_daily_base.columns)
df_daily_base.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate daily derived metrics
df_daily_metrics = (
    df_daily_base

    # Valid-response percentage
    .withColumn(
        "valid_response_percentage",
        F.round(
            F.col("valid_response_rate") * 100.0,
            2
        )
    )

    # Average response measures in minutes
    .withColumn(
        "average_dispatch_response_minutes",
        seconds_to_minutes(
            "average_dispatch_response_seconds"
        )
    )
    .withColumn(
        "average_incident_response_minutes",
        seconds_to_minutes(
            "average_incident_response_seconds"
        )
    )
    .withColumn(
        "average_travel_time_minutes",
        seconds_to_minutes(
            "average_travel_time_seconds"
        )
    )

    # Response-time variability in minutes
    .withColumn(
        "population_stddev_response_minutes",
        seconds_to_minutes(
            "population_stddev_response_seconds"
        )
    )

    # Percentiles in minutes
    .withColumn(
        "p50_response_minutes",
        seconds_to_minutes(
            "p50_response_seconds"
        )
    )
    .withColumn(
        "p75_response_minutes",
        seconds_to_minutes(
            "p75_response_seconds"
        )
    )
    .withColumn(
        "p90_response_minutes",
        seconds_to_minutes(
            "p90_response_seconds"
        )
    )
    .withColumn(
        "p95_response_minutes",
        seconds_to_minutes(
            "p95_response_seconds"
        )
    )

    # Minimum and maximum response times in minutes
    .withColumn(
        "minimum_response_minutes",
        seconds_to_minutes(
            "minimum_response_seconds"
        )
    )
    .withColumn(
        "maximum_response_minutes",
        seconds_to_minutes(
            "maximum_response_seconds"
        )
    )

    # Coefficient of variation
    .withColumn(
        "response_coefficient_of_variation",
        F.when(
            F.col(
                "average_incident_response_seconds"
            ) > 0,
            F.col(
                "population_stddev_response_seconds"
            )
            / F.col(
                "average_incident_response_seconds"
            )
        )
    )
    .withColumn(
        "response_cv_percentage",
        F.round(
            F.col(
                "response_coefficient_of_variation"
            )
            .cast("decimal(38, 10)")
            * F.lit(100),
            2
        ).cast("double")
    )

    # Operational incident rates
    .withColumn(
        "held_incident_rate",
        F.when(
            F.col("total_incidents") > 0,
            F.col("held_incident_count")
            / F.col("total_incidents")
        )
    )
    .withColumn(
        "special_event_rate",
        F.when(
            F.col("total_incidents") > 0,
            F.col("special_event_count")
            / F.col("total_incidents")
        )
    )
    .withColumn(
        "transfer_incident_rate",
        F.when(
            F.col("total_incidents") > 0,
            F.col("transfer_incident_count")
            / F.col("total_incidents")
        )
    )

    # Operational percentages for reporting
    .withColumn(
        "held_incident_percentage",
        count_to_percentage(
            "held_incident_count",
            "total_incidents"
        )
    )
    .withColumn(
        "special_event_percentage",
        count_to_percentage(
            "special_event_count",
            "total_incidents"
        )
    )
    .withColumn(
        "transfer_incident_percentage",
        count_to_percentage(
            "transfer_incident_count",
            "total_incidents"
        )
    )
)

# Validate result
display(
    df_daily_metrics
    .select(
        "incident_date",
        "borough",
        "initial_severity_level_code",
        "total_incidents",
        "valid_incident_response_count",
        "valid_response_percentage",
        "average_dispatch_response_minutes",
        "average_incident_response_minutes",
        "average_travel_time_minutes",
        "population_stddev_response_minutes",
        "response_cv_percentage",
        "p50_response_minutes",
        "p90_response_minutes",
        "p95_response_minutes",
        "held_incident_percentage",
        "special_event_percentage",
        "transfer_incident_percentage"
    )
    .orderBy(
        F.col("incident_date").desc(),
        "borough",
        "initial_severity_level_code"
    )
)

derived_metric_validation = (
    df_daily_metrics
    .agg(
        F.min("valid_response_percentage").alias(
            "minimum_valid_response_percentage"
        ),
        F.max("valid_response_percentage").alias(
            "maximum_valid_response_percentage"
        ),
        F.min("response_cv_percentage").alias(
            "minimum_cv_percentage"
        ),
        F.max("response_cv_percentage").alias(
            "maximum_cv_percentage"
        )
    )
)

display(derived_metric_validation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate operational reporting percentages
operational_percentage_validation = (
    df_daily_metrics
    .agg(
        F.sum(
            F.when(
                ~F.col(
                    "held_incident_percentage"
                ).eqNullSafe(
                    count_to_percentage(
                        "held_incident_count",
                        "total_incidents"
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "held_percentage_mismatch_rows"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "special_event_percentage"
                ).eqNullSafe(
                    count_to_percentage(
                        "special_event_count",
                        "total_incidents"
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "special_event_percentage_mismatch_rows"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "transfer_incident_percentage"
                ).eqNullSafe(
                    count_to_percentage(
                        "transfer_incident_count",
                        "total_incidents"
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "transfer_percentage_mismatch_rows"
        )
    )
    .collect()[0]
)

for field_name, field_value in (
    operational_percentage_validation
    .asDict()
    .items()
):
    print(
        f"{field_name}: "
        f"{field_value:,}"
    )

assert all(
    field_value == 0
    for field_value in (
        operational_percentage_validation
        .asDict()
        .values()
    )
), (
    "One or more operational percentages "
    "are invalid."
)

print(
    "Operational percentage validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate coefficient-of-variation percentage
cv_percentage_validation = (
    df_daily_metrics
    .filter(
        ~F.col(
            "response_cv_percentage"
        ).eqNullSafe(
            F.round(
                F.col(
                    "response_coefficient_of_variation"
                )
                .cast("decimal(38, 10)")
                * F.lit(100),
                2
            ).cast("double")
        )
    )
    .count()
)

print(
    f"CV percentage mismatch rows: "
    f"{cv_percentage_validation:,}"
)

assert cv_percentage_validation == 0, (
    "Coefficient-of-variation percentage "
    "validation failed."
)

print(
    "Coefficient-of-variation percentage "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add sample quality field
minimum_daily_sample_size = 30

df_daily_metrics = (
    df_daily_metrics
    .withColumn(
        "has_sufficient_sample",
        F.col("valid_incident_response_count")
        >= F.lit(minimum_daily_sample_size)
    )
    .withColumn(
        "analytics_quality_status",
        F.when(
            F.col("valid_incident_response_count") == 0,
            F.lit("NO_VALID_RESPONSE")
        )
        .when(
            F.col("valid_incident_response_count")
            < minimum_daily_sample_size,
            F.lit("LOW_SAMPLE_SIZE")
        )
        .otherwise(
            F.lit("SUFFICIENT_SAMPLE")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define a historical rolling window
from pyspark.sql.window import Window

historical_control_window = (
    Window
    .partitionBy(
        "borough",
        "initial_severity_key"
    )
    .orderBy("incident_date")
    .rowsBetween(-30, -1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate historical center lines and standard deviations
# Only include dates with sufficient sample sizes in the historical baseline
baseline_response_expression = F.when(
    F.col("has_sufficient_sample"),
    F.col("average_incident_response_seconds")
)

df_daily_metrics = (
    df_daily_metrics
    .withColumn(
        "historical_baseline_day_count",
        F.count(
            baseline_response_expression
        ).over(historical_control_window)
    )
    .withColumn(
        "response_center_line_seconds",
        F.avg(
            baseline_response_expression
        ).over(historical_control_window)
    )
    .withColumn(
        "daily_average_stddev_seconds",
        F.stddev_pop(
            baseline_response_expression
        ).over(historical_control_window)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate control limits
minimum_baseline_days = 20

df_daily_metrics = (
    df_daily_metrics
    .withColumn(
        "has_sufficient_baseline",
        (
            F.col("historical_baseline_day_count")
            >= F.lit(minimum_baseline_days)
        )
        & F.col("daily_average_stddev_seconds").isNotNull()
    )
    .withColumn(
        "upper_control_limit_seconds",
        F.when(
            F.col("has_sufficient_baseline"),
            F.col("response_center_line_seconds")
            + (
                F.lit(3.0)
                * F.col("daily_average_stddev_seconds")
            )
        )
    )
    .withColumn(
        "lower_control_limit_seconds",
        F.when(
            F.col("has_sufficient_baseline"),
            F.greatest(
                F.lit(0.0),
                F.col("response_center_line_seconds")
                - (
                    F.lit(3.0)
                    * F.col("daily_average_stddev_seconds")
                )
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create exception field
df_daily_metrics = (
    df_daily_metrics
    .withColumn(
        "is_above_upper_control_limit",
        F.when(
            F.col("has_sufficient_baseline"),
            F.col("average_incident_response_seconds")
            > F.col("upper_control_limit_seconds")
        ).otherwise(False)
    )
    .withColumn(
        "is_below_lower_control_limit",
        F.when(
            F.col("has_sufficient_baseline"),
            F.col("average_incident_response_seconds")
            < F.col("lower_control_limit_seconds")
        ).otherwise(False)
    )
    .withColumn(
        "is_response_anomaly",
        F.col("is_above_upper_control_limit")
        | F.col("is_below_lower_control_limit")
    )
    .withColumn(
        "response_anomaly_direction",
        F.when(
            ~F.col("has_sufficient_baseline"),
            F.lit("Insufficient Baseline")
        )
        .when(
            F.col("is_above_upper_control_limit"),
            F.lit("Above UCL")
        )
        .when(
            F.col("is_below_lower_control_limit"),
            F.lit("Below LCL")
        )
        .otherwise(
            F.lit("Within Control Limits")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Formal Report Anomaly Markers
df_daily_metrics = (
    df_daily_metrics
    .withColumn(
        "is_reportable_response_anomaly",
        F.col("has_sufficient_sample")
        & F.col("has_sufficient_baseline")
        & F.col("is_response_anomaly")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert control lines into minutes using decimal arithmetic
df_daily_metrics = (
    df_daily_metrics
    .withColumn(
        "response_center_line_minutes",
        seconds_to_minutes(
            "response_center_line_seconds"
        )
    )
    .withColumn(
        "upper_control_limit_minutes",
        seconds_to_minutes(
            "upper_control_limit_seconds"
        )
    )
    .withColumn(
        "lower_control_limit_minutes",
        seconds_to_minutes(
            "lower_control_limit_seconds"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate all seconds-to-minutes conversions
minute_conversion_pairs = [
    (
        "average_dispatch_response_seconds",
        "average_dispatch_response_minutes"
    ),
    (
        "average_incident_response_seconds",
        "average_incident_response_minutes"
    ),
    (
        "average_travel_time_seconds",
        "average_travel_time_minutes"
    ),
    (
        "population_stddev_response_seconds",
        "population_stddev_response_minutes"
    ),
    (
        "p50_response_seconds",
        "p50_response_minutes"
    ),
    (
        "p75_response_seconds",
        "p75_response_minutes"
    ),
    (
        "p90_response_seconds",
        "p90_response_minutes"
    ),
    (
        "p95_response_seconds",
        "p95_response_minutes"
    ),
    (
        "minimum_response_seconds",
        "minimum_response_minutes"
    ),
    (
        "maximum_response_seconds",
        "maximum_response_minutes"
    ),
    (
        "response_center_line_seconds",
        "response_center_line_minutes"
    ),
    (
        "upper_control_limit_seconds",
        "upper_control_limit_minutes"
    ),
    (
        "lower_control_limit_seconds",
        "lower_control_limit_minutes"
    )
]

minute_conversion_checks = []

for seconds_column, minutes_column in minute_conversion_pairs:
    minute_conversion_checks.append(
        F.sum(
            F.when(
                ~F.col(minutes_column).eqNullSafe(
                    seconds_to_minutes(seconds_column)
                ),
                1
            ).otherwise(0)
        ).alias(
            f"{minutes_column}_mismatch_count"
        )
    )

minute_conversion_validation = (
    df_daily_metrics
    .agg(*minute_conversion_checks)
    .collect()[0]
)

for field_name, field_value in (
    minute_conversion_validation
    .asDict()
    .items()
):
    print(
        f"{field_name}: "
        f"{field_value:,}"
    )

assert all(
    field_value == 0
    for field_value in (
        minute_conversion_validation
        .asDict()
        .values()
    )
), (
    "One or more seconds-to-minutes "
    "conversions are invalid."
)

print(
    "Seconds-to-minutes conversion "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View results
display(
    df_daily_metrics
    .select(
        "date_key",
        "incident_date",
        "incident_year",
        "borough",
        "initial_severity_key",
        "initial_severity_level_code",
        "initial_severity_label",

        "total_incidents",
        "valid_incident_response_count",
        "valid_response_percentage",

        "average_incident_response_minutes",
        "population_stddev_response_minutes",
        "response_cv_percentage",

        "p50_response_minutes",
        "p75_response_minutes",
        "p90_response_minutes",
        "p95_response_minutes",

        "historical_baseline_day_count",
        "response_center_line_minutes",
        "lower_control_limit_minutes",
        "upper_control_limit_minutes",

        "has_sufficient_sample",
        "has_sufficient_baseline",
        "is_response_anomaly",
        "is_reportable_response_anomaly",
        "response_anomaly_direction",
        "analytics_quality_status"
    )
    .orderBy(
        F.col("incident_date").desc(),
        "borough",
        "initial_severity_level_code"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify final metrics
daily_metric_validation = (
    df_daily_metrics
    .agg(
        F.count("*").alias("total_daily_rows"),

        F.min(
            "valid_response_percentage"
        ).alias("minimum_valid_response_percentage"),

        F.max(
            "valid_response_percentage"
        ).alias("maximum_valid_response_percentage"),

        F.sum(
            F.when(
                F.col("analytics_quality_status")
                == "LOW_SAMPLE_SIZE",
                1
            ).otherwise(0)
        ).alias("low_sample_rows"),

        F.sum(
            F.when(
                ~F.col("has_sufficient_baseline"),
                1
            ).otherwise(0)
        ).alias("insufficient_baseline_rows"),

        F.sum(
            F.when(
                F.col("is_response_anomaly"),
                1
            ).otherwise(0)
        ).alias("statistical_anomaly_rows"),

        F.sum(
            F.when(
                F.col("is_reportable_response_anomaly"),
                1
            ).otherwise(0)
        ).alias("reportable_anomaly_rows")
    )
)

display(daily_metric_validation)


daily_metric_validation_result = (
    daily_metric_validation.collect()[0]
)

assert (
    daily_metric_validation_result[
        "minimum_valid_response_percentage"
    ] >= 0
), "Valid-response percentage is below zero."

assert (
    daily_metric_validation_result[
        "maximum_valid_response_percentage"
    ] <= 100
), "Valid-response percentage exceeds 100."

print("Daily derived-metric validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate control limits and anomaly indicators
control_limit_validation = (
    df_daily_metrics
    .agg(
        F.count("*").alias(
            "total_daily_rows"
        ),

        F.sum(
            F.when(
                F.col("has_sufficient_sample"),
                1
            ).otherwise(0)
        ).alias(
            "sufficient_sample_rows"
        ),

        F.sum(
            F.when(
                F.col("has_sufficient_baseline"),
                1
            ).otherwise(0)
        ).alias(
            "sufficient_baseline_rows"
        ),

        F.sum(
            F.when(
                F.col("is_response_anomaly"),
                1
            ).otherwise(0)
        ).alias(
            "statistical_anomaly_rows"
        ),

        F.sum(
            F.when(
                F.col(
                    "is_reportable_response_anomaly"
                ),
                1
            ).otherwise(0)
        ).alias(
            "reportable_anomaly_rows"
        ),

        # Control-limit consistency
        F.sum(
            F.when(
                F.col("has_sufficient_baseline")
                & (
                    (
                        F.col(
                            "lower_control_limit_seconds"
                        ) < 0
                    )
                    |
                    (
                        F.col(
                            "lower_control_limit_seconds"
                        )
                        > F.col(
                            "response_center_line_seconds"
                        )
                    )
                    |
                    (
                        F.col(
                            "upper_control_limit_seconds"
                        )
                        < F.col(
                            "response_center_line_seconds"
                        )
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "invalid_control_limit_rows"
        ),

        # Reportable anomalies must satisfy both rules
        F.sum(
            F.when(
                F.col(
                    "is_reportable_response_anomaly"
                )
                & (
                    ~F.col("has_sufficient_sample")
                    |
                    ~F.col("has_sufficient_baseline")
                ),
                1
            ).otherwise(0)
        ).alias(
            "invalid_reportable_anomaly_rows"
        ),

        F.min(
            "historical_baseline_day_count"
        ).alias(
            "minimum_baseline_day_count"
        ),

        F.max(
            "historical_baseline_day_count"
        ).alias(
            "maximum_baseline_day_count"
        )
    )
)

display(control_limit_validation)


control_validation_result = (
    control_limit_validation.collect()[0]
)

assert (
    control_validation_result[
        "total_daily_rows"
    ] > 0
), "Daily analytics table contains no rows."

assert (
    control_validation_result[
        "invalid_control_limit_rows"
    ] == 0
), "One or more control limits are invalid."

assert (
    control_validation_result[
        "invalid_reportable_anomaly_rows"
    ] == 0
), (
    "A reportable anomaly does not satisfy "
    "the sample or baseline requirements."
)

assert (
    control_validation_result[
        "maximum_baseline_day_count"
    ] <= 30
), "Historical window contains more than 30 observations."

assert (
    control_validation_result[
        "reportable_anomaly_rows"
    ]
    <= control_validation_result[
        "statistical_anomaly_rows"
    ]
), (
    "Reportable anomaly count exceeds "
    "the statistical anomaly count."
)

print("Control-limit and anomaly validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View official anomaly samples
display(
    df_daily_metrics
    .filter(
        F.col("is_reportable_response_anomaly")
    )
    .select(
        "incident_date",
        "borough",
        "initial_severity_level_code",
        "initial_severity_label",
        "total_incidents",
        "valid_incident_response_count",
        "average_incident_response_minutes",
        "response_center_line_minutes",
        "lower_control_limit_minutes",
        "upper_control_limit_minutes",
        "response_anomaly_direction"
    )
    .orderBy(
        F.col(
            "average_incident_response_minutes"
        ).desc()
    )
    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Persist Daily Performance Analytics
# 
# The validated daily EMS performance dataset is written to the
# `gold_agg_daily_performance` Delta table.
# 
# The table has the following grain:
# 
# > One row = one incident date × borough × initial severity level
# 
# The table is partitioned by `incident_year` and provides reusable
# measures for SQL analytics, the Power BI semantic model, statistical
# control charts and anomaly reporting.
# 
# Because the analytical table is fully rebuilt from the validated Gold
# fact table, each successful notebook run replaces the previous table.

# CELL ********************

# Define the table name and expected number of rows
gold_daily_table_name = (
    "gold_agg_daily_performance"
)

expected_daily_rows = (
    control_validation_result[
        "total_daily_rows"
    ]
)

print(
    f"Target table: {gold_daily_table_name}"
)

print(
    f"Expected daily rows: "
    f"{expected_daily_rows:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enable Fabric Delta write optimisations
spark.conf.set(
    "spark.microsoft.delta.optimizeWrite.enabled",
    "true"
)

spark.conf.set(
    "spark.microsoft.delta.autoCompact.enabled",
    "true"
)

# Write the Gold daily-performance table
(
    df_daily_metrics
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("incident_year")
    .saveAsTable(gold_daily_table_name)
)

print(
    f"Successfully created: "
    f"{gold_daily_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reread the saved table
df_gold_daily_saved = spark.table(
    gold_daily_table_name
)

print(
    f"Successfully loaded: "
    f"{gold_daily_table_name}"
)

print(
    f"Column count: "
    f"{len(df_gold_daily_saved.columns)}"
)

df_gold_daily_saved.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the number of persisted rows and the business granularity
saved_daily_rows = (
    df_gold_daily_saved.count()
)

# Check for duplicate business combinations
duplicate_daily_groups = (
    df_gold_daily_saved
    .groupBy(
        "date_key",
        "borough",
        "initial_severity_key"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

# Show the result
print(
    f"Expected daily rows: "
    f"{expected_daily_rows:,}"
)

print(
    f"Saved daily rows: "
    f"{saved_daily_rows:,}"
)

print(
    f"Duplicate daily groups: "
    f"{duplicate_daily_groups:,}"
)


assert (
    saved_daily_rows
    == expected_daily_rows
), (
    f"Expected {expected_daily_rows:,} rows, "
    f"but saved {saved_daily_rows:,} rows."
)

assert (
    duplicate_daily_groups == 0
), (
    "Duplicate date, borough and severity "
    "groups were found."
)

print(
    "Saved row-count and grain validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the total number of aggregated events
gold_fact_table_name = (
    "gold_fact_ems_incident"
)

fact_total_rows = (
    spark.table(gold_fact_table_name)
    .agg(
        F.count("*").alias("total_rows")
    )
    .collect()[0]["total_rows"]
)

aggregated_total_incidents = (
    df_gold_daily_saved
    .agg(
        F.sum("total_incidents").alias(
            "total_incidents"
        )
    )
    .collect()[0]["total_incidents"]
)

print(
    f"Gold fact rows: "
    f"{fact_total_rows:,}"
)

print(
    f"Aggregated incidents: "
    f"{aggregated_total_incidents:,}"
)

assert (
    aggregated_total_incidents
    == fact_total_rows
), (
    "Daily aggregated incident total does "
    "not match the Gold fact table."
)

print(
    "Incident-total reconciliation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the saved control limits and exception fields
saved_analytics_validation = (
    df_gold_daily_saved
    .agg(
        F.sum(
            F.when(
                F.col("has_sufficient_baseline")
                & (
                    F.col(
                        "lower_control_limit_seconds"
                    ) < 0
                ),
                1
            ).otherwise(0)
        ).alias(
            "negative_lower_control_limits"
        ),

        F.sum(
            F.when(
                F.col(
                    "is_reportable_response_anomaly"
                )
                & (
                    ~F.col("has_sufficient_sample")
                    |
                    ~F.col(
                        "has_sufficient_baseline"
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "invalid_reportable_anomalies"
        ),

        F.sum(
            F.when(
                F.col(
                    "is_reportable_response_anomaly"
                ),
                1
            ).otherwise(0)
        ).alias(
            "reportable_anomaly_rows"
        )
    )
    .collect()[0]
)

for field_name, field_value in (
    saved_analytics_validation
    .asDict()
    .items()
):
    print(
        f"{field_name}: "
        f"{field_value:,}"
    )


assert (
    saved_analytics_validation[
        "negative_lower_control_limits"
    ] == 0
), "Negative lower control limits were saved."

assert (
    saved_analytics_validation[
        "invalid_reportable_anomalies"
    ] == 0
), "Invalid reportable anomalies were saved."

print(
    "Saved analytical-field validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Delta table properties
display(
    spark.sql(
        f"DESCRIBE DETAIL "
        f"{gold_daily_table_name}"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View the final sample
display(
    df_gold_daily_saved
    .select(
        "incident_date",
        "borough",
        "initial_severity_label",
        "total_incidents",
        "valid_response_percentage",
        "average_incident_response_minutes",
        "population_stddev_response_minutes",
        "p90_response_minutes",
        "p95_response_minutes",
        "response_center_line_minutes",
        "lower_control_limit_minutes",
        "upper_control_limit_minutes",
        "is_reportable_response_anomaly",
        "response_anomaly_direction",
        "analytics_quality_status"
    )
    .orderBy(
        F.col("incident_date").desc(),
        "borough",
        "initial_severity_level_code"
    )
    .limit(100)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Data Quality Audit
# 
# The Gold audit table records the validation results for the completed
# dimensional, fact and analytical layers.
# 
# Each audit record contains:
# 
# - Gold dimension row counts
# - Gold fact-table row count
# - Daily analytical-table row count
# - Fact-to-aggregation reconciliation result
# - Duplicate daily-grain count
# - Statistical and reportable anomaly counts
# - Small-sample and insufficient-baseline counts
# - Control-limit validation results
# - Overall validation status
# 
# The audit table uses append mode so that each successful pipeline run
# is retained as a separate historical audit record.

# CELL ********************

# Define Gold table names
gold_audit_table_name = (
    "gold_ems_data_quality_audit"
)

gold_dimension_tables = {
    "date_dimension_rows": "gold_dim_date",
    "time_dimension_rows": "gold_dim_time",
    "geography_dimension_rows": "gold_dim_geography",
    "call_type_dimension_rows": "gold_dim_call_type",
    "severity_dimension_rows": "gold_dim_severity",
    "disposition_dimension_rows": (
        "gold_dim_disposition"
    )
}

gold_fact_table_name = (
    "gold_fact_ems_incident"
)

gold_daily_table_name = (
    "gold_agg_daily_performance"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the final persistent table
df_gold_fact_audit_source = spark.table(
    gold_fact_table_name
)

df_gold_daily_audit_source = spark.table(
    gold_daily_table_name
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Count the number of rows in the dimension table
dimension_row_counts = {}

for (
    audit_field_name,
    dimension_table_name
) in gold_dimension_tables.items():

    dimension_count = (
        spark.table(dimension_table_name)
        .count()
    )

    dimension_row_counts[
        audit_field_name
    ] = dimension_count

    print(
        f"{dimension_table_name}: "
        f"{dimension_count:,}"
    )


assert all(
    row_count > 0
    for row_count
    in dimension_row_counts.values()
), "One or more Gold dimensions are empty."

print("Gold dimension row-count validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summarize Fact Table
gold_fact_summary = (
    df_gold_fact_audit_source
    .agg(
        F.count("*").alias(
            "fact_row_count"
        ),

        F.countDistinct("incident_id").alias(
            "distinct_incident_ids"
        ),

        F.sum(
            F.when(
                F.col("incident_id").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "missing_incident_ids"
        )
    )
    .collect()[0]
)

fact_row_count = (
    gold_fact_summary["fact_row_count"]
)

distinct_incident_ids = (
    gold_fact_summary[
        "distinct_incident_ids"
    ]
)

missing_incident_ids = (
    gold_fact_summary[
        "missing_incident_ids"
    ]
)

duplicate_incident_ids = (
    fact_row_count
    - distinct_incident_ids
)

print(
    f"Fact rows: "
    f"{fact_row_count:,}"
)

print(
    f"Distinct incident IDs: "
    f"{distinct_incident_ids:,}"
)

print(
    f"Missing incident IDs: "
    f"{missing_incident_ids:,}"
)

print(
    f"Duplicate incident IDs: "
    f"{duplicate_incident_ids:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compile statistics for the Daily Analytics table
gold_daily_summary = (
    df_gold_daily_audit_source
    .agg(
        F.count("*").alias(
            "daily_row_count"
        ),

        F.sum("total_incidents").alias(
            "aggregated_incident_count"
        ),

        F.sum(
            F.when(
                F.col("analytics_quality_status")
                == "LOW_SAMPLE_SIZE",
                1
            ).otherwise(0)
        ).alias(
            "low_sample_row_count"
        ),

        F.sum(
            F.when(
                ~F.col("has_sufficient_baseline"),
                1
            ).otherwise(0)
        ).alias(
            "insufficient_baseline_row_count"
        ),

        F.sum(
            F.when(
                F.col("is_response_anomaly"),
                1
            ).otherwise(0)
        ).alias(
            "statistical_anomaly_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "is_reportable_response_anomaly"
                ),
                1
            ).otherwise(0)
        ).alias(
            "reportable_anomaly_count"
        ),

        F.sum(
            F.when(
                F.col("has_sufficient_baseline")
                & (
                    (
                        F.col(
                            "lower_control_limit_seconds"
                        ) < 0
                    )
                    |
                    (
                        F.col(
                            "lower_control_limit_seconds"
                        )
                        > F.col(
                            "response_center_line_seconds"
                        )
                    )
                    |
                    (
                        F.col(
                            "upper_control_limit_seconds"
                        )
                        < F.col(
                            "response_center_line_seconds"
                        )
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "invalid_control_limit_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "is_reportable_response_anomaly"
                )
                & (
                    ~F.col("has_sufficient_sample")
                    |
                    ~F.col(
                        "has_sufficient_baseline"
                    )
                ),
                1
            ).otherwise(0)
        ).alias(
            "invalid_reportable_anomaly_count"
        )
    )
    .collect()[0]
)

# Convert the result to Dict
gold_daily_audit = (
    gold_daily_summary.asDict()
)

for (
    field_name,
    field_value
) in gold_daily_audit.items():

    print(
        f"{field_name}: "
        f"{field_value:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for duplicate Daily Grains
duplicate_daily_group_count = (
    df_gold_daily_audit_source
    .groupBy(
        "date_key",
        "borough",
        "initial_severity_key"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Duplicate daily groups: "
    f"{duplicate_daily_group_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate the status of each verification item
fact_identifier_validation_passed = (
    fact_row_count > 0
    and missing_incident_ids == 0
    and duplicate_incident_ids == 0
)

fact_reconciliation_passed = (
    gold_daily_audit[
        "aggregated_incident_count"
    ]
    == fact_row_count
)

daily_grain_validation_passed = (
    gold_daily_audit[
        "daily_row_count"
    ] > 0
    and duplicate_daily_group_count == 0
)

control_limit_validation_passed = (
    gold_daily_audit[
        "invalid_control_limit_count"
    ] == 0
)

anomaly_validation_passed = (
    gold_daily_audit[
        "invalid_reportable_anomaly_count"
    ] == 0
    and (
        gold_daily_audit[
            "reportable_anomaly_count"
        ]
        <= gold_daily_audit[
            "statistical_anomaly_count"
        ]
    )
)

dimension_validation_passed = all(
    row_count > 0
    for row_count
    in dimension_row_counts.values()
)

gold_audit_status = (
    "PASS"
    if all([
        dimension_validation_passed,
        fact_identifier_validation_passed,
        fact_reconciliation_passed,
        daily_grain_validation_passed,
        control_limit_validation_passed,
        anomaly_validation_passed
    ])
    else "FAIL"
)

print(
    f"Gold audit status: "
    f"{gold_audit_status}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add a final assertion
assert (
    gold_audit_status == "PASS"
), (
    "Gold data-quality validation failed. "
    "The audit record will not be written."
)

print(
    "Gold data-quality validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create audit record
import uuid

gold_audit_run_id = str(
    uuid.uuid4()
)

df_gold_audit_record = (
    spark.range(1)
    .select(
        F.lit(gold_audit_run_id)
        .alias("audit_run_id"),

        F.current_timestamp()
        .alias("audited_at"),

        F.lit(gold_fact_table_name)
        .alias("fact_table"),

        F.lit(gold_daily_table_name)
        .alias("analytics_table"),

        F.lit(
            dimension_row_counts[
                "date_dimension_rows"
            ]
        ).cast("long")
        .alias("date_dimension_rows"),

        F.lit(
            dimension_row_counts[
                "time_dimension_rows"
            ]
        ).cast("long")
        .alias("time_dimension_rows"),

        F.lit(
            dimension_row_counts[
                "geography_dimension_rows"
            ]
        ).cast("long")
        .alias("geography_dimension_rows"),

        F.lit(
            dimension_row_counts[
                "call_type_dimension_rows"
            ]
        ).cast("long")
        .alias("call_type_dimension_rows"),

        F.lit(
            dimension_row_counts[
                "severity_dimension_rows"
            ]
        ).cast("long")
        .alias("severity_dimension_rows"),

        F.lit(
            dimension_row_counts[
                "disposition_dimension_rows"
            ]
        ).cast("long")
        .alias("disposition_dimension_rows"),

        F.lit(fact_row_count)
        .cast("long")
        .alias("fact_row_count"),

        F.lit(distinct_incident_ids)
        .cast("long")
        .alias("distinct_incident_ids"),

        F.lit(missing_incident_ids)
        .cast("long")
        .alias("missing_incident_ids"),

        F.lit(duplicate_incident_ids)
        .cast("long")
        .alias("duplicate_incident_ids"),

        F.lit(
            gold_daily_audit[
                "daily_row_count"
            ]
        ).cast("long")
        .alias("daily_row_count"),

        F.lit(
            gold_daily_audit[
                "aggregated_incident_count"
            ]
        ).cast("long")
        .alias("aggregated_incident_count"),

        F.lit(duplicate_daily_group_count)
        .cast("long")
        .alias("duplicate_daily_group_count"),

        F.lit(
            gold_daily_audit[
                "low_sample_row_count"
            ]
        ).cast("long")
        .alias("low_sample_row_count"),

        F.lit(
            gold_daily_audit[
                "insufficient_baseline_row_count"
            ]
        ).cast("long")
        .alias("insufficient_baseline_row_count"),

        F.lit(
            gold_daily_audit[
                "statistical_anomaly_count"
            ]
        ).cast("long")
        .alias("statistical_anomaly_count"),

        F.lit(
            gold_daily_audit[
                "reportable_anomaly_count"
            ]
        ).cast("long")
        .alias("reportable_anomaly_count"),

        F.lit(
            gold_daily_audit[
                "invalid_control_limit_count"
            ]
        ).cast("long")
        .alias("invalid_control_limit_count"),

        F.lit(
            gold_daily_audit[
                "invalid_reportable_anomaly_count"
            ]
        ).cast("long")
        .alias(
            "invalid_reportable_anomaly_count"
        ),

        F.lit(dimension_validation_passed)
        .cast("boolean")
        .alias("dimension_validation_passed"),

        F.lit(
            fact_identifier_validation_passed
        ).cast("boolean")
        .alias(
            "fact_identifier_validation_passed"
        ),

        F.lit(fact_reconciliation_passed)
        .cast("boolean")
        .alias("fact_reconciliation_passed"),

        F.lit(daily_grain_validation_passed)
        .cast("boolean")
        .alias(
            "daily_grain_validation_passed"
        ),

        F.lit(control_limit_validation_passed)
        .cast("boolean")
        .alias(
            "control_limit_validation_passed"
        ),

        F.lit(anomaly_validation_passed)
        .cast("boolean")
        .alias("anomaly_validation_passed"),

        F.lit(gold_audit_status)
        .alias("validation_status")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Review audit records
display(df_gold_audit_record)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to the audit table
(
    df_gold_audit_record
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(gold_audit_table_name)
)

print(
    f"Successfully updated audit table: "
    f"{gold_audit_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View the latest audit records
df_gold_audit_saved = spark.table(
    gold_audit_table_name
)

display(
    df_gold_audit_saved
    .orderBy(
        F.col("audited_at").desc()
    )
    .limit(20)
)

saved_audit_run_count = (
    df_gold_audit_saved
    .filter(
        F.col("audit_run_id")
        == gold_audit_run_id
    )
    .count()
)

assert (
    saved_audit_run_count == 1
), (
    "The current Gold audit run was not "
    "written exactly once."
)

print(
    "Gold audit-table persistence "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Analytics Result
# 
# The Gold analytics pipeline completed successfully.
# 
# ### Output tables
# 
# - `gold_agg_daily_performance`
# - `gold_ems_data_quality_audit`
# 
# ### Analytical features
# 
# - Daily EMS incident volume
# - Valid response-time coverage
# - Average dispatch, incident-response and travel time
# - Population standard deviation
# - Coefficient of variation
# - P50, P75, P90 and P95 response times
# - Historical rolling centre line
# - Three-standard-deviation control limits
# - Statistical anomaly detection
# - Minimum sample-size validation
# - Gold fact-to-aggregate reconciliation
# 
# ### Control-limit methodology
# 
# Control limits are calculated separately for each borough and initial
# severity level using the previous 30 eligible daily observations.
# 
# The current observation is excluded from its own historical baseline.
# Daily groups require at least 30 valid response records, and at least
# 20 eligible historical observations are required before a reportable
# anomaly can be produced.
# 
# ### Validation result
# 
# - Gold dimension tables are non-empty
# - Gold fact incident identifiers are complete and unique
# - Daily aggregated incident totals match the Gold fact table
# - Daily analytical grain contains no duplicates
# - Control-limit validation passed
# - Reportable anomaly validation passed
# - Final Gold audit status: PASS


# CELL ********************

print("=" * 70)
print("GOLD ANALYTICS COMPLETION SUMMARY")
print("=" * 70)

print(
    f"Daily analytics table: "
    f"{gold_daily_table_name}"
)

print(
    f"Gold audit table: "
    f"{gold_audit_table_name}"
)

print(
    f"Gold fact rows: "
    f"{fact_row_count:,}"
)

print(
    f"Daily analytics rows: "
    f"{gold_daily_audit['daily_row_count']:,}"
)

print(
    f"Aggregated incidents: "
    f"{gold_daily_audit['aggregated_incident_count']:,}"
)

print(
    f"Low-sample rows: "
    f"{gold_daily_audit['low_sample_row_count']:,}"
)

print(
    f"Statistical anomalies: "
    f"{gold_daily_audit['statistical_anomaly_count']:,}"
)

print(
    f"Reportable anomalies: "
    f"{gold_daily_audit['reportable_anomaly_count']:,}"
)

print(
    f"Validation status: "
    f"{gold_audit_status}"
)

print("=" * 70)

assert gold_audit_status == "PASS"

print(
    "Gold analytics layer completed successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
