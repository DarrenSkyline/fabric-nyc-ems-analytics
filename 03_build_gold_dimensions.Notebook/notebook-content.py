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

# # NYC EMS Gold Dimensions
# 
# ## Purpose
# 
# This notebook builds and validates the conformed dimension tables used by
# the NYC EMS Gold analytical model.
# 
# ## Outputs
# 
# - `gold_dim_date`
# - `gold_dim_time`
# - `gold_dim_geography`
# - `gold_dim_call_type`
# - `gold_dim_severity`
# - `gold_dim_disposition`

# CELL ********************

# Import PySpark components
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
    DateType,
    TimestampType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define all table names

# Input tables
silver_table_name = "silver_ems_incidents"
silver_audit_table_name = "silver_ems_data_quality_audit"

# Dimension tables
gold_date_table_name = "gold_dim_date"
gold_time_table_name = "gold_dim_time"
gold_geography_table_name = "gold_dim_geography"
gold_call_type_table_name = "gold_dim_call_type"
gold_severity_table_name = "gold_dim_severity"
gold_disposition_table_name = "gold_dim_disposition"

# Fact and aggregate tables
gold_fact_table_name = "gold_fact_ems_incident"
gold_daily_table_name = "gold_agg_daily_performance"
gold_audit_table_name = "gold_ems_data_quality_audit"

print(f"Silver input table: {silver_table_name}")
print(f"Gold fact table: {gold_fact_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver table
df_silver = spark.table(silver_table_name)

print(f"Input table: {silver_table_name}")
print(f"Silver column count: {len(df_silver.columns)}")

df_silver.printSchema()

assert len(df_silver.columns) == 63, (
    f"Expected 63 Silver columns, "
    f"found {len(df_silver.columns)}"
)

print("Silver schema validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the latest Silver audit results
df_latest_silver_audit = (
    spark.table(silver_audit_table_name)
    .orderBy(
        F.col("audit_timestamp").desc()
    )
    .limit(1)
)

display(df_latest_silver_audit)

# Get the record of audit
latest_silver_audit = (
    df_latest_silver_audit
    .collect()[0]
    .asDict()
)

silver_audit_status = latest_silver_audit[
    "validation_status"
]

silver_audit_distinct_incident_ids = latest_silver_audit[
    "distinct_incident_ids"
]

silver_audit_missing_ids = latest_silver_audit[
    "missing_incident_ids"
]

silver_audit_duplicate_ids = latest_silver_audit[
    "duplicate_incident_ids"
]

assert silver_audit_status == "PASS", (
    "The latest Silver audit did not pass."
)

assert silver_audit_missing_ids == 0, (
    f"Found {silver_audit_missing_ids} missing incident IDs."
)

assert silver_audit_duplicate_ids == 0, (
    f"Found {silver_audit_duplicate_ids} duplicate incident IDs."
)

print(f"Latest Silver audit status: {silver_audit_status}")

print(
    f"Expected Silver records: "
    f"{silver_audit_distinct_incident_ids:,}"
)

print("Silver audit validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the required fields for Gold exist
required_gold_columns = [
    "incident_id",
    "incident_datetime",
    "incident_date",
    "incident_year",
    "incident_month",
    "incident_hour",
    "time_of_day",
    "borough",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict",
    "initial_call_type",
    "final_call_type",
    "initial_severity_level_code",
    "final_severity_level_code",
    "incident_disposition_code",
    "valid_dispatch_response_seconds",
    "valid_incident_response_seconds",
    "valid_travel_time_seconds",
    "incident_duration_seconds",
    "hospital_travel_seconds",
    "is_held",
    "is_reopened",
    "is_special_event",
    "is_standby",
    "is_transfer",
    "is_weekend",
    "call_type_changed",
    "severity_changed",
    "record_quality_status"
]

missing_gold_columns = [
    column_name
    for column_name in required_gold_columns
    if column_name not in df_silver.columns
]

assert len(missing_gold_columns) == 0, (
    f"Missing Gold source columns: "
    f"{missing_gold_columns}"
)

print(
    f"Required Gold columns validated: "
    f"{len(required_gold_columns)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for null values ​​and cardinality in dimension fields
dimension_candidate_columns = [
    "borough",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict",
    "initial_call_type",
    "final_call_type",
    "initial_severity_level_code",
    "final_severity_level_code",
    "incident_disposition_code"
]

profiling_expressions = []

for column_name in dimension_candidate_columns:
    profiling_expressions.extend([
        F.sum(
            F.when(
                F.col(column_name).isNull(),
                1
            ).otherwise(0)
        ).alias(f"{column_name}_nulls"),

        F.approx_count_distinct(
            F.col(column_name)
        ).alias(f"{column_name}_approx_distinct")
    ])

dimension_profile = (
    df_silver
    .agg(*profiling_expressions)
)

display(dimension_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the Borough value
display(
    df_silver
    .groupBy("borough")
    .agg(
        F.count("*").alias("incident_count")
    )
    .orderBy(
        F.col("incident_count").desc()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the Severity value
display(
    df_silver
    .groupBy(
        "initial_severity_level_code"
    )
    .agg(
        F.count("*").alias("incident_count")
    )
    .orderBy(
        F.col(
            "initial_severity_level_code"
        ).asc()
    )
)

display(
    df_silver
    .groupBy(
        "final_severity_level_code"
    )
    .agg(
        F.count("*").alias("incident_count")
    )
    .orderBy(
        F.col(
            "final_severity_level_code"
        ).asc()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the value of Disposition
display(
    df_silver
    .groupBy(
        "incident_disposition_code"
    )
    .agg(
        F.count("*").alias("incident_count")
    )
    .orderBy(
        F.col("incident_count").desc()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the value of Call Type
display(
    df_silver
    .groupBy("initial_call_type")
    .agg(
        F.count("*").alias("incident_count")
    )
    .orderBy(
        F.col("incident_count").desc()
    )
    .limit(20)
)

display(
    df_silver
    .groupBy("final_call_type")
    .agg(
        F.count("*").alias("incident_count")
    )
    .orderBy(
        F.col("incident_count").desc()
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Date Dimension
# 
# The date dimension provides a continuous calendar covering the complete
# Silver incident-date range.
# 
# ### Grain
# 
# One row represents one calendar date.
# 
# ### Purpose
# 
# - Support year, quarter, month, week and weekday analysis
# - Provide correctly sortable Power BI date labels
# - Support continuous time-series reporting
# - Support forecasting and year-over-year comparisons

# CELL ********************

# Get the date range
silver_date_range = (
    df_silver
    .agg(
        F.min("incident_date").alias("minimum_date"),
        F.max("incident_date").alias("maximum_date")
    )
    .collect()[0]
)

minimum_date = silver_date_range["minimum_date"]
maximum_date = silver_date_range["maximum_date"]

print(f"Minimum incident date: {minimum_date}")
print(f"Maximum incident date: {maximum_date}")

assert minimum_date is not None, (
    "Minimum incident date is null."
)

assert maximum_date is not None, (
    "Maximum incident date is null."
)

assert minimum_date <= maximum_date, (
    "Minimum incident date is after maximum incident date."
)

print("Silver date range validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate consecutive dates
df_date_range = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(minimum_date),
                F.lit(maximum_date),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("full_date")
    )
)

display(df_date_range.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a date dimension field
df_gold_dim_date = (
    df_date_range

    # Primary key
    .withColumn(
        "date_key",
        F.date_format(
            F.col("full_date"),
            "yyyyMMdd"
        ).cast("int")
    )

    # Year attributes
    .withColumn(
        "year",
        F.year("full_date")
    )
    .withColumn(
        "quarter_number",
        F.quarter("full_date")
    )
    .withColumn(
        "quarter_name",
        F.concat(
            F.lit("Q"),
            F.quarter("full_date")
        )
    )
    .withColumn(
        "year_quarter",
        F.concat(
            F.year("full_date"),
            F.lit("-Q"),
            F.quarter("full_date")
        )
    )
    .withColumn(
        "year_quarter_sort",
        (
            F.year("full_date") * 10
            + F.quarter("full_date")
        ).cast("int")
    )

    # Month attributes
    .withColumn(
        "month_number",
        F.month("full_date")
    )
    .withColumn(
        "month_name",
        F.date_format(
            F.col("full_date"),
            "MMMM"
        )
    )
    .withColumn(
        "month_short_name",
        F.date_format(
            F.col("full_date"),
            "MMM"
        )
    )
    .withColumn(
        "year_month",
        F.date_format(
            F.col("full_date"),
            "yyyy-MM"
        )
    )
    .withColumn(
        "year_month_label",
        F.date_format(
            F.col("full_date"),
            "MMM yyyy"
        )
    )
    .withColumn(
        "year_month_sort",
        F.date_format(
            F.col("full_date"),
            "yyyyMM"
        ).cast("int")
    )

    # Week and day attributes
    .withColumn(
        "week_of_year",
        F.weekofyear("full_date")
    )
    .withColumn(
        "day_of_month",
        F.dayofmonth("full_date")
    )
    .withColumn(
        "day_of_year",
        F.dayofyear("full_date")
    )
    .withColumn(
        "day_of_week_number",
        F.dayofweek("full_date")
    )
    .withColumn(
        "day_of_week_name",
        F.date_format(
            F.col("full_date"),
            "EEEE"
        )
    )
    .withColumn(
        "day_of_week_short_name",
        F.date_format(
            F.col("full_date"),
            "EEE"
        )
    )

    # Calendar flags
    .withColumn(
        "is_weekend",
        F.dayofweek("full_date").isin([1, 7])
    )
    .withColumn(
        "is_month_start",
        F.col("full_date")
        == F.trunc(
            F.col("full_date"),
            "month"
        )
    )
    .withColumn(
        "is_month_end",
        F.col("full_date")
        == F.last_day("full_date")
    )

    # Final column order
    .select(
        "date_key",
        "full_date",
        "year",
        "quarter_number",
        "quarter_name",
        "year_quarter",
        "year_quarter_sort",
        "month_number",
        "month_name",
        "month_short_name",
        "year_month",
        "year_month_label",
        "year_month_sort",
        "week_of_year",
        "day_of_month",
        "day_of_year",
        "day_of_week_number",
        "day_of_week_name",
        "day_of_week_short_name",
        "is_weekend",
        "is_month_start",
        "is_month_end"
    )
    .orderBy("full_date")
)

print(
    f"Date dimension column count: "
    f"{len(df_gold_dim_date.columns)}"
)

display(df_gold_dim_date.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validating the Date Dimension
expected_date_rows = (
    maximum_date - minimum_date
).days + 1

date_validation = (
    df_gold_dim_date
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("date_key").alias(
            "distinct_date_keys"
        ),
        F.countDistinct("full_date").alias(
            "distinct_dates"
        ),
        F.sum(
            F.when(
                F.col("date_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_date_keys"),
        F.min("full_date").alias("minimum_date"),
        F.max("full_date").alias("maximum_date")
    )
    .collect()[0]
)

for field_name, field_value in date_validation.asDict().items():
    print(f"{field_name}: {field_value}")

assert date_validation["total_rows"] == expected_date_rows, (
    f"Expected {expected_date_rows} date rows, "
    f"found {date_validation['total_rows']}"
)

assert (
    date_validation["total_rows"]
    == date_validation["distinct_date_keys"]
), "Date key is not unique."

assert (
    date_validation["total_rows"]
    == date_validation["distinct_dates"]
), "Full date is not unique."

assert date_validation["null_date_keys"] == 0, (
    "Date dimension contains null date keys."
)

assert date_validation["minimum_date"] == minimum_date, (
    "Date dimension minimum date is incorrect."
)

assert date_validation["maximum_date"] == maximum_date, (
    "Date dimension maximum date is incorrect."
)

print("Gold date dimension validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta table
(
    df_gold_dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_date_table_name)
)

print(
    f"Successfully created Gold date dimension: "
    f"{gold_date_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reread the persisted table
df_gold_dim_date_saved = spark.table(
    gold_date_table_name
)

print(
    f"Saved date rows: "
    f"{df_gold_dim_date_saved.count():,}"
)

print(
    f"Saved date columns: "
    f"{len(df_gold_dim_date_saved.columns)}"
)

display(
    df_gold_dim_date_saved
    .orderBy("full_date")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the final saved result
saved_date_validation = (
    df_gold_dim_date_saved
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("date_key").alias(
            "distinct_date_keys"
        ),
        F.min("full_date").alias("minimum_date"),
        F.max("full_date").alias("maximum_date")
    )
    .collect()[0]
)

assert (
    saved_date_validation["total_rows"]
    == expected_date_rows
), "Saved date dimension has an incorrect row count."

assert (
    saved_date_validation["total_rows"]
    == saved_date_validation["distinct_date_keys"]
), "Saved date dimension contains duplicate date keys."

assert saved_date_validation["minimum_date"] == minimum_date

assert saved_date_validation["maximum_date"] == maximum_date

print(
    "Saved Gold date dimension validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Time Dimension
# 
# The time dimension provides one row for each hour of the day.
# 
# ### Grain
# 
# One row represents one hour.
# 
# ### Purpose
# 
# - Support hourly EMS demand analysis
# - Support time-of-day comparisons
# - Provide correctly formatted and sortable hour labels for Power BI

# CELL ********************

# Create 24 hours
df_time_hours = (
    spark.range(0, 24)
    .withColumnRenamed("id", "hour_number")
    .withColumn(
        "hour_number",
        F.col("hour_number").cast("int")
    )
)

display(df_time_hours)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a time dimension
df_gold_dim_time = (
    df_time_hours

    # Primary key
    .withColumn(
        "time_key",
        F.col("hour_number")
    )

    # Display label
    .withColumn(
        "hour_label",
        F.concat(
            F.lpad(
                F.col("hour_number").cast("string"),
                2,
                "0"
            ),
            F.lit(":00")
        )
    )

    # Time-of-day category
    .withColumn(
        "time_of_day",
        F.when(
            F.col("hour_number").between(0, 5),
            "Overnight"
        )
        .when(
            F.col("hour_number").between(6, 11),
            "Morning"
        )
        .when(
            F.col("hour_number").between(12, 17),
            "Afternoon"
        )
        .otherwise("Evening")
    )

    # Time-of-day sorting
    .withColumn(
        "time_of_day_sort",
        F.when(
            F.col("hour_number").between(0, 5),
            1
        )
        .when(
            F.col("hour_number").between(6, 11),
            2
        )
        .when(
            F.col("hour_number").between(12, 17),
            3
        )
        .otherwise(4)
    )

    # Final column order
    .select(
        "time_key",
        "hour_number",
        "hour_label",
        "time_of_day",
        "time_of_day_sort"
    )
    .orderBy("hour_number")
)

print(
    f"Time dimension column count: "
    f"{len(df_gold_dim_time.columns)}"
)

display(df_gold_dim_time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Time Dimension
time_validation = (
    df_gold_dim_time
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("time_key").alias(
            "distinct_time_keys"
        ),
        F.countDistinct("hour_number").alias(
            "distinct_hours"
        ),
        F.sum(
            F.when(
                F.col("time_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_time_keys"),
        F.min("hour_number").alias("minimum_hour"),
        F.max("hour_number").alias("maximum_hour")
    )
    .collect()[0]
)

for field_name, field_value in (
    time_validation.asDict().items()
):
    print(f"{field_name}: {field_value}")


assert time_validation["total_rows"] == 24, (
    f"Expected 24 time rows, "
    f"found {time_validation['total_rows']}"
)

assert (
    time_validation["total_rows"]
    == time_validation["distinct_time_keys"]
), "Time key is not unique."

assert (
    time_validation["total_rows"]
    == time_validation["distinct_hours"]
), "Hour number is not unique."

assert time_validation["null_time_keys"] == 0, (
    "Time dimension contains null time keys."
)

assert time_validation["minimum_hour"] == 0, (
    "Minimum hour should be 0."
)

assert time_validation["maximum_hour"] == 23, (
    "Maximum hour should be 23."
)

print("Gold time dimension validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that each time slot is 6 hours long
display(
    df_gold_dim_time
    .groupBy(
        "time_of_day",
        "time_of_day_sort"
    )
    .agg(
        F.count("*").alias("hour_count")
    )
    .orderBy("time_of_day_sort")
)

invalid_time_periods = (
    df_gold_dim_time
    .groupBy("time_of_day")
    .agg(
        F.count("*").alias("hour_count")
    )
    .filter(
        F.col("hour_count") != 6
    )
    .count()
)

assert invalid_time_periods == 0, (
    "One or more time-of-day categories "
    "do not contain exactly six hours."
)

print("Time-of-day category validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta table
(
    df_gold_dim_time
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_time_table_name)
)

print(
    f"Successfully created Gold time dimension: "
    f"{gold_time_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reread and verify the persisted table
df_gold_dim_time_saved = spark.table(
    gold_time_table_name
)

saved_time_validation = (
    df_gold_dim_time_saved
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("time_key").alias(
            "distinct_time_keys"
        ),
        F.min("hour_number").alias("minimum_hour"),
        F.max("hour_number").alias("maximum_hour")
    )
    .collect()[0]
)

assert saved_time_validation["total_rows"] == 24

assert (
    saved_time_validation["distinct_time_keys"]
    == 24
)

assert saved_time_validation["minimum_hour"] == 0

assert saved_time_validation["maximum_hour"] == 23

print(
    "Saved Gold time dimension validation passed."
)

display(
    df_gold_dim_time_saved
    .orderBy("hour_number")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Geography Dimension
# 
# The geography dimension contains the unique geographic combinations
# associated with NYC EMS incidents.
# 
# ### Grain
# 
# One row represents one unique combination of borough, ZIP code, police
# precinct, city council district and community district.
# 
# ### Purpose
# 
# - Support EMS demand analysis by borough and local area
# - Support geographic response-time comparisons
# - Provide a single geography key for the incident fact table
# - Standardise missing geographic values

# CELL ********************

# Define geographic fields
geography_columns = [
    "borough",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict"
]

print(
    f"Geography source columns: "
    f"{len(geography_columns)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardized geographic null values
df_geography_source = (
    df_silver
    .select(
        *[
            F.coalesce(
                F.col(column_name),
                F.lit("UNKNOWN")
            ).alias(column_name)
            for column_name in geography_columns
        ]
    )
)

display(df_geography_source.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract unique geographic combinations
df_geography_distinct = (
    df_geography_source
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate Geography surrogate key
# Use row_number() to generate unique integer keys starting from 1
geography_key_window = Window.orderBy(
    "borough",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict"
)

df_gold_dim_geography = (
    df_geography_distinct
    .withColumn(
        "geography_key",
        F.row_number()
        .over(geography_key_window)
        .cast("long")
    )
    .withColumn(
        "geography_label",
        F.concat_ws(
            " | ",
            F.col("borough"),
            F.col("zipcode")
        )
    )
    .select(
        "geography_key",
        "borough",
        "zipcode",
        "policeprecinct",
        "citycouncildistrict",
        "communitydistrict",
        "geography_label"
    )
    .orderBy("geography_key")
)

print(
    f"Geography dimension column count: "
    f"{len(df_gold_dim_geography.columns)}"
)

display(df_gold_dim_geography.limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the number of rows in the dimension
geography_row_count = (
    df_gold_dim_geography
    .count()
)

print(
    f"Geography dimension rows: "
    f"{geography_row_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Geography Dimension
geography_validation = (
    df_gold_dim_geography
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("geography_key").alias(
            "distinct_geography_keys"
        ),

        F.countDistinct(
            F.struct(
                "borough",
                "zipcode",
                "policeprecinct",
                "citycouncildistrict",
                "communitydistrict"
            )
        ).alias(
            "distinct_geography_combinations"
        ),

        F.sum(
            F.when(
                F.col("geography_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_geography_keys"),

        F.sum(
            F.when(
                F.col("borough").isNull()
                | F.col("zipcode").isNull()
                | F.col("policeprecinct").isNull()
                | F.col("citycouncildistrict").isNull()
                | F.col("communitydistrict").isNull(),
                1
            ).otherwise(0)
        ).alias("remaining_null_attributes")
    )
    .collect()[0]
)

for field_name, field_value in (
    geography_validation.asDict().items()
):
    print(f"{field_name}: {field_value}")


assert (
    geography_validation["total_rows"]
    == geography_validation[
        "distinct_geography_keys"
    ]
), "Geography key is not unique."

assert (
    geography_validation["total_rows"]
    == geography_validation[
        "distinct_geography_combinations"
    ]
), "Duplicate geography combinations were found."

assert (
    geography_validation["null_geography_keys"]
    == 0
), "Geography dimension contains null keys."

assert (
    geography_validation["remaining_null_attributes"]
    == 0
), (
    "Geography dimension contains geographic "
    "attributes that were not standardised."
)

print("Gold geography dimension validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the borough distribution
display(
    df_gold_dim_geography
    .groupBy("borough")
    .agg(
        F.count("*").alias(
            "geography_combination_count"
        )
    )
    .orderBy(
        F.col(
            "geography_combination_count"
        ).desc()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for UNKNOWN values
display(
    df_gold_dim_geography
    .filter(
        (F.col("borough") == "UNKNOWN")
        | (F.col("zipcode") == "UNKNOWN")
        | (F.col("policeprecinct") == "UNKNOWN")
        | (F.col("citycouncildistrict") == "UNKNOWN")
        | (F.col("communitydistrict") == "UNKNOWN")
    )
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta table
(
    df_gold_dim_geography
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_geography_table_name)
)

print(
    f"Successfully created Gold geography dimension: "
    f"{gold_geography_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reread the persisted table
df_gold_dim_geography_saved = spark.table(
    gold_geography_table_name
)

print(
    f"Saved geography rows: "
    f"{df_gold_dim_geography_saved.count():,}"
)

print(
    f"Saved geography columns: "
    f"{len(df_gold_dim_geography_saved.columns)}"
)

display(
    df_gold_dim_geography_saved
    .orderBy("geography_key")
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the final saved result
saved_geography_validation = (
    df_gold_dim_geography_saved
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("geography_key").alias(
            "distinct_geography_keys"
        ),
        F.countDistinct(
            F.struct(
                "borough",
                "zipcode",
                "policeprecinct",
                "citycouncildistrict",
                "communitydistrict"
            )
        ).alias(
            "distinct_geography_combinations"
        )
    )
    .collect()[0]
)

assert (
    saved_geography_validation["total_rows"]
    == geography_validation["total_rows"]
), "Saved geography row count is incorrect."

assert (
    saved_geography_validation["total_rows"]
    == saved_geography_validation[
        "distinct_geography_keys"
    ]
), "Saved geography table contains duplicate keys."

assert (
    saved_geography_validation["total_rows"]
    == saved_geography_validation[
        "distinct_geography_combinations"
    ]
), "Saved geography table contains duplicate combinations."

print(
    "Saved Gold geography dimension validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Call Type Dimension
# 
# The call-type dimension contains the combined set of initial and final EMS
# dispatch call classifications, together with their documented descriptions.
# 
# ### Grain
# 
# One row represents one unique EMS call type.
# 
# ### Source of Descriptions
# 
# Call-type descriptions are sourced from the `Call Type Descriptions` worksheet
# in the official NYC EMS incident dispatch data-description workbook.
# 
# The reference mapping is loaded from:
# 
# `Files/reference/call_type_descriptions.csv`
# 
# ### Purpose
# 
# - Support analysis by initial call type
# - Support analysis by final call type
# - Provide a readable description for each call-type code
# - Support initial-to-final classification-change analysis
# - Identify call types that are not documented in the reference mapping
# - Standardise missing call-type values
# 
# ### Key Attributes
# 
# - `call_type_key`: Surrogate key used by the Gold fact table
# - `call_type`: EMS dispatch call-type code
# - `call_type_description`: Documented description of the call-type code
# - `appears_as_initial`: Indicates whether the call type appears as an initial classification
# - `appears_as_final`: Indicates whether the call type appears as a final classification
# 
# ### Mapping Rules
# 
# - Call-type codes are trimmed and converted to uppercase before matching.
# - Description mappings are joined using the standardized call-type code.
# - All call types appearing in the incident data are retained.
# - A call type that does not match the reference mapping is assigned
#   `UNDOCUMENTED CALL TYPE`.
# - A missing source call type is standardized as `UNKNOWN`.
# 
# ### Mapping Coverage
# 
# Call types observed in the incident data but absent from the official
# description mapping are retained as `UNDOCUMENTED CALL TYPE`.
# 
# In the current dataset, all undocumented call types appear only as initial
# dispatch classifications and do not appear as final call types. No inferred
# descriptions are assigned without documented evidence.
# 
# ### Important Notes
# 
# Call types represent EMS dispatch classifications based on the information
# available to dispatchers. They should not be interpreted as confirmed clinical
# diagnoses.
# 
# A difference between the initial and final call type does not necessarily mean
# that the initial classification was incorrect. It may reflect additional
# information obtained during call handling or incident response.
# 
# An undocumented call type is different from an unknown call type:
# 
# - `UNDOCUMENTED CALL TYPE` means that a call-type code exists in the incident
#   data but does not have a matching description in the reference mapping.
# - `UNKNOWN` means that the source incident did not contain a usable call-type
#   value.


# CELL ********************

# Read the official call-type description mapping
call_type_description_path = (
    "Files/reference/call_type_descriptions.csv"
)

df_call_type_description = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(call_type_description_path)
    .select(
        F.upper(
            F.trim(
                F.col("call_type_code")
            )
        ).alias("call_type"),

        F.trim(
            F.col("call_type_description")
        ).alias("call_type_description")
    )
)

display(
    df_call_type_description
    .orderBy("call_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Mapping
call_type_description_validation = (
    df_call_type_description
    .agg(
        F.count("*")
        .alias("mapping_row_count"),

        F.countDistinct("call_type")
        .alias("distinct_call_type_count"),

        F.sum(
            F.when(
                F.col("call_type").isNull()
                |
                (
                    F.length(
                        F.trim(
                            F.col("call_type")
                        )
                    ) == 0
                ),
                1
            ).otherwise(0)
        ).alias("missing_call_type_count"),

        F.sum(
            F.when(
                F.col("call_type_description").isNull()
                |
                (
                    F.length(
                        F.trim(
                            F.col(
                                "call_type_description"
                            )
                        )
                    ) == 0
                ),
                1
            ).otherwise(0)
        ).alias("missing_description_count")
    )
    .collect()[0]
    .asDict()
)

assert (
    call_type_description_validation[
        "mapping_row_count"
    ]
    ==
    call_type_description_validation[
        "distinct_call_type_count"
    ]
), "Duplicate call-type codes found in description mapping"

assert (
    call_type_description_validation[
        "missing_call_type_count"
    ]
    == 0
), "Missing call-type codes found"

assert (
    call_type_description_validation[
        "missing_description_count"
    ]
    == 0
), "Missing call-type descriptions found"

print("Call-type description mapping validation passed.")

expected_call_type_mapping_rows = 233

assert (
    call_type_description_validation[
        "mapping_row_count"
    ]
    == expected_call_type_mapping_rows
), (
    "Call-type description mapping row count is incorrect. "
    f"Expected {expected_call_type_mapping_rows:,}, "
    f"found "
    f"{call_type_description_validation['mapping_row_count']:,}."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract and standardize Initial Call Type
df_initial_call_types = (
    df_silver
    .select(
        F.when(
            F.col("initial_call_type").isNull()
            |
            (
                F.length(
                    F.trim(
                        F.col("initial_call_type")
                    )
                ) == 0
            ),
            F.lit("UNKNOWN")
        )
        .otherwise(
            F.upper(
                F.trim(
                    F.col("initial_call_type")
                )
            )
        )
        .alias("call_type")
    )
    .withColumn(
        "appears_as_initial",
        F.lit(True)
    )
    .withColumn(
        "appears_as_final",
        F.lit(False)
    )
)

display(df_initial_call_types.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract and standardize Final Call Type
df_final_call_types = (
    df_silver
    .select(
        F.when(
            F.col("final_call_type").isNull()
            |
            (
                F.length(
                    F.trim(
                        F.col("final_call_type")
                    )
                ) == 0
            ),
            F.lit("UNKNOWN")
        )
        .otherwise(
            F.upper(
                F.trim(
                    F.col("final_call_type")
                )
            )
        )
        .alias("call_type")
    )
    .withColumn(
        "appears_as_initial",
        F.lit(False)
    )
    .withColumn(
        "appears_as_final",
        F.lit(True)
    )
)

display(df_final_call_types.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge Initial and Final Call Types
df_call_type_combined = (
    df_initial_call_types
    .unionByName(
        df_final_call_types
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate a unique Call Type record
df_call_type_distinct = (
    df_call_type_combined
    .groupBy("call_type")
    .agg(
        F.max(
            F.col("appears_as_initial").cast("int")
        ).cast("boolean").alias(
            "appears_as_initial"
        ),

        F.max(
            F.col("appears_as_final").cast("int")
        ).cast("boolean").alias(
            "appears_as_final"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate Call Type surrogate key
call_type_key_window = Window.orderBy(
    "call_type"
)

df_gold_dim_call_type = (
    df_call_type_distinct
    .withColumn(
        "call_type_key",
        F.row_number()
        .over(call_type_key_window)
        .cast("long")
    )
    .select(
        "call_type_key",
        "call_type",
        "appears_as_initial",
        "appears_as_final"
    )
    .orderBy("call_type_key")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add the official call-type description

# Remove an existing description column so this cell can be rerun safely
df_call_type_dimension_base = (
    df_gold_dim_call_type
    .drop("call_type_description")
    .alias("ct")
)

df_call_type_description_lookup = (
    df_call_type_description
    .alias("desc")
)

df_gold_dim_call_type = (
    df_call_type_dimension_base
    .join(
        df_call_type_description_lookup,
        on=(
            F.col("ct.call_type")
            ==
            F.col("desc.call_type")
        ),
        how="left"
    )
    .select(
        F.col("ct.call_type_key")
        .alias("call_type_key"),

        F.col("ct.call_type")
        .alias("call_type"),

        F.when(
            F.col("ct.call_type")
            ==
            F.lit("UNKNOWN"),
            F.lit("UNKNOWN CALL TYPE")
        )
        .when(
            F.col(
                "desc.call_type_description"
            ).isNull(),
            F.lit("UNDOCUMENTED CALL TYPE")
        )
        .otherwise(
            F.col(
                "desc.call_type_description"
            )
        )
        .alias("call_type_description"),

        F.col("ct.appears_as_initial")
        .alias("appears_as_initial"),

        F.col("ct.appears_as_final")
        .alias("appears_as_final")
    )
    .orderBy("call_type_key")
)

print(
    f"Call Type dimension column count: "
    f"{len(df_gold_dim_call_type.columns)}"
)

display(df_gold_dim_call_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check call-type description mapping coverage
call_type_mapping_coverage = (
    df_gold_dim_call_type
    .agg(
        F.count("*")
        .alias("dimension_row_count"),

        F.sum(
            F.when(
                F.col("call_type_description")
                == F.lit("UNKNOWN CALL TYPE"),
                1
            ).otherwise(0)
        ).alias("unknown_call_type_count"),

        F.sum(
            F.when(
                F.col("call_type_description")
                == F.lit(
                    "UNDOCUMENTED CALL TYPE"
                ),
                1
            ).otherwise(0)
        ).alias("undocumented_call_type_count"),

        F.sum(
            F.when(
                ~F.col("call_type_description").isin(
                    "UNKNOWN CALL TYPE",
                    "UNDOCUMENTED CALL TYPE"
                ),
                1
            ).otherwise(0)
        ).alias("documented_call_type_count")
    )
    .collect()[0]
    .asDict()
)

for field_name, field_value in (
    call_type_mapping_coverage.items()
):
    print(f"{field_name}: {field_value:,}")

assert (
    call_type_mapping_coverage[
        "unknown_call_type_count"
    ]
    +
    call_type_mapping_coverage[
        "undocumented_call_type_count"
    ]
    +
    call_type_mapping_coverage[
        "documented_call_type_count"
    ]
    ==
    call_type_mapping_coverage[
        "dimension_row_count"
    ]
), "Call-type mapping categories do not reconcile."

print("Call-type mapping coverage validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Review undocumented call types
df_undocumented_call_types = (
    df_gold_dim_call_type
    .filter(
        F.col("call_type_description")
        ==
        F.lit("UNDOCUMENTED CALL TYPE")
    )
    .select(
        "call_type",
        "call_type_description",
        "appears_as_initial",
        "appears_as_final"
    )
    .orderBy("call_type")
)

print(
    f"Undocumented Call Types: "
    f"{df_undocumented_call_types.count():,}"
)

display(df_undocumented_call_types)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the number of rows in the dimension
call_type_row_count = (
    df_gold_dim_call_type
    .count()
)

print(
    f"Call Type dimension rows: "
    f"{call_type_row_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify Call Type Dimension
call_type_validation = (
    df_gold_dim_call_type
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("call_type_key").alias(
            "distinct_call_type_keys"
        ),

        F.countDistinct("call_type").alias(
            "distinct_call_types"
        ),

        F.sum(
            F.when(
                F.col("call_type_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_call_type_keys"),

        F.sum(
            F.when(
                F.col("call_type").isNull(),
                1
            ).otherwise(0)
        ).alias("null_call_types"),

        F.sum(
            F.when(
                F.col("call_type_description").isNull()
                |
                (
                    F.length(
                        F.trim(
                            F.col("call_type_description")
                        )
                    ) == 0
                ),
                1
            ).otherwise(0)
        ).alias("null_or_blank_call_type_descriptions"),

        F.sum(
            F.when(
                ~F.col("appears_as_initial")
                & ~F.col("appears_as_final"),
                1
            ).otherwise(0)
        ).alias("unused_call_types")
    )
    .collect()[0]
)

for field_name, field_value in (
    call_type_validation.asDict().items()
):
    print(f"{field_name}: {field_value}")

assert (
    call_type_validation["total_rows"]
    == call_type_validation[
        "distinct_call_type_keys"
    ]
), "Call Type key is not unique."

assert (
    call_type_validation["total_rows"]
    == call_type_validation[
        "distinct_call_types"
    ]
), "Duplicate Call Type values were found."

assert (
    call_type_validation["null_call_type_keys"]
    == 0
), "Call Type dimension contains null keys."

assert (
    call_type_validation["null_call_types"]
    == 0
), "Call Type dimension contains null values."

assert (
    call_type_validation["unused_call_types"]
    == 0
), (
    "One or more Call Types do not appear "
    "as either Initial or Final Call Type."
)

assert (
    call_type_validation[
        "null_or_blank_call_type_descriptions"
    ]
    == 0
), (
    "Call Type dimension contains null or "
    "blank descriptions."
)

expected_call_type_columns = [
    "call_type_key",
    "call_type",
    "call_type_description",
    "appears_as_initial",
    "appears_as_final"
]

assert (
    df_gold_dim_call_type.columns
    == expected_call_type_columns
), (
    "Call Type dimension schema is incorrect. "
    f"Expected {expected_call_type_columns}, "
    f"found {df_gold_dim_call_type.columns}."
)

print("Gold Call Type dimension validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the initial and final role distributions
display(
    df_gold_dim_call_type
    .groupBy(
        "appears_as_initial",
        "appears_as_final"
    )
    .agg(
        F.count("*").alias(
            "call_type_count"
        )
    )
    .orderBy(
        "appears_as_initial",
        "appears_as_final"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check UNKNOWN records
display(
    df_gold_dim_call_type
    .filter(
        F.col("call_type") == "UNKNOWN"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta table
(
    df_gold_dim_call_type
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_call_type_table_name)
)

print(
    f"Successfully created Gold Call Type dimension: "
    f"{gold_call_type_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reload the saved table
df_gold_dim_call_type_saved = spark.table(
    gold_call_type_table_name
)

print(
    f"Saved Call Type rows: "
    f"{df_gold_dim_call_type_saved.count():,}"
)

print(
    f"Saved Call Type columns: "
    f"{len(df_gold_dim_call_type_saved.columns)}"
)

display(
    df_gold_dim_call_type_saved
    .orderBy("call_type_key")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the persistence results
saved_call_type_validation = (
    df_gold_dim_call_type_saved
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("call_type_key").alias(
            "distinct_call_type_keys"
        ),
        F.countDistinct("call_type").alias(
            "distinct_call_types"
        ),
        F.sum(
            F.when(
                F.col("call_type_description").isNull()
                |
                (
                    F.length(
                        F.trim(
                            F.col("call_type_description")
                        )
                    ) == 0
                ),
                1
            ).otherwise(0)
        ).alias("null_or_blank_descriptions")
    )
    .collect()[0]
)

assert (
    saved_call_type_validation["total_rows"]
    == call_type_validation["total_rows"]
), "Saved Call Type row count is incorrect."

assert (
    saved_call_type_validation["total_rows"]
    == saved_call_type_validation[
        "distinct_call_type_keys"
    ]
), "Saved Call Type table contains duplicate keys."

assert (
    saved_call_type_validation["total_rows"]
    == saved_call_type_validation[
        "distinct_call_types"
    ]
), "Saved Call Type table contains duplicate values."

assert (
    saved_call_type_validation[
        "null_or_blank_descriptions"
    ]
    == 0
), (
    "Saved Call Type dimension contains "
    "null or blank descriptions."
)

assert (
    df_gold_dim_call_type_saved.columns
    == expected_call_type_columns
), (
    "Saved Call Type dimension schema is incorrect. "
    f"Expected {expected_call_type_columns}, "
    f"found {df_gold_dim_call_type_saved.columns}."
)

print(
    "Saved Gold Call Type dimension validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Severity Dimension
# 
# The severity dimension contains the combined set of initial and final EMS
# severity-level codes.
# 
# ### Grain
# 
# One row represents one unique severity-level code.
# 
# ### Purpose
# 
# - Support analysis by initial severity
# - Support analysis by final severity
# - Support severity-classification change analysis
# - Standardise missing severity values
# 
# ### Interpretation Note
# 
# Severity descriptions and priority ordering must be based on documented
# NYC EMS definitions. Numeric codes are not interpreted solely from their
# numeric order.

# CELL ********************

# Extract Initial Severity
df_initial_severity = (
    df_silver
    .select(
        F.coalesce(
            F.col(
                "initial_severity_level_code"
            ),
            F.lit(-1)
        )
        .cast("int")
        .alias("severity_level_code")
    )
    .withColumn(
        "appears_as_initial",
        F.lit(True)
    )
    .withColumn(
        "appears_as_final",
        F.lit(False)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract Final Severity
df_final_severity = (
    df_silver
    .select(
        F.coalesce(
            F.col(
                "final_severity_level_code"
            ),
            F.lit(-1)
        )
        .cast("int")
        .alias("severity_level_code")
    )
    .withColumn(
        "appears_as_initial",
        F.lit(False)
    )
    .withColumn(
        "appears_as_final",
        F.lit(True)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge Initial and Final Severity
df_severity_combined = (
    df_initial_severity
    .unionByName(
        df_final_severity
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate a unique Severity record
df_severity_distinct = (
    df_severity_combined
    .groupBy("severity_level_code")
    .agg(
        F.max(
            F.col(
                "appears_as_initial"
            ).cast("int")
        )
        .cast("boolean")
        .alias("appears_as_initial"),

        F.max(
            F.col(
                "appears_as_final"
            ).cast("int")
        )
        .cast("boolean")
        .alias("appears_as_final")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add a neutral severity label
df_severity_labelled = (
    df_severity_distinct
    .withColumn(
        "severity_label",
        F.when(
            F.col("severity_level_code") == -1,
            F.lit("Unknown")
        )
        .otherwise(
            F.concat(
                F.lit("Severity Level "),
                F.col(
                    "severity_level_code"
                ).cast("string")
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

# Generate Severity surrogate key
severity_key_window = Window.orderBy(
    "severity_level_code"
)

df_gold_dim_severity = (
    df_severity_labelled
    .withColumn(
        "severity_key",
        F.row_number()
        .over(severity_key_window)
        .cast("long")
    )
    .select(
        "severity_key",
        "severity_level_code",
        "severity_label",
        "appears_as_initial",
        "appears_as_final"
    )
    .orderBy("severity_key")
)

print(
    f"Severity dimension column count: "
    f"{len(df_gold_dim_severity.columns)}"
)

display(df_gold_dim_severity)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Severity Dimension
severity_validation = (
    df_gold_dim_severity
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("severity_key").alias(
            "distinct_severity_keys"
        ),

        F.countDistinct(
            "severity_level_code"
        ).alias(
            "distinct_severity_codes"
        ),

        F.sum(
            F.when(
                F.col("severity_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_severity_keys"),

        F.sum(
            F.when(
                F.col(
                    "severity_level_code"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_severity_codes"),

        F.sum(
            F.when(
                ~F.col("appears_as_initial")
                & ~F.col("appears_as_final"),
                1
            ).otherwise(0)
        ).alias("unused_severity_codes")
    )
    .collect()[0]
)

for field_name, field_value in (
    severity_validation.asDict().items()
):
    print(f"{field_name}: {field_value}")


assert (
    severity_validation["total_rows"]
    == severity_validation[
        "distinct_severity_keys"
    ]
), "Severity key is not unique."

assert (
    severity_validation["total_rows"]
    == severity_validation[
        "distinct_severity_codes"
    ]
), "Duplicate severity codes were found."

assert (
    severity_validation["null_severity_keys"]
    == 0
), "Severity dimension contains null keys."

assert (
    severity_validation["null_severity_codes"]
    == 0
), "Severity dimension contains null codes."

assert (
    severity_validation["unused_severity_codes"]
    == 0
), (
    "One or more Severity Codes do not appear "
    "as either Initial or Final Severity."
)

print("Gold severity dimension validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the severity role distributions
display(
    df_gold_dim_severity
    .groupBy(
        "appears_as_initial",
        "appears_as_final"
    )
    .agg(
        F.count("*").alias(
            "severity_code_count"
        )
    )
    .orderBy(
        "appears_as_initial",
        "appears_as_final"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Unknown records
display(
    df_gold_dim_severity
    .filter(
        F.col("severity_level_code") == -1
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta table
(
    df_gold_dim_severity
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_severity_table_name)
)

print(
    f"Successfully created Gold severity dimension: "
    f"{gold_severity_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reload the saved table
df_gold_dim_severity_saved = spark.table(
    gold_severity_table_name
)

print(
    f"Saved severity rows: "
    f"{df_gold_dim_severity_saved.count():,}"
)

print(
    f"Saved severity columns: "
    f"{len(df_gold_dim_severity_saved.columns)}"
)

display(
    df_gold_dim_severity_saved
    .orderBy("severity_key")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the persistence results
saved_severity_validation = (
    df_gold_dim_severity_saved
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("severity_key").alias(
            "distinct_severity_keys"
        ),

        F.countDistinct(
            "severity_level_code"
        ).alias(
            "distinct_severity_codes"
        )
    )
    .collect()[0]
)

assert (
    saved_severity_validation["total_rows"]
    == severity_validation["total_rows"]
), "Saved Severity row count is incorrect."

assert (
    saved_severity_validation["total_rows"]
    == saved_severity_validation[
        "distinct_severity_keys"
    ]
), "Saved Severity table contains duplicate keys."

assert (
    saved_severity_validation["total_rows"]
    == saved_severity_validation[
        "distinct_severity_codes"
    ]
), "Saved Severity table contains duplicate codes."

print(
    "Saved Gold severity dimension validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build Gold Disposition Dimension
# 
# The disposition dimension contains the unique incident-disposition codes
# recorded in the Silver EMS incident table.
# 
# ### Grain
# 
# One row represents one unique incident-disposition code.
# 
# ### Source
# 
# Disposition descriptions are based on the official NYC EMS Incident
# Dispatch Data documentation.
# 
# ### Analytical Classification
# 
# The `disposition_category` field is an analytical grouping derived from
# the documented disposition descriptions. It supports transport,
# non-transport and other incident-outcome analysis.
# 
# ### Output
# 
# - `gold_dim_disposition`

# CELL ********************

# Create official source maps
disposition_mapping_data = [
    (
        "82",
        "transporting patient",
        "Transported"
    ),
    (
        "83",
        "patient pronounced dead",
        "Death on Scene"
    ),
    (
        "87",
        "cancelled",
        "Cancelled / Not Dispatched"
    ),
    (
        "90",
        "unfounded",
        "Patient Not Located / Unfounded"
    ),
    (
        "91",
        "condition corrected",
        "Not Transported"
    ),
    (
        "92",
        "treated not transported",
        "Not Transported"
    ),
    (
        "93",
        "refused medical aid",
        "Not Transported"
    ),
    (
        "94",
        "treated and transported",
        "Transported"
    ),
    (
        "95",
        "triaged at scene no transport",
        "Not Transported"
    ),
    (
        "96",
        "patient gone on arrival",
        "Patient Not Located / Unfounded"
    ),
    (
        "CANCEL",
        "cancelled",
        "Cancelled / Not Dispatched"
    ),
    (
        "DUP",
        "duplicate incident",
        "Duplicate Incident"
    ),
    (
        "NOTSNT",
        "unit not sent",
        "Cancelled / Not Dispatched"
    ),
    (
        "ZZZZZZ",
        "no disposition",
        "Unknown / No Disposition"
    )
]

df_disposition_mapping = (
    spark.createDataFrame(
        disposition_mapping_data,
        [
            "disposition_code",
            "disposition_description",
            "disposition_category"
        ]
    )
    .withColumn(
        "is_documented_code",
        F.lit(True)
    )
)

display(
    df_disposition_mapping
    .orderBy("disposition_code")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that the official mapping itself contains no duplicates
mapping_validation = (
    df_disposition_mapping
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("disposition_code").alias(
            "distinct_codes"
        )
    )
    .collect()[0]
)

assert (
    mapping_validation["total_rows"]
    == mapping_validation["distinct_codes"]
), "Official disposition mapping contains duplicate codes."

print(
    "Official disposition mapping validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract the actual Disposition Code from Silver
df_disposition_source = (
    df_silver
    .select(
        F.coalesce(
            F.col("incident_disposition_code"),
            F.lit("UNKNOWN")
        )
        .cast("string")
        .alias("disposition_code")
    )
)

display(
    df_disposition_source
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract unique Disposition Code
df_disposition_distinct = (
    df_disposition_source
    .distinct()
)

display(
    df_disposition_distinct
    .orderBy("disposition_code")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect to official mappings
df_disposition_enriched = (
    df_disposition_distinct
    .join(
        df_disposition_mapping,
        on="disposition_code",
        how="left"
    )
    .withColumn(
        "disposition_description",
        F.when(
            F.col("disposition_code") == "UNKNOWN",
            F.lit("unknown")
        )
        .otherwise(
            F.coalesce(
                F.col("disposition_description"),
                F.lit("undocumented code")
            )
        )
    )
    .withColumn(
        "disposition_category",
        F.when(
            F.col("disposition_code") == "UNKNOWN",
            F.lit("Unknown / No Disposition")
        )
        .otherwise(
            F.coalesce(
                F.col("disposition_category"),
                F.lit("Undocumented")
            )
        )
    )
    .withColumn(
        "disposition_category_sort",
        F.when(
            F.col("disposition_category") == "Transported",
            1
        )
        .when(
            F.col("disposition_category") == "Not Transported",
            2
        )
        .when(
            F.col("disposition_category") == "Death on Scene",
            3
        )
        .when(
            F.col("disposition_category")
            == "Patient Not Located / Unfounded",
            4
        )
        .when(
            F.col("disposition_category")
            == "Cancelled / Not Dispatched",
            5
        )
        .when(
            F.col("disposition_category")
            == "Duplicate Incident",
            6
        )
        .when(
            F.col("disposition_category")
            == "Unknown / No Disposition",
            7
        )
        .otherwise(8)
    )
    .withColumn(
        "is_documented_code",
        F.coalesce(
            F.col("is_documented_code"),
            F.lit(False)
        )
    )
    .withColumn(
        "is_unknown",
        F.col("disposition_code") == "UNKNOWN"
    )
    .withColumn(
        "mapping_status",
        F.when(
            F.col("disposition_code") == "UNKNOWN",
            F.lit("Unknown")
        )
        .when(
            F.col("is_documented_code"),
            F.lit("Documented")
        )
        .otherwise(
            F.lit("Undocumented")
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View connection results
display(
    df_disposition_enriched
    .orderBy("disposition_code")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check code not covered by official documentation
df_undocumented_dispositions = (
    df_disposition_enriched
    .filter(
        ~F.col("is_documented_code")
        & ~F.col("is_unknown")
    )
)

display(df_undocumented_dispositions)

undocumented_disposition_count = (
    df_undocumented_dispositions
    .count()
)

print(
    f"Undocumented disposition codes: "
    f"{undocumented_disposition_count}"
)

if undocumented_disposition_count > 0:
    print(
        "WARNING: "
        f"{undocumented_disposition_count} "
        "disposition codes are not included "
        "in the current official mapping."
    )

    print(
        "These codes are retained as "
        "'undocumented code' with category "
        "'Undocumented'."
    )
else:
    print(
        "All actual disposition codes are documented."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate Disposition surrogate key
disposition_key_window = Window.orderBy(
    "disposition_code"
)

df_gold_dim_disposition = (
    df_disposition_enriched
    .withColumn(
        "disposition_key",
        F.row_number()
        .over(disposition_key_window)
        .cast("long")
    )
    .select(
        "disposition_key",
        "disposition_code",
        "disposition_description",
        "disposition_category",
        "disposition_category_sort",
        "mapping_status",
        "is_documented_code",
        "is_unknown"
    )
    .orderBy("disposition_key")
)

display(df_gold_dim_disposition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that no original code has been lost
source_disposition_count = (
    df_disposition_distinct
    .count()
)

gold_disposition_count = (
    df_gold_dim_disposition
    .count()
)

assert (
    gold_disposition_count
    == source_disposition_count
), (
    "One or more Silver disposition codes "
    "were lost while building the Gold dimension."
)

print(
    f"Silver distinct disposition codes: "
    f"{source_disposition_count}"
)

print(
    f"Gold disposition dimension rows: "
    f"{gold_disposition_count}"
)

print(
    "All Silver disposition codes were retained."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display category resault
display(
    df_gold_dim_disposition
    .groupBy(
        "disposition_category",
        "disposition_category_sort"
    )
    .agg(
        F.count("*").alias(
            "disposition_code_count"
        )
    )
    .orderBy(
        "disposition_category_sort"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the completed disposition dimension
disposition_validation = (
    df_gold_dim_disposition
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("disposition_key").alias(
            "distinct_disposition_keys"
        ),

        F.countDistinct("disposition_code").alias(
            "distinct_disposition_codes"
        ),

        F.sum(
            F.when(
                F.col("disposition_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_disposition_keys"),

        F.sum(
            F.when(
                F.col("disposition_code").isNull(),
                1
            ).otherwise(0)
        ).alias("null_disposition_codes"),

        F.sum(
            F.when(
                F.col(
                    "disposition_description"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_descriptions"),

        F.sum(
            F.when(
                F.col(
                    "disposition_category"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_categories"),

        F.sum(
            F.when(
                F.col(
                    "disposition_category_sort"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_category_sort_values"),

        F.sum(
            F.when(
                F.col("mapping_status").isNull(),
                1
            ).otherwise(0)
        ).alias("null_mapping_statuses")
    )
    .collect()[0]
)

for field_name, field_value in (
    disposition_validation.asDict().items()
):
    print(f"{field_name}: {field_value}")


assert (
    disposition_validation["total_rows"]
    == disposition_validation[
        "distinct_disposition_keys"
    ]
), "Disposition key is not unique."

assert (
    disposition_validation["total_rows"]
    == disposition_validation[
        "distinct_disposition_codes"
    ]
), "Duplicate disposition codes were found."

assert (
    disposition_validation["null_disposition_keys"]
    == 0
), "Disposition dimension contains null keys."

assert (
    disposition_validation["null_disposition_codes"]
    == 0
), "Disposition dimension contains null codes."

assert (
    disposition_validation["null_descriptions"]
    == 0
), "Disposition dimension contains null descriptions."

assert (
    disposition_validation["null_categories"]
    == 0
), "Disposition dimension contains null categories."

assert (
    disposition_validation["null_category_sort_values"]
    == 0
), "Disposition dimension contains null category sort values."

assert (
    disposition_validation["null_mapping_statuses"]
    == 0
), "Disposition dimension contains null mapping statuses."

print(
    "Gold disposition dimension validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that Category and Sort have a one-to-one correspondence
category_sort_validation = (
    df_gold_dim_disposition
    .groupBy("disposition_category")
    .agg(
        F.countDistinct(
            "disposition_category_sort"
        ).alias("sort_value_count")
    )
)

display(
    category_sort_validation
    .orderBy("disposition_category")
)

# Check for anomalies
invalid_category_sort_count = (
    category_sort_validation
    .filter(
        F.col("sort_value_count") != 1
    )
    .count()
)

assert invalid_category_sort_count == 0, (
    "One or more disposition categories "
    "have multiple sort values."
)

print(
    "Disposition category sorting validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the default value for an undocumented code
invalid_undocumented_records = (
    df_gold_dim_disposition
    .filter(
        F.col("mapping_status") == "Undocumented"
    )
    .filter(
        (F.col("disposition_description")
            != "undocumented code")
        | (F.col("disposition_category")
            != "Undocumented")
        | (F.col("is_documented_code")
            != False)
        | (F.col("is_unknown")
            != False)
    )
    .count()
)

assert invalid_undocumented_records == 0, (
    "One or more undocumented disposition codes "
    "were classified incorrectly."
)

print(
    "Undocumented disposition-code "
    "validation passed."
)

display(
    df_gold_dim_disposition
    .filter(
        F.col("mapping_status") == "Undocumented"
    )
    .orderBy("disposition_code")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the mapping status logic
invalid_mapping_status_count = (
    df_gold_dim_disposition
    .filter(
        (
            (F.col("mapping_status") == "Documented")
            & ~F.col("is_documented_code")
        )
        |
        (
            (F.col("mapping_status") == "Undocumented")
            & F.col("is_documented_code")
        )
        |
        (
            (F.col("mapping_status") == "Unknown")
            & ~F.col("is_unknown")
        )
    )
    .count()
)

assert invalid_mapping_status_count == 0, (
    "Mapping status is inconsistent with "
    "the documented and unknown flags."
)

print(
    "Disposition mapping-status validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta table
# Write the Gold disposition dimension
(
    df_gold_dim_disposition
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_disposition_table_name)
)

print(
    f"Successfully created Gold disposition dimension: "
    f"{gold_disposition_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reload the saved table
df_gold_dim_disposition_saved = spark.table(
    gold_disposition_table_name
)

print(
    f"Saved disposition rows: "
    f"{df_gold_dim_disposition_saved.count():,}"
)

print(
    f"Saved disposition columns: "
    f"{len(df_gold_dim_disposition_saved.columns)}"
)

display(
    df_gold_dim_disposition_saved
    .orderBy("disposition_key")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the persistence results
saved_disposition_validation = (
    df_gold_dim_disposition_saved
    .agg(
        F.count("*").alias("total_rows"),

        F.countDistinct("disposition_key").alias(
            "distinct_disposition_keys"
        ),

        F.countDistinct("disposition_code").alias(
            "distinct_disposition_codes"
        ),

        F.sum(
            F.when(
                F.col("disposition_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_disposition_keys"),

        F.sum(
            F.when(
                F.col("disposition_description").isNull()
                | F.col("disposition_category").isNull()
                | F.col("disposition_category_sort").isNull()
                | F.col("mapping_status").isNull(),
                1
            ).otherwise(0)
        ).alias("incomplete_records")
    )
    .collect()[0]
)

assert (
    saved_disposition_validation["total_rows"]
    == disposition_validation["total_rows"]
), "Saved disposition row count is incorrect."

assert (
    saved_disposition_validation["total_rows"]
    == saved_disposition_validation[
        "distinct_disposition_keys"
    ]
), "Saved disposition table contains duplicate keys."

assert (
    saved_disposition_validation["total_rows"]
    == saved_disposition_validation[
        "distinct_disposition_codes"
    ]
), "Saved disposition table contains duplicate codes."

assert (
    saved_disposition_validation["null_disposition_keys"]
    == 0
), "Saved disposition table contains null keys."

assert (
    saved_disposition_validation["incomplete_records"]
    == 0
), "Saved disposition table contains incomplete records."

print(
    "Saved Gold disposition dimension "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Dimension Result
# 
# The Gold dimension-building process completed successfully.
# 
# ### Output tables
# 
# - `gold_dim_date`
# - `gold_dim_time`
# - `gold_dim_geography`
# - `gold_dim_call_type`
# - `gold_dim_severity`
# - `gold_dim_disposition`
# 
# ### Validation
# 
# - All dimension keys are unique and non-null
# - All source dimension values are retained
# - Missing source values are represented by controlled unknown members
# - Official call-type descriptions were added from the reference mapping
# - Unmapped call-type codes are retained and identified as undocumented
# - Undocumented disposition codes are retained and flagged
# - Dimension tables were persisted successfully as Delta tables
# 
# The incident-level Gold fact table is built separately in
# `04_build_gold_fact`.
