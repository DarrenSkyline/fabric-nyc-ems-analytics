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

# # NYC EMS Silver Data Cleaning and Transformation
# 
# ## Purpose
# 
# This notebook transforms the raw Bronze EMS incident table into a
# validated, analysis-ready Silver Delta table.
# 
# ## Responsibilities
# 
# - Read the Bronze Delta table
# - Validate incident identifier uniqueness
# - Trim text and convert empty strings to null
# - Convert date-time and numeric columns
# - Standardise indicator values
# - Create valid response-time measures
# - Derive date and operational analysis fields
# - Preserve invalid records using data-quality flags
# - Write a partitioned Silver Delta table
# 
# ## Input
# 
# - `bronze_ems_incidents`
# 
# ## Planned output
# 
# - `silver_ems_incidents`
# - `silver_ems_data_quality_audit`

# CELL ********************

# Import PySpark component
from pyspark.sql import functions as F
from pyspark.sql.types import(
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DataType,
    BooleanType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table names
bronze_table_name = "bronze_ems_incidents"
silver_table_name = "silver_ems_incidents"
silver_audit_table_name = "silver_ems_data_quality_audit"

# Read Bronze table
df_bronze = spark.table(bronze_table_name)

print(f"Input table: {bronze_table_name}")
print(f"Bronze columns: {len(df_bronze.columns)}")

df_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the original schema
display(
    df_bronze.select(
        "incident_id",
        "incident_datetime",
        "initial_call_type",
        "initial_severity_level_code",
        "dispatch_response_seconds_qy",
        "incident_response_seconds_qy",
        "borough",
        "held_indicator",
        "_source_year"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Primary Key Validation
# 
# `incident_id` is described by the source as the unique identifier for
# an EMS incident. The Silver transformation verifies that it is both
# complete and unique before deduplication.

# CELL ********************

# Primary key validation
incident_id_statistics = (
    df_bronze
    .agg(
        F.count("*").alias("total_rows"),
        F.count("incident_id").alias("non_null_incident_ids"),
        F.countDistinct("incident_id").alias("distinct_incident_ids")
    )
    .collect()[0]
)

bronze_total_rows = incident_id_statistics["total_rows"]
non_null_incident_ids = incident_id_statistics["non_null_incident_ids"]
distinct_incident_ids = incident_id_statistics["distinct_incident_ids"]

missing_incident_ids = bronze_total_rows - non_null_incident_ids
duplicate_incident_rows = (
    non_null_incident_ids - distinct_incident_ids
)

print(f"Total rows:              {bronze_total_rows:,}")
print(f"Non-null incident IDs:   {non_null_incident_ids:,}")
print(f"Distinct incident IDs:   {distinct_incident_ids:,}")
print(f"Missing incident IDs:    {missing_incident_ids:,}")
print(f"Duplicate incident rows: {duplicate_incident_rows:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add a uniqueness assertion
assert bronze_total_rows == 10881496, (
    f"Expected 10,881,496 rows, found {bronze_total_rows}"
)

assert missing_incident_ids == 0, (
    f"Found {missing_incident_ids} missing incident IDs"
)

assert duplicate_incident_rows == 0, (
    f"Found {duplicate_incident_rows} duplicate incident rows"
)

print("Incident ID validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the value of the Indicator field
indicator_columns = [
    "valid_dispatch_rspns_time_indc",
    "valid_incident_rspns_time_indc",
    "held_indicator",
    "reopen_indicator",
    "special_event_indicator",
    "standby_indicator",
    "transfer_indicator"
]

for column_name in indicator_columns:
    print(f"Values in {column_name}:")

    (
        df_bronze
        .groupBy(column_name)
        .count()
        .orderBy(F.col("count").desc())
        .show(truncate=False)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for date and numerical conversion risks
datetime_columns = [
    "incident_datetime",
    "first_assignment_datetime",
    "first_activation_datetime",
    "first_on_scene_datetime",
    "first_to_hosp_datetime",
    "first_hosp_arrival_datetime",
    "incident_close_datetime"
]

numeric_columns = [
    "initial_severity_level_code",
    "final_severity_level_code",
    "dispatch_response_seconds_qy",
    "incident_response_seconds_qy",
    "incident_travel_tm_seconds_qy"
]

datetime_validation_expressions = [
    F.sum(
        F.when(
            F.col(column_name).isNotNull() &
            F.to_timestamp(F.col(column_name)).isNull(),
            1
        ).otherwise(0)
    ).alias(f"{column_name}_invalid")
    for column_name in datetime_columns
]

numeric_validation_expressions = [
    F.sum(
        F.when(
            F.col(column_name).isNotNull() &
            F.col(column_name).cast("double").isNull(),
            1
        ).otherwise(0)
    ).alias(f"{column_name}_invalid")
    for column_name in numeric_columns
]

conversion_validation = (
    df_bronze
    .agg(
        *datetime_validation_expressions,
        *numeric_validation_expressions
    )
)

display(conversion_validation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Clean Text Values
# 
# Text values are trimmed and empty strings are converted to null.
# Indicator fields are standardised to uppercase while preserving their
# original Y/N representation.

# CELL ********************

df_silver = df_bronze

# Trim every string column and convert empty strings to null
for field in df_silver.schema.fields:
    if isinstance(field.dataType, StringType):
        column_name = field.name

        df_silver = df_silver.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(column_name))
            )
        )

indicator_columns = [
    "valid_dispatch_rspns_time_indc",
    "valid_incident_rspns_time_indc",
    "held_indicator",
    "reopen_indicator",
    "special_event_indicator",
    "standby_indicator",
    "transfer_indicator"
]

# Standardise indicator values
for column_name in indicator_columns:
    df_silver = df_silver.withColumn(
        column_name,
        F.upper(F.col(column_name))
    )

print("Text cleaning rules applied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert Date-Time Columns
# 
# All EMS timestamps are treated as NYC local operational timestamps.
# No conversion to New Zealand time or UTC is applied because the source
# does not provide a timezone offset.

# CELL ********************

datetime_columns = [
    "incident_datetime",
    "first_assignment_datetime",
    "first_activation_datetime",
    "first_on_scene_datetime",
    "first_to_hosp_datetime",
    "first_hosp_arrival_datetime",
    "incident_close_datetime"
]

for column_name in datetime_columns:
    df_silver = df_silver.withColumn(
        column_name,
        F.to_timestamp(F.col(column_name))
    )

print("Date-time conversions applied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert numeric columns
integer_columns = [
    "initial_severity_level_code",
    "final_severity_level_code"
]

response_time_columns = [
    "dispatch_response_seconds_qy",
    "incident_response_seconds_qy",
    "incident_travel_tm_seconds_qy"
]

for column_name in integer_columns:
    df_silver = df_silver.withColumn(
        column_name,
        F.col(column_name).cast("int")
    )

for column_name in response_time_columns:
    df_silver = df_silver.withColumn(
        column_name,
        F.col(column_name).cast("double")
    )

print("Numeric conversions applied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create boolean columns
def yn_to_boolean(column_name):
    return (
        F.when(F.col(column_name) == "Y", F.lit(True))
        .when(F.col(column_name) == "N", F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )

df_silver = (
    df_silver
    .withColumn(
        "is_valid_dispatch_response",
        yn_to_boolean("valid_dispatch_rspns_time_indc")
    )
    .withColumn(
        "is_valid_incident_response",
        yn_to_boolean("valid_incident_rspns_time_indc")
    )
    .withColumn(
        "is_held",
        yn_to_boolean("held_indicator")
    )
    .withColumn(
        "is_reopened",
        yn_to_boolean("reopen_indicator")
    )
    .withColumn(
        "is_special_event",
        yn_to_boolean("special_event_indicator")
    )
    .withColumn(
        "is_standby",
        yn_to_boolean("standby_indicator")
    )
    .withColumn(
        "is_transfer",
        yn_to_boolean("transfer_indicator")
    )
)

print("Boolean indicator columns created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create an Effective Response Time field
df_silver = (
    df_silver
    .withColumn(
        "valid_dispatch_response_seconds",
        F.when(
            (F.col("is_valid_dispatch_response") == True) &
            (F.col("dispatch_response_seconds_qy") >= 0),
            F.col("dispatch_response_seconds_qy")
        )
    )
    .withColumn(
        "valid_incident_response_seconds",
        F.when(
            (F.col("is_valid_incident_response") == True) &
            (F.col("incident_response_seconds_qy") >= 0),
            F.col("incident_response_seconds_qy")
        )
    )
    .withColumn(
        "valid_travel_time_seconds",
        F.when(
            F.col("incident_travel_tm_seconds_qy") >= 0,
            F.col("incident_travel_tm_seconds_qy")
        )
    )
)

print("Valid response-time measures created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Date Analysis Fields
df_silver = (
    df_silver
    .withColumn(
        "incident_date",
        F.to_date("incident_datetime")
    )
    .withColumn(
        "incident_year",
        F.year("incident_datetime")
    )
    .withColumn(
        "incident_quarter",
        F.quarter("incident_datetime")
    )
    .withColumn(
        "incident_month",
        F.month("incident_datetime")
    )
    .withColumn(
        "incident_month_name",
        F.date_format("incident_datetime", "MMMM")
    )
    .withColumn(
        "incident_year_month",
        F.date_format("incident_datetime", "yyyy-MM")
    )
    .withColumn(
        "incident_day_of_week_number",
        F.dayofmonth("incident_datetime")
    )
    .withColumn(
        "incident_day_of_week_name",
        F.date_format("incident_datetime", "EEEE")
    )
    .withColumn(
        "incident_hour",
        F.hour("incident_datetime")
    )
    .withColumn(
        "is_weekend",
        F.dayofweek("incident_datetime").isin([1, 7])
    )
)

print("Date analysis columns created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Time of Day
df_silver = df_silver.withColumn(
    "time_of_day",
    F.when(
        F.col("incident_hour").between(0, 5),
        "Overnight"
    )
    .when(
        F.col("incident_hour").between(6, 11),
        "Morning"
    )
    .when(
        F.col("incident_hour").between(12, 17),
        "Afternoon"
    )
    .otherwise("Evening")
)

print("Time-of-day category created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a field with minute-level granularity
df_silver = (
    df_silver
    .withColumn(
        "dispatch_response_minutes",
        F.round(
            F.col("valid_dispatch_response_seconds") / 60.0,
            2
        )
    )
    .withColumn(
        "incident_response_minutes",
        F.round(
            F.col("valid_incident_response_seconds") / 60.0,
            2
        )
    )
    .withColumn(
        "travel_time_minutes",
        F.round(
            F.col("valid_travel_time_seconds") / 60.0,
            2
        )
    )
)

print("Response-time minute columns created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create fields for Call Type and Severity changes
df_silver = (
    df_silver
    .withColumn(
        "call_type_changed",
        F.when(
            F.col("initial_call_type").isNull() |
            F.col("final_call_type").isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.col("initial_call_type") !=
            F.col("final_call_type")
        )
    )
    .withColumn(
        "severity_changed",
        F.when(
            F.col("initial_severity_level_code").isNull() |
            F.col("final_severity_level_code").isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.col("initial_severity_level_code") !=
            F.col("final_severity_level_code")
        )
    )
)

print("Call-type and severity-change indicators created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Track incident duration and time to hospital arrival
incident_duration_expression = (
    F.col("incident_close_datetime").cast("long") -
    F.col("incident_datetime").cast("long")
)

hospital_travel_expression = (
    F.col("first_hosp_arrival_datetime").cast("long") -
    F.col("first_to_hosp_datetime").cast("long")
)

df_silver = (
    df_silver
    .withColumn(
        "incident_duration_seconds",
        F.when(
            incident_duration_expression >= 0,
            incident_duration_expression .cast("double")
        )
    )
    .withColumn(
        "hospital_travel_seconds",
        F.when(
            hospital_travel_expression >= 0,
            hospital_travel_expression.cast("double")
        )
    )
)

print("Incident-duration fields created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create record-quality status
df_silver = df_silver.withColumn(
    "record_quality_status",
    F.when(
        F.col("incident_id").isNull(),
        "MISSING_INCIDENT_ID"
    )
    .when(
        F.col("incident_datetime").isNull(),
        "INVALID_INCIDENT_DATETIME"
    )
    .when(
        F.col("incident_year") != F.col("_source_year"),
        "SOURCE_YEAR_MISMATCH"
    )
    .otherwise("VALID")
)

print("Record-quality status created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the resault of Sliver table
print(f"Silver column count: {len(df_silver.columns)}")

df_silver.printSchema()

display(
    df_silver.select(
        "incident_id",
        "incident_datetime",
        "incident_date",
        "incident_year_month",
        "incident_hour",
        "time_of_day",
        "borough",
        "incident_response_seconds_qy",
        "valid_incident_response_seconds",
        "incident_response_minutes",
        "is_held",
        "call_type_changed",
        "record_quality_status"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Sliver transformation
silver_validation = (
    df_silver
    .agg(
        F.count("*").alias("total_rows"),

        F.sum(
            F.when(
                F.col("record_quality_status") != "VALID",
                1
            ).otherwise(0)
        ).alias("invalid_records"),

        F.sum(
            F.when(
                F.col("incident_year") != F.col("_source_year"),
                1
            ).otherwise(0)
        ).alias("source_year_mismatches"),

        F.sum(
            F.when(
                F.col("dispatch_response_seconds_qy") < 0,
                1
            ).otherwise(0)
        ).alias("negative_dispatch_times"),

        F.sum(
            F.when(
                F.col("incident_travel_tm_seconds_qy") < 0,
                1
            ).otherwise(0)
        ).alias("negative_travel_times"),
    )
    .collect()[0]
)

for field_name in silver_validation.asDict():
    print(
        f"{field_name}: "
        f"{silver_validation[field_name]:,}"
    )

assert silver_validation["total_rows"] == bronze_total_rows

assert silver_validation["source_year_mismatches"] == 0, (
    "Incident year does not match source partition year."
)

assert silver_validation["invalid_records"] == 0, (
    "One or more records failed Silver quality validation."
)

print("Silver transformation validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write df_silver to the Silver Delta table
# Enable write optimizations
spark.conf.set(
    "spark.microsoft.delta.optimizeWrite.enabled",
    "true"
)

spark.conf.set(
    "spark.microsoft.delta.autoCompact.enabled",
    "true"
)

# Write Silver table
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("incident_year")
    .saveAsTable(silver_table_name)
)

print(f"Successfully created Silver table: {silver_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the persisted Silver table
df_silver_saved = spark.table(silver_table_name)

print(f"Successfully loaded: {silver_table_name}")
print(f"Column count: {len(df_silver_saved.columns)}")

df_silver_saved.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Perform a quality audit on the final Silver table
silver_audit_result = (
    df_silver_saved
    .agg(
        F.count("*").alias("total_rows"),

         F.countDistinct("incident_id").alias(
            "distinct_incident_ids"
        ),

        F.sum(
            F.when(
                F.col("incident_id").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_incident_ids"),

        F.sum(
            F.when(
                F.col("record_quality_status") != "VALID",
                1
            ).otherwise(0)
        ).alias("invalid_records"),

        F.sum(
            F.when(
                F.col("incident_year") != F.col("_source_year"),
                1
            ).otherwise(0)
        ).alias("source_year_mismatches"),

        F.sum(
            F.when(
                F.col("dispatch_response_seconds_qy") < 0,
                1
            ).otherwise(0)
        ).alias("negative_dispatch_times"),

        F.sum(
            F.when(
                F.col("incident_response_seconds_qy") < 0,
                1
            ).otherwise(0)
        ).alias("negative_incident_times"),

        F.sum(
            F.when(
                F.col("incident_travel_tm_seconds_qy") < 0,
                1
            ).otherwise(0)
        ).alias("negative_travel_times"),

        F.min("incident_datetime").alias(
            "minimum_incident_datetime"
        ),

        F.max("incident_datetime").alias(
            "maximum_incident_datetime"
        )
    )
    .first()
)

silver_audit = silver_audit_result.asDict()

silver_audit["duplicate_incident_ids"] = (
    silver_audit["total_rows"]
    - silver_audit["distinct_incident_ids"]
)

silver_audit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate audit records
from datetime import datetime
from pyspark.sql import Row

audit_status = (
    "PASS"
    if (
        silver_audit["total_rows"] == bronze_total_rows
        and silver_audit["missing_incident_ids"] == 0
        and silver_audit["duplicate_incident_ids"] == 0
        and silver_audit["invalid_records"] == 0
        and silver_audit["source_year_mismatches"] == 0
        and silver_audit["negative_dispatch_times"] == 0
        and silver_audit["negative_incident_times"] == 0
        and silver_audit["negative_travel_times"] == 0
    )
    else "FAIL"
)

audit_record = Row(
    audit_timestamp = datetime.utcnow(),
    source_table = "bronze_ems_incidents",
    target_table = silver_table_name,
    total_columns = len(df_silver_saved.columns),
    distinct_incident_ids = (
        silver_audit["distinct_incident_ids"]
    ),
    missing_incident_ids = (
        silver_audit["missing_incident_ids"]
    ),
    duplicate_incident_ids = (
        silver_audit["duplicate_incident_ids"]
    ),
    invalid_records = silver_audit["invalid_records"],
    source_year_mismatches = (
        silver_audit["source_year_mismatches"]
    ),
    negative_dispatch_times = (
        silver_audit["negative_dispatch_times"]
    ),
    negative_incident_times = (
        silver_audit["negative_incident_times"]
    ),
    negative_travel_times = (
        silver_audit["negative_travel_times"]
    ),
    minimum_incident_datetime = (
        silver_audit["minimum_incident_datetime"]
    ),
    maximum_incident_datetime = (
        silver_audit["maximum_incident_datetime"]
    ),
    validation_status = audit_status
)

df_silver_audit = spark.createDataFrame([audit_record])

display(df_silver_audit)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write into audit table
(
    df_silver_audit
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(silver_audit_table_name)
)

print(
    f"Successfully updated audit table: "
    f"{silver_audit_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final Confirmation
print("=" * 60)
print("SILVER LAYER COMPLETION SUMMARY")
print("=" * 60)

print(f"Silver table: {silver_table_name}")
print(f"Audit table: {silver_audit_table_name}")
print(f"Total rows: {silver_audit['total_rows']:,}")
print(f"Total columns: {len(df_silver_saved.columns)}")
print(
    "Distinct incident IDs: "
    f"{silver_audit['distinct_incident_ids']:,}"
)
print(
    "Missing incident IDs: "
    f"{silver_audit['missing_incident_ids']:,}"
)
print(
    "Duplicate incident IDs: "
    f"{silver_audit['duplicate_incident_ids']:,}"
)
print(f"Validation status: {audit_status}")

print("=" * 60)

if audit_status != "PASS":
    raise ValueError(
        "Silver validation failed. "
        "Do not continue to the Gold layer."
    )

print("Silver layer completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
