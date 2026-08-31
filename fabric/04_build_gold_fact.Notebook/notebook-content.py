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

# # NYC EMS Gold Incident Fact Table
# 
# ## Purpose
# 
# This notebook transforms the validated Silver EMS incident table into the
# incident-level Gold fact table and connects each incident to the conformed
# Gold dimensions.
# 
# ## Grain
# 
# One row represents one EMS incident.
# 
# ## Inputs
# 
# - `silver_ems_incidents`
# - `silver_ems_data_quality_audit`
# - `gold_dim_date`
# - `gold_dim_time`
# - `gold_dim_geography`
# - `gold_dim_call_type`
# - `gold_dim_severity`
# - `gold_dim_disposition`
# 
# ## Output
# 
# - `gold_fact_ems_incident`
# 
# ## Responsibilities
# 
# - Read and validate the Silver input
# - Read the persisted Gold dimensions
# - Standardise dimension lookup values
# - Resolve dimension surrogate keys
# - Preserve incident-level analytical measures
# - Preserve operational indicator fields
# - Validate fact-table grain and referential integrity
# - Write and validate the Gold Delta fact table

# CELL ********************

# Import PySpark component
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define table names
# Silver tables
silver_table_name = "silver_ems_incidents"
silver_audit_table_name = (
    "silver_ems_data_quality_audit"
)

# Gold dimension tables
gold_date_table_name = "gold_dim_date"
gold_time_table_name = "gold_dim_time"
gold_geography_table_name = (
    "gold_dim_geography"
)
gold_call_type_table_name = (
    "gold_dim_call_type"
)
gold_severity_table_name = (
    "gold_dim_severity"
)
gold_disposition_table_name = (
    "gold_dim_disposition"
)

# Gold fact table
gold_fact_table_name = (
    "gold_fact_ems_incident"
)

print(
    f"Gold fact output: "
    f"{gold_fact_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver table
df_silver = spark.table(
    silver_table_name
)

print(
    f"Silver columns: "
    f"{len(df_silver.columns)}"
)

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

# Read latest Silver audit record
df_latest_silver_audit = (
    spark.table(
        silver_audit_table_name
    )
    .orderBy(
        F.col("audit_timestamp").desc()
    )
    .limit(1)
)

display(df_latest_silver_audit)

# Convert to Dict
latest_silver_audit = (
    df_latest_silver_audit
    .collect()[0]
    .asDict()
)

silver_audit_status = latest_silver_audit[
    "validation_status"
]

expected_silver_rows = latest_silver_audit[
    "distinct_incident_ids"
]

silver_missing_ids = latest_silver_audit[
    "missing_incident_ids"
]

silver_duplicate_ids = latest_silver_audit[
    "duplicate_incident_ids"
]

# validation
assert silver_audit_status == "PASS", (
    "The latest Silver audit did not pass."
)

assert silver_missing_ids == 0, (
    f"Silver contains {silver_missing_ids} "
    "missing incident IDs."
)

assert silver_duplicate_ids == 0, (
    f"Silver contains {silver_duplicate_ids} "
    "duplicate incident IDs."
)

print(
    f"Expected Fact rows: "
    f"{expected_silver_rows:,}"
)

print(
    "Silver audit validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read 6 Dimension tables
df_dim_date = spark.table(
    gold_date_table_name
)

df_dim_time = spark.table(
    gold_time_table_name
)

df_dim_geography = spark.table(
    gold_geography_table_name
)

df_dim_call_type = spark.table(
    gold_call_type_table_name
)

df_dim_severity = spark.table(
    gold_severity_table_name
)

df_dim_disposition = spark.table(
    gold_disposition_table_name
)

# Display dimension rows
dimension_tables = {
    gold_date_table_name: df_dim_date,
    gold_time_table_name: df_dim_time,
    gold_geography_table_name: (
        df_dim_geography
    ),
    gold_call_type_table_name: (
        df_dim_call_type
    ),
    gold_severity_table_name: (
        df_dim_severity
    ),
    gold_disposition_table_name: (
        df_dim_disposition
    )
}

for table_name, dataframe in (
    dimension_tables.items()
):
    print(
        f"{table_name}: "
        f"{dataframe.count():,} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the uniqueness of the dimension primary key
dimension_key_checks = [
    (
        gold_date_table_name,
        df_dim_date,
        "date_key"
    ),
    (
        gold_time_table_name,
        df_dim_time,
        "time_key"
    ),
    (
        gold_geography_table_name,
        df_dim_geography,
        "geography_key"
    ),
    (
        gold_call_type_table_name,
        df_dim_call_type,
        "call_type_key"
    ),
    (
        gold_severity_table_name,
        df_dim_severity,
        "severity_key"
    ),
    (
        gold_disposition_table_name,
        df_dim_disposition,
        "disposition_key"
    )
]

for (
    table_name,
    dataframe,
    key_column
) in dimension_key_checks:

    key_validation = (
        dataframe
        .agg(
            F.count("*").alias("total_rows"),

            F.countDistinct(
                key_column
            ).alias("distinct_keys"),

            F.sum(
                F.when(
                    F.col(key_column).isNull(),
                    1
                ).otherwise(0)
            ).alias("null_keys")
        )
        .collect()[0]
    )

    assert (
        key_validation["total_rows"]
        == key_validation["distinct_keys"]
    ), (
        f"{table_name} contains "
        f"duplicate keys."
    )

    assert key_validation["null_keys"] == 0, (
        f"{table_name} contains null keys."
    )

    print(
        f"{table_name} key validation passed."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare Fact source fields
df_fact_source = (
    df_silver
    .select(
        # Incident identifier and time
        "incident_id",
        "incident_datetime",
        "incident_date",
        "incident_year",
        "incident_hour",

        # Geography
        "borough",
        "zipcode",
        "policeprecinct",
        "citycouncildistrict",
        "communitydistrict",

        # Call type and severity
        "initial_call_type",
        "final_call_type",
        "initial_severity_level_code",
        "final_severity_level_code",

        # Disposition
        "incident_disposition_code",

        # Valid response-time measures
        "valid_dispatch_response_seconds",
        "valid_incident_response_seconds",
        "valid_travel_time_seconds",

        # Display measures
        "dispatch_response_minutes",
        "incident_response_minutes",
        "travel_time_minutes",

        # Duration measures
        "incident_duration_seconds",
        "hospital_travel_seconds",

        # Operational indicators
        "is_valid_dispatch_response",
        "is_valid_incident_response",
        "is_held",
        "is_reopened",
        "is_special_event",
        "is_standby",
        "is_transfer",
        "call_type_changed",
        "severity_changed"
    )
)

print(
    f"Fact source columns: "
    f"{len(df_fact_source.columns)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Date Key and Time Key
df_fact_prepared = (
    df_fact_source
    .withColumn(
        "date_key",
        F.date_format(
            F.col("incident_date"),
            "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "time_key",
        F.col("incident_hour").cast("int")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardized Geographic Lookup Value
for geography_column in [
    "borough",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict"
]:
    df_fact_prepared = (
        df_fact_prepared
        .withColumn(
            geography_column,
            F.coalesce(
                F.col(geography_column),
                F.lit("UNKNOWN")
            )
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardize Call Type, Severity, and Disposition Lookup Value
df_fact_prepared = (
    df_fact_prepared
    .withColumn(
        "initial_call_type_lookup",
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
    )
    .withColumn(
        "final_call_type_lookup",
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
    )
    .withColumn(
        "initial_severity_lookup",
        F.coalesce(
            F.col(
                "initial_severity_level_code"
            ),
            F.lit(-1)
        ).cast("int")
    )
    .withColumn(
        "final_severity_lookup",
        F.coalesce(
            F.col(
                "final_severity_level_code"
            ),
            F.lit(-1)
        ).cast("int")
    )
    .withColumn(
        "disposition_lookup",
        F.coalesce(
            F.col(
                "incident_disposition_code"
            ),
            F.lit("UNKNOWN")
        ).cast("string")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preparing the Lookup DataFrame
df_initial_call_type_lookup = (
    df_dim_call_type
    .select(
        F.col("call_type").alias(
            "initial_call_type_lookup"
        ),
        F.col("call_type_key").alias(
            "initial_call_type_key"
        )
    )
)

df_final_call_type_lookup = (
    df_dim_call_type
    .select(
        F.col("call_type").alias(
            "final_call_type_lookup"
        ),
        F.col("call_type_key").alias(
            "final_call_type_key"
        )
    )
)

df_initial_severity_lookup = (
    df_dim_severity
    .select(
        F.col("severity_level_code").alias(
            "initial_severity_lookup"
        ),
        F.col("severity_key").alias(
            "initial_severity_key"
        )
    )
)

df_final_severity_lookup = (
    df_dim_severity
    .select(
        F.col("severity_level_code").alias(
            "final_severity_lookup"
        ),
        F.col("severity_key").alias(
            "final_severity_key"
        )
    )
)

df_disposition_lookup = (
    df_dim_disposition
    .select(
        F.col("disposition_code").alias(
            "disposition_lookup"
        ),
        "disposition_key"
    )
)

# Geography Lookup
df_geography_lookup = (
    df_dim_geography
    .select(
        "borough",
        "zipcode",
        "policeprecinct",
        "citycouncildistrict",
        "communitydistrict",
        "geography_key"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect to Geography Key
df_fact_with_geography = (
    df_fact_prepared
    .join(
        F.broadcast(
            df_geography_lookup
        ),
        on=[
            "borough",
            "zipcode",
            "policeprecinct",
            "citycouncildistrict",
            "communitydistrict"
        ],
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect to Call Type Keys
df_fact_with_call_types = (
    df_fact_with_geography
    .join(
        F.broadcast(
            df_initial_call_type_lookup
        ),
        on="initial_call_type_lookup",
        how="left"
    )
    .join(
        F.broadcast(
            df_final_call_type_lookup
        ),
        on="final_call_type_lookup",
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect to Severity Keys
df_fact_with_severity = (
    df_fact_with_call_types
    .join(
        F.broadcast(
            df_initial_severity_lookup
        ),
        on="initial_severity_lookup",
        how="left"
    )
    .join(
        F.broadcast(
            df_final_severity_lookup
        ),
        on="final_severity_lookup",
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect to Disposition Key
df_fact_joined = (
    df_fact_with_severity
    .join(
        F.broadcast(
            df_disposition_lookup
        ),
        on="disposition_lookup",
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Construct the final fact table
df_gold_fact_ems_incident = (
    df_fact_joined
    .withColumn(
        "incident_count",
        F.lit(1).cast("int")
    )
    .select(
        # Degenerate dimension
        "incident_id",
        "incident_datetime",

        # Partition support
        "incident_year",

        # Dimension foreign keys
        "date_key",
        "time_key",
        "geography_key",
        "initial_call_type_key",
        "final_call_type_key",
        "initial_severity_key",
        "final_severity_key",
        "disposition_key",

        # Additive count
        "incident_count",

        # Response-time measures
        "valid_dispatch_response_seconds",
        "valid_incident_response_seconds",
        "valid_travel_time_seconds",

        # Display measures
        "dispatch_response_minutes",
        "incident_response_minutes",
        "travel_time_minutes",

        # Duration measures
        "incident_duration_seconds",
        "hospital_travel_seconds",

        # Validity indicators
        "is_valid_dispatch_response",
        "is_valid_incident_response",

        # Operational indicators
        "is_held",
        "is_reopened",
        "is_special_event",
        "is_standby",
        "is_transfer",
        "call_type_changed",
        "severity_changed"
    )
)

print(
    f"Gold Fact columns: "
    f"{len(df_gold_fact_ems_incident.columns)}"
)

df_gold_fact_ems_incident.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the sample
display(
    df_gold_fact_ems_incident
    .select(
        "incident_id",
        "incident_datetime",
        "date_key",
        "time_key",
        "geography_key",
        "initial_call_type_key",
        "final_call_type_key",
        "initial_severity_key",
        "final_severity_key",
        "disposition_key",
        "valid_incident_response_seconds",
        "is_held"
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Gold Fact Delta Table
# Enable write optimization
spark.conf.set(
    "spark.microsoft.delta.optimizeWrite.enabled",
    "true"
)

spark.conf.set(
    "spark.microsoft.delta.autoCompact.enabled",
    "true"
)

# Partitioned by incident_year
(
    df_gold_fact_ems_incident
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("incident_year")
    .saveAsTable(gold_fact_table_name)
)

print(
    f"Successfully created Gold Fact table: "
    f"{gold_fact_table_name}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reload the persisted fact table
df_gold_fact_saved = spark.table(
    gold_fact_table_name
)

print(
    f"Saved Gold Fact columns: "
    f"{len(df_gold_fact_saved.columns)}"
)

df_gold_fact_saved.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the number of columns in the fact table
expected_fact_column_count = 29
actual_fact_column_count = len(
    df_gold_fact_saved.columns
)

assert (
    actual_fact_column_count
    == expected_fact_column_count
), (
    f"Expected {expected_fact_column_count} "
    f"Gold Fact columns, "
    f"found {actual_fact_column_count}"
)

print(
    f"Gold Fact column count validated: "
    f"{actual_fact_column_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify row count, Incident ID, and foreign keys
fact_validation = (
    df_gold_fact_saved
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
        ).alias("null_incident_ids"),

        F.sum(
            F.when(
                F.col("date_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_date_keys"),

        F.sum(
            F.when(
                F.col("time_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_time_keys"),

        F.sum(
            F.when(
                F.col("geography_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_geography_keys"),

        F.sum(
            F.when(
                F.col(
                    "initial_call_type_key"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_initial_call_type_keys"),

        F.sum(
            F.when(
                F.col(
                    "final_call_type_key"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_final_call_type_keys"),

        F.sum(
            F.when(
                F.col(
                    "initial_severity_key"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_initial_severity_keys"),

        F.sum(
            F.when(
                F.col(
                    "final_severity_key"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias("null_final_severity_keys"),

        F.sum(
            F.when(
                F.col("disposition_key").isNull(),
                1
            ).otherwise(0)
        ).alias("null_disposition_keys"),

        F.sum(
            F.when(
                F.col("incident_count") != 1,
                1
            ).otherwise(0)
        ).alias("invalid_incident_counts")
    )
    .collect()[0]
)

for field_name, field_value in (
    fact_validation.asDict().items()
):
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

# Core Assertions
assert (
    fact_validation["total_rows"]
    == expected_silver_rows
), (
    "Gold Fact row count does not match "
    "the latest validated Silver record count. "
    f"Expected {expected_silver_rows:,}, "
    f"found {fact_validation['total_rows']:,}."
)

assert (
    fact_validation["total_rows"]
    == fact_validation[
        "distinct_incident_ids"
    ]
), (
    "Gold Fact incident_id is not unique."
)

assert (
    fact_validation["null_incident_ids"]
    == 0
), "Gold Fact contains null incident IDs."

foreign_key_null_fields = [
    "null_date_keys",
    "null_time_keys",
    "null_geography_keys",
    "null_initial_call_type_keys",
    "null_final_call_type_keys",
    "null_initial_severity_keys",
    "null_final_severity_keys",
    "null_disposition_keys"
]

for field_name in foreign_key_null_fields:
    assert fact_validation[field_name] == 0, (
        f"Gold Fact validation failed: "
        f"{field_name} = "
        f"{fact_validation[field_name]:,}"
    )

assert (
    fact_validation["invalid_incident_counts"]
    == 0
), "Gold Fact contains invalid incident_count values."

print(
    "Gold Fact grain, row count and "
    "foreign-key null validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify response time metrics
response_measure_validation = (
    df_gold_fact_saved
    .agg(
        F.sum(
            F.when(
                F.col(
                    "valid_dispatch_response_seconds"
                ) < 0,
                1
            ).otherwise(0)
        ).alias(
            "negative_dispatch_response_values"
        ),

        F.sum(
            F.when(
                F.col(
                    "valid_incident_response_seconds"
                ) < 0,
                1
            ).otherwise(0)
        ).alias(
            "negative_incident_response_values"
        ),

        F.sum(
            F.when(
                F.col(
                    "valid_travel_time_seconds"
                ) < 0,
                1
            ).otherwise(0)
        ).alias(
            "negative_travel_time_values"
        ),

        F.sum(
            F.when(
                F.col(
                    "incident_duration_seconds"
                ) < 0,
                1
            ).otherwise(0)
        ).alias(
            "negative_incident_duration_values"
        ),

        F.sum(
            F.when(
                F.col(
                    "hospital_travel_seconds"
                ) < 0,
                1
            ).otherwise(0)
        ).alias(
            "negative_hospital_travel_values"
        )
    )
    .collect()[0]
)

for field_name, field_value in (
    response_measure_validation
    .asDict()
    .items()
):
    print(
        f"{field_name}: "
        f"{field_value:,}"
    )


for (
    field_name,
    field_value
) in response_measure_validation.asDict().items():

    assert field_value == 0, (
        f"{field_name} contains "
        f"{field_value:,} negative values."
    )

print(
    "Gold Fact response-measure "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that all foreign keys exist in the dimension tables
# Create a function to check Orphan Key
def count_orphaned_keys(
    fact_dataframe,
    fact_key_column,
    dimension_dataframe,
    dimension_key_column
):
    fact_keys = (
        fact_dataframe
        .select(
            F.col(fact_key_column).alias("key")
        )
        .distinct()
    )

    dimension_keys = (
        dimension_dataframe
        .select(
            F.col(dimension_key_column).alias("key")
        )
        .distinct()
    )

    return (
        fact_keys
        .join(
            F.broadcast(dimension_keys),
            on="key",
            how="left_anti"
        )
        .count()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforce foreign key referential integrity validation
foreign_key_checks = [
    (
        "date_key",
        df_dim_date,
        "date_key"
    ),
    (
        "time_key",
        df_dim_time,
        "time_key"
    ),
    (
        "geography_key",
        df_dim_geography,
        "geography_key"
    ),
    (
        "initial_call_type_key",
        df_dim_call_type,
        "call_type_key"
    ),
    (
        "final_call_type_key",
        df_dim_call_type,
        "call_type_key"
    ),
    (
        "initial_severity_key",
        df_dim_severity,
        "severity_key"
    ),
    (
        "final_severity_key",
        df_dim_severity,
        "severity_key"
    ),
    (
        "disposition_key",
        df_dim_disposition,
        "disposition_key"
    )
]

orphan_key_results = {}

for (
    fact_key,
    dimension_dataframe,
    dimension_key
) in foreign_key_checks:

    orphan_count = count_orphaned_keys(
        df_gold_fact_saved,
        fact_key,
        dimension_dataframe,
        dimension_key
    )

    orphan_key_results[fact_key] = (
        orphan_count
    )

    print(
        f"{fact_key} orphan keys: "
        f"{orphan_count:,}"
    )


for (
    fact_key,
    orphan_count
) in orphan_key_results.items():

    assert orphan_count == 0, (
        f"{fact_key} contains "
        f"{orphan_count:,} orphan keys."
    )

print(
    "Gold Fact referential-integrity "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compare the annual number of rows for Silver and Gold
df_silver_year_counts = (
    df_silver
    .groupBy("incident_year")
    .agg(
        F.count("*").alias(
            "silver_rows"
        )
    )
)

df_gold_year_counts = (
    df_gold_fact_saved
    .groupBy("incident_year")
    .agg(
        F.count("*").alias(
            "gold_rows"
        )
    )
)

df_year_count_comparison = (
    df_silver_year_counts
    .join(
        df_gold_year_counts,
        on="incident_year",
        how="full"
    )
    .fillna(
        0,
        [
            "silver_rows",
            "gold_rows"
        ]
    )
    .withColumn(
        "row_difference",
        F.col("gold_rows")
        - F.col("silver_rows")
    )
    .withColumn(
        "validation_status",
        F.when(
            F.col("row_difference") == 0,
            F.lit("PASS")
        )
        .otherwise(
            F.lit("FAIL")
        )
    )
    .orderBy("incident_year")
)

display(df_year_count_comparison)


failed_year_count = (
    df_year_count_comparison
    .filter(
        F.col("validation_status") == "FAIL"
    )
    .count()
)

assert failed_year_count == 0, (
    "One or more yearly Gold Fact row counts "
    "do not match Silver."
)

print(
    "Silver-to-Gold yearly row-count "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify Delta partition
gold_fact_detail = (
    spark.sql(
        f"DESCRIBE DETAIL "
        f"{gold_fact_table_name}"
    )
    .collect()[0]
    .asDict()
)

print(
    f"Format: "
    f"{gold_fact_detail['format']}"
)

print(
    f"Partition columns: "
    f"{gold_fact_detail['partitionColumns']}"
)

print(
    f"Number of files: "
    f"{gold_fact_detail['numFiles']}"
)

print(
    f"Size in bytes: "
    f"{gold_fact_detail['sizeInBytes']:,}"
)


assert (
    gold_fact_detail["format"] == "delta"
), "Gold Fact is not stored in Delta format."

assert (
    "incident_year"
    in gold_fact_detail["partitionColumns"]
), (
    "Gold Fact is not partitioned "
    "by incident_year."
)

print(
    "Gold Fact Delta-storage "
    "validation passed."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Incident Fact Result
# 
# The incident-level Gold fact table was created and validated successfully.
# 
# ### Output
# 
# - `gold_fact_ems_incident`
# 
# ### Grain
# 
# - One row represents one EMS incident
# 
# ### Validation
# 
# - Gold Fact row count matches the latest validated Silver record count
# - Incident identifiers are complete and unique
# - All dimension foreign keys are non-null
# - No orphaned dimension keys were found
# - All yearly row counts match the Silver source
# - Valid response and duration measures contain no negative values
# - `incident_count` equals one for every row
# - The table is stored in Delta format
# - The table is partitioned by `incident_year`
# 
# The daily statistical and anomaly-detection tables are built separately in
# `05_build_gold_analytics`.
