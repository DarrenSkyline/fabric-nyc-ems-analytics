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

# # NYC EMS Bronze Data Ingestion
# 
# ## Purpose
# 
# This notebook ingests NYC EMS Incident Dispatch CSV files from
# 2019 to 2025 into the Bronze layer.
# 
# ## Responsibilities
# 
# - Read all 47 source CSV files
# - Apply an explicit 31-column schema
# - Preserve source values without business transformations
# - Add ingestion metadata
# - Validate row counts by year
# - Validate required incident identifiers
# - Write the validated data to a partitioned Bronze Delta table
# 
# ## Source
# 
# - NYC Open Data
# - Dataset: EMS Incident Dispatch Data
# - Dataset ID: `76xm-jjuj`
# - Reporting period: 2019–2025
# - Expected rows: 10,881,496

# CELL ********************

# Import PySpark components
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define schema of 31 columns
# Read all columns as String, maintain the raw data

source_columns = [
    "incident_id",
    "incident_datetime",
    "initial_call_type",
    "initial_severity_level_code",
    "final_call_type",
    "final_severity_level_code",
    "first_assignment_datetime",
    "valid_dispatch_rspns_time_indc",
    "dispatch_response_seconds_qy",
    "first_activation_datetime",
    "first_on_scene_datetime",
    "valid_incident_rspns_time_indc",
    "incident_response_seconds_qy",
    "incident_travel_tm_seconds_qy",
    "first_to_hosp_datetime",
    "first_hosp_arrival_datetime",
    "incident_close_datetime",
    "held_indicator",
    "incident_disposition_code",
    "borough",
    "incident_dispatch_area",
    "zipcode",
    "policeprecinct",
    "citycouncildistrict",
    "communitydistrict",
    "communityschooldistrict",
    "congressionaldistrict",
    "reopen_indicator",
    "special_event_indicator",
    "standby_indicator",
    "transfer_indicator"
]

source_schema = StructType([
    StructField(column_name, StringType(), True)
    for column_name in source_columns
])

print(f"Expected source columns: {len(source_columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check csv files number
expected_file_counts = {
    2025: 7,
    2024: 7,
    2023: 7,
    2022: 7,
    2021: 6,
    2020: 6,
    2019: 7
}

file_validation_results = []
total_csv_files = 0

for year, expected_file_count in expected_file_counts.items():
    year_path = f"Files/raw/ems_incidents/year={year}"

    files = notebookutils.fs.ls(year_path)

    csv_files = [
        file
        for file in files
        if file.name.lower().endswith(".csv")
    ]

    actual_file_count = len(csv_files)
    total_csv_files += actual_file_count

    file_validation_results.append((
        year,
        expected_file_count,
        actual_file_count,
        "PASS"
        if expected_file_count == actual_file_count
        else "FAIL"
    ))

df_file_validation = spark.createDataFrame(
    file_validation_results,
    [
        "year",
        "expected_files",
        "actual_files",
        "status"
    ]
)

display(
    df_file_validation.orderBy(
        F.col("year").desc()
    )
)

assert total_csv_files == 47, (
    f"Expected 47 CSV files, found {total_csv_files}"
)

assert all(
    result[3] == "PASS"
    for result in file_validation_results
), "One or more yearly file-count checks failed."

print("Source file validation passed.")
print(f"Total CSV files: {total_csv_files}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read all csv files
raw_data_path = "Files/raw/ems_incidents/year=*/*.csv"

df_bronze_source = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("enforceSchema", "false")
    .schema(source_schema)
    .load(raw_data_path)
    .withColumn(
        "_source_file",
        F.input_file_name()
    )
    .withColumn(
        "_source_year",
        F.regexp_extract(
            F.col("_source_file"),
            r"year=(\d{4})",
            1
        ).cast("int")
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
)

print("CSV files were successfully registered as a DataFrame.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Chech schema
df_bronze_source.printSchema()

display(
    df_bronze_source.select(
        "incident_id",
        "incident_datetime",
        "initial_call_type",
        "incident_response_seconds_qy",
        "borough",
        "_source_year",
        "_source_file"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify rows of each year
expected_counts = {
    2025: 1612273,
    2024: 1630447,
    2023: 1617839,
    2022: 1583531,
    2021: 1491454,
    2020: 1412701,
    2019: 1533251
}

year_validation_df = (
    df_bronze_source
    .groupBy("_source_year")
    .agg(
        F.count("*").alias("actual_rows"),
        F.sum(
            F.when(
                F.col("incident_id").isNull() |
                (F.trim(F.col("incident_id")) == ""),
                1
            ).otherwise(0)
        ).alias("missing_incident_ids")
    )
    .orderBy(F.col("_source_year").desc())
)

validation_rows = year_validation_df.collect()

validation_results = []

for row in validation_rows:
    year = row["_source_year"]
    actual_rows = row["actual_rows"]
    expected_rows = expected_counts.get(year)
    missing_ids = row["missing_incident_ids"]

    validation_results.append((
        year,
        expected_rows,
        actual_rows,
        actual_rows - expected_rows if expected_rows is not None else None,
        missing_ids,
        "PASS"
        if expected_rows == actual_rows and missing_ids == 0
        else "FAIL"
    ))

validation_schema = [
    "year",
    "expected_rows",
    "actual_rows",
    "row_difference",
    "missing_incident_ids",
    "status"
]

df_validation_results = spark.createDataFrame(
    validation_results,
    validation_schema
)

display(
    df_validation_results.orderBy(
        F.col("year").desc()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final assertion
actual_counts = {
    row["_source_year"]: row["actual_rows"]
    for row in validation_rows
}

total_actual_rows = sum(actual_counts.values())
total_expected_rows = sum(expected_counts.values())

failed_years = [
    row
    for row in validation_results
    if row[-1] != "PASS"
]

assert actual_counts == expected_counts, (
    f"Year row-count validation failed: {actual_counts}"
)

assert len(failed_years) == 0, (
    f"Data-quality validation failed: {failed_years}"
)

assert total_actual_rows == 10881496, (
    f"Expected 10,881,496 rows but found {total_actual_rows}"
)

print("Bronze source validation passed.")
print(f"Total expected rows: {total_expected_rows:,}")
print(f"Total actual rows:   {total_actual_rows:,}")
print("Missing incident IDs: 0")
print("Validated years: 2019–2025")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write Bronze Delta Table
# 
# The validated source records are written to a Delta table without
# business-level cleaning or type conversion.
# 
# The table is partitioned by source year to improve year-based filtering
# and future incremental processing.

# CELL ********************

bronze_table_name = "bronze_ems_incidents"

(
    df_bronze_source.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_source_year")
    .saveAsTable(bronze_table_name)
)

print(f"Delta table created: {bronze_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Refresh and check tale
spark.catalog.refreshTable(bronze_table_name)

df_bronze_delta = spark.table(bronze_table_name)

print(f"Bronze Delta columns: {len(df_bronze_delta.columns)}")
df_bronze_delta.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify rows of each year of Delta table
delta_year_counts_df = (
    df_bronze_delta
    .groupBy("_source_year")
    .agg(
        F.count("*").alias("delta_rows"),
        F.sum(
            F.when(
                F.col("incident_id").isNull() |
                (F.trim(F.col("incident_id")) == ""),
                1
            ).otherwise(0)
        ).alias("missing_incident_ids")
    )
    .orderBy(F.col("_source_year").desc())
)

display(delta_year_counts_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Automatically assert Delta results

delta_count_rows = delta_year_counts_df.collect()

delta_counts = {
    row["_source_year"]: row["delta_rows"]
    for row in delta_count_rows
}

delta_missing_ids = sum(
    row["missing_incident_ids"]
    for row in delta_count_rows
)

delta_total_rows = sum(delta_counts.values())

assert delta_counts == expected_counts, (
    f"Delta year counts do not match expected counts: {delta_counts}"
)

assert delta_total_rows == 10881496, (
    f"Expected 10,881,496 Delta rows, found {delta_total_rows}"
)

assert delta_missing_ids == 0, (
    f"Found {delta_missing_ids} missing incident IDs"
)

print("Bronze Delta validation passed.")
print(f"Delta table: {bronze_table_name}")
print(f"Total rows: {delta_total_rows:,}")
print(f"Source columns: {len(source_columns)}")
print(f"Total table columns: {len(df_bronze_delta.columns)}")
print("Partition column: _source_year")
print(f"Total CSV files: {total_csv_files}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bronze Ingestion Audit
# 
# The ingestion audit table records expected and actual row counts,
# missing identifier counts and validation results for each source year.

# CELL ********************

# Create Bronze audit table
df_bronze_audit = (
    df_validation_results
    .withColumn(
        "source_path",
        F.lit(raw_data_path)
    )
    .withColumn(
        "bronze_table",
        F.lit(bronze_table_name)
    )
    .withColumn(
        "validated_at",
        F.current_timestamp()
    )
)

(
    df_bronze_audit.write
    .format("delta")
    .mode("append")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_ems_ingestion_audit")
)

display(
    spark.table("bronze_ems_ingestion_audit")
    .orderBy(F.col("year").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Delta table detail
display(
    spark.sql("""
        DESCRIBE DETAIL bronze_ems_incidents
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bronze Layer Result
# 
# The Bronze ingestion completed successfully.
# 
# ### Output tables
# 
# - `bronze_ems_incidents`
# - `bronze_ems_ingestion_audit`
# 
# ### Validation result
# 
# - 47 source CSV files validated
# - 10,881,496 source records validated
# - 31 original source columns validated
# - 7 source years identified
# - Delta table partitioned by `_source_year`
# - 0 missing incident identifiers
# - All yearly file-count and row-count validations passed
# 
# Business-level cleaning, deduplication and type conversion are performed
# in the Silver layer.
