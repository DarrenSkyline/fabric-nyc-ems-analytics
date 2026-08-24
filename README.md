# NYC EMS Analytics

A large-scale emergency medical services (EMS) analytics portfolio project built using Microsoft Fabric, PySpark, SQL, DAX, and Power BI.

## Project Overview

This project analyses more than 10 million NYC EMS Incident Dispatch records from 2019 to 2025 to explore emergency demand, response performance, service reliability, and operational patterns across New York City.

The project demonstrates:

* Large-scale data ingestion and transformation
* Medallion architecture using Bronze, Silver, and Gold layers
* PySpark data engineering
* Data quality validation and feature engineering
* SQL analytics
* Dimensional modelling and star schema design
* Power BI semantic modelling
* DAX measures and statistical analysis
* Population standard deviation
* Upper and lower statistical control limits
* Anomaly detection
* Time-series analysis and forecasting
* Business-focused data storytelling

## Dataset

* **Source:** NYC Open Data
* **Dataset:** EMS Incident Dispatch Data
* **Dataset ID:** `76xm-jjuj`
* **Period:** 2019–2025
* **Source columns:** 31
* **Incident records:** 10,881,496
* **Source-year partitions:** 7

Raw source files are excluded from this repository because of their size.

The dataset contains operational EMS dispatch information, including incident timestamps, call types, severity levels, geographic areas, response times, incident dispositions, and operational indicators.

> **Note:** Call types represent EMS dispatch classifications based on information available to dispatchers and should not be interpreted as confirmed clinical diagnoses.

## Business Questions

The analytical model is designed around the following business questions.

### Emergency Demand

**BQ01.** How has EMS incident volume changed over time from 2019 to 2025?

**BQ02.** At what times of day and days of the week is EMS demand highest?

**BQ03.** Which NYC boroughs experience the highest EMS incident volumes?

**BQ04.** What are the most common EMS call types?

### Response Performance and Reliability

**BQ05.** How does EMS response time vary by initial severity level?

**BQ06.** How does EMS response performance differ across NYC boroughs?

**BQ07.** Whi ch areas have the greatest variability in EMS response times?

This question goes beyond average response time by using measures such as standard deviation, percentiles, and coefficient of variation to evaluate response reliability.

**BQ08.** Do held incidents experience longer or more variable response times than incidents that are immediately assigned?

**BQ09.** How much of total incident response time is associated with dispatch delay versus travel time?

**BQ10.** Which periods experience both high EMS demand and slower response performance?

### Incident Outcomes and Classification

**BQ11.** What are the most common EMS incident dispositions, including patient transport and non-transport outcomes?

**BQ12.** How frequently do call type and severity classifications change between the initial and final EMS dispatch records?

## Statistical Analysis

The project uses statistical measures to evaluate not only average EMS performance but also the consistency and variability of service delivery.

Key measures include:

* Mean response time
* Median response time
* Population standard deviation
* Coefficient of variation
* Percentiles (P75, P90, P95)
* Upper and lower statistical control limits
* Anomaly detection
* Time-series trends and forecasting

A particular focus is placed on identifying situations where two areas may have similar average response times but substantially different response-time variability.

## Data Architecture

The project follows a Medallion Architecture in Microsoft Fabric:

### Bronze Layer

Raw NYC EMS CSV files are ingested into a Delta table with minimal transformation.

Key Bronze validations include:

* Source file validation
* Row-count validation
* Schema validation
* Incident identifier completeness
* Incident identifier uniqueness
* Source-year partition validation

### Silver Layer

The Silver layer cleans, standardises, validates, and enriches incident-level EMS data while preserving the grain of:

> **One row = one EMS incident**

Transformations include:

* String trimming and null standardisation
* Date-time conversion
* Numeric type conversion
* Indicator standardisation
* Boolean feature creation
* Valid response-time measures
* Date and time feature engineering
* Time-of-day classification
* Response-time conversion
* Call-type and severity-change indicators
* Incident-duration calculations
* Data-quality status validation

Invalid response-time observations are retained in the source fields for traceability but excluded from validated response-time measures used for statistical analysis.

### Gold Layer

The Gold layer will provide business-ready fact and dimension tables designed around the project's business questions and Power BI analytical requirements.

The target model will support analysis across:

* Date and time
* Geography
* Call type
* Severity
* Incident disposition
* Operational indicators
* Response performance

## Analytics Workflow

```text
NYC Open Data
      ↓
Raw CSV Files
      ↓
Bronze Layer
Raw ingestion and validation
      ↓
Silver Layer
Cleaning, validation and feature engineering
      ↓
Gold Layer
Dimensional modelling and analytical tables
      ↓
SQL Analytics Endpoint
      ↓
Power BI Semantic Model
      ↓
DAX + Statistical Analysis
      ↓
Interactive Dashboard
      ↓
Business Insights and Recommendations
```

## Technology Stack

* Microsoft Fabric
* Fabric Lakehouse
* OneLake
* PySpark
* Delta Lake
* SQL Analytics Endpoint
* Power BI
* DAX
* Git
* GitHub

## Project Status

### Completed

* Project structure and repository setup
* Source data acquisition for 2019–2025
* Bronze ingestion pipeline
* Bronze data-quality validation
* Silver cleaning and transformation design
* Silver feature engineering
* Silver response-time validation
* Business question definition

### In Progress

* Silver Delta table persistence and validation
* Silver data-quality audit

### Next Steps

* Design the Gold dimensional model
* Build Gold fact and dimension tables
* Validate Gold analytical outputs
* Develop SQL analytical queries
* Build the Power BI semantic model
* Create statistical DAX measures
* Develop the Power BI dashboard
* Perform anomaly detection and forecasting
* Document findings and business recommendations
