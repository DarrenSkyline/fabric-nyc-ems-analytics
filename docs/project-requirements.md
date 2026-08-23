# NYC EMS Analytics — Project Requirements

## 1. Project Overview

This project builds a large-scale EMS analytics solution using
NYC EMS Incident Dispatch Data from 2019 to 2025.

The solution uses Microsoft Fabric, PySpark, Delta Lake, SQL,
DAX and Power BI to process and analyse millions of EMS incidents.

## 2. Project Objectives

- Build a Bronze, Silver and Gold medallion architecture
- Process large-scale CSV data using PySpark
- Create validated Delta tables in Microsoft Fabric
- Analyse EMS demand and operational response performance
- Measure variation using population standard deviation
- Calculate statistical upper and lower control limits
- Identify abnormal periods and operational instability
- Forecast future EMS incident demand and response performance
- Build an interactive Power BI dashboard
- Optimise the solution for an F2 Fabric capacity

## 3. Dataset Scope

- Source: NYC Open Data
- Dataset: EMS Incident Dispatch Data
- Dataset ID: `76xm-jjuj`
- Reporting period: 2019–2025
- Source columns: 31
- Source format: CSV
- Storage structure: Files partitioned by year

Raw data files are excluded from GitHub because of their size.

## 4. Core KPIs

- Total EMS Incidents
- Average Dispatch Response Time
- Average Incident Response Time
- Average Travel Time
- Median Incident Response Time
- P90 Incident Response Time
- P95 Incident Response Time
- Response Time Standard Deviation
- Coefficient of Variation
- Valid Response Time Rate
- Held Incident Rate
- Transfer Incident Rate
- Special Event Incident Rate

## 5. Statistical Requirements

The project treats the selected EMS incident records as a population
and primarily uses population standard deviation.

### Core statistics

- Mean
- Median
- Population variance
- Population standard deviation
- P90
- P95
- Minimum
- Maximum
- Coefficient of variation

### Statistical limits

- Center Line: Mean
- Upper Warning Limit: Mean + 2σ
- Lower Warning Limit: MAX(0, Mean - 2σ)
- Upper Control Limit: Mean + 3σ
- Lower Control Limit: MAX(0, Mean - 3σ)

### KPI status

- Normal: within two standard deviations
- Warning: outside two standard deviations
- Out of Control: outside three standard deviations

Statistical control limits represent historical process variation.
They must not be described as official NYC EMS service targets.

## 6. Forecasting Requirements

- Monthly EMS incident volume forecast
- Monthly average response time forecast
- Six-to-twelve-month forecast horizon
- 95% confidence interval
- Actual versus forecast comparison
- Forecast error measurement
- Historical trend and seasonality analysis

Where appropriate:

- Training period: 2019–2024
- Validation period: 2025

## 7. Analysis Dimensions

- Year
- Quarter
- Month
- Day of week
- Hour of day
- Borough
- ZIP code
- Dispatch area
- Initial call type
- Final call type
- Initial severity level
- Final severity level
- Held indicator
- Transfer indicator
- Standby indicator
- Special event indicator

## 8. Planned Power BI Pages

1. Executive Overview
2. Response Time and Statistical Control
3. Demand Trends and Forecasting
4. Operational Drivers and Geographic Analysis

## 9. Technology Stack

- Microsoft Fabric
- OneLake
- Fabric Lakehouse
- PySpark
- Delta Lake
- SQL Analytics Endpoint
- Power BI
- DAX
- Git and GitHub

## 10. Success Criteria

The project is complete when:

- All 2019–2025 source data is validated
- Bronze, Silver and Gold layers are implemented
- Data quality checks pass
- Statistical KPIs are implemented in PySpark, SQL or DAX
- Upper and lower control limits are visualised
- Forecasting is included in the Power BI report
- Report interactions and filters are validated
- Technical documentation and screenshots are published to GitHub