# NYC EMS Analytics – Project Requirements

## Project Goal

Build a large-scale Microsoft Fabric and Power BI analytics solution
using NYC EMS incident dispatch data from 2019 to 2025.

The project demonstrates:

- Large-scale data ingestion and transformation
- Medallion architecture
- PySpark data engineering
- SQL analytics
- Power BI semantic modelling
- DAX statistical measures
- Standard deviation analysis
- Statistical upper and lower control limits
- Anomaly identification
- Time-series forecasting

## Core Statistical Requirements

- Mean
- Median
- Population standard deviation
- Variance
- P90 and P95 response time
- Coefficient of variation
- Upper warning limit: Mean + 2σ
- Lower warning limit: MAX(0, Mean - 2σ)
- Upper control limit: Mean + 3σ
- Lower control limit: MAX(0, Mean - 3σ)
- Out-of-control period identification

## Forecasting Requirements

- Monthly EMS incident volume forecast
- Monthly average response time forecast
- 95% confidence interval
- Actual versus forecast comparison
- Forecast error measurement

## Reporting Period

2019–2025

## Source

NYC Open Data – EMS Incident Dispatch Data
Dataset ID: 76xm-jjuj