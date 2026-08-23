# NYC EMS Raw Data Inventory

## Dataset

- Source: NYC Open Data
- Dataset: EMS Incident Dispatch Data
- Dataset ID: `76xm-jjuj`
- Reporting period: 2019鈥?025
- Expected columns: 31
- Local raw data path: `D:\NYC_EMS_Data`

## Validation Summary

| Year | CSV Files | Local Rows | Source Rows | Size GB | Part Files | Empty Files | Schema Errors | Header Errors | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2025 | 7 | 1612273 | 1612273 | 0.44 | 0 | 0 | 0 | 0 | PASS |
| 2024 | 7 | 1630447 | 1630447 | 0.447 | 0 | 0 | 0 | 0 | PASS |
| 2023 | 7 | 1617839 | 1617839 | 0.445 | 0 | 0 | 0 | 0 | PASS |
| 2022 | 7 | 1583531 | 1583531 | 0.435 | 0 | 0 | 0 | 0 | PASS |
| 2021 | 6 | 1491454 | 1491454 | 0.411 | 0 | 0 | 0 | 0 | PASS |
| 2020 | 6 | 1412701 | 1412701 | 0.387 | 0 | 0 | 0 | 0 | PASS |
| 2019 | 7 | 1533251 | 1533251 | 0.426 | 0 | 0 | 0 | 0 | PASS |

## Overall Totals

- Local rows: 10881496
- Source rows: 10881496
- Total raw CSV size: 2.991 GB

## File Schema Validation

All CSV files contain 31 columns and use a consistent header.

## Validation Notes

- Each CSV file contains its own header row.
- Header rows are excluded from local row counts.
- Source row counts are queried from the Socrata API.
- Raw CSV files are excluded from GitHub.
- Exact row counts will be validated again after Bronze ingestion.
