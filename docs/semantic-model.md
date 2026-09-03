# NYC EMS Analytics Semantic Model

## Overview

The NYC EMS Analytics semantic model is a Microsoft Fabric Direct Lake model
built over the Gold-layer Delta tables in OneLake. It provides a governed
star-schema interface for Power BI analysis of 10,881,496 EMS incidents from
2019 to 2025.

The model supports all 12 project business questions across emergency demand,
response performance, reliability, incident outcomes, and dispatch
classification changes.

**Fabric Git item:** `NYC EMS Analytics Semantic Model.SemanticModel`  
**Repository path:**
`fabric/NYC EMS Analytics Semantic Model.SemanticModel/semantic-model.md`

> Call types are EMS dispatch classifications based on information available
> to dispatchers. They should not be interpreted as confirmed clinical
> diagnoses.

## Model Design

The model contains:

* Six conformed dimensions
* One incident-level fact table
* One daily analytical aggregate
* One dedicated measure table
* Ten one-to-many, single-direction relationships
* Active and inactive role-playing relationships for initial and final
  classifications

All relationships filter from the one-side dimension to the many-side fact or
aggregate table. Bidirectional filtering is not used.

## Tables and Grain

| Table | Role | Grain |
|---|---|---|
| `_Measures` | Dedicated DAX measure table | One placeholder row; no analytical grain |
| `gold_dim_date` | Date dimension | One row per calendar date |
| `gold_dim_time` | Time dimension | One row per EMS time key |
| `gold_dim_geography` | Geography dimension | One row per unique borough and geographic-attribute combination |
| `gold_dim_call_type` | Initial and final call-type role-playing dimension | One row per unique EMS dispatch call type |
| `gold_dim_severity` | Initial and final severity role-playing dimension | One row per unique EMS severity level |
| `gold_dim_disposition` | Incident-outcome dimension | One row per source disposition code |
| `gold_fact_ems_incident` | Incident fact | One row per unique EMS incident |
| `gold_agg_daily_performance` | Performance aggregate | One row per date, borough, and initial severity level |

The incident fact contains 10,881,496 unique incident records. The daily
aggregate stores reusable counts, sums, averages, percentiles, variability
statistics, control limits, and anomaly-quality fields.

## Relationships

| Many-side table and column | One-side table and column | Status | Purpose |
|---|---|---|---|
| `gold_fact_ems_incident[date_key]` | `gold_dim_date[date_key]` | Active | Incident-date analysis |
| `gold_fact_ems_incident[time_key]` | `gold_dim_time[time_key]` | Active | Hour and time-of-day analysis |
| `gold_fact_ems_incident[geography_key]` | `gold_dim_geography[geography_key]` | Active | Borough and geographic analysis |
| `gold_fact_ems_incident[disposition_key]` | `gold_dim_disposition[disposition_key]` | Active | Incident-outcome analysis |
| `gold_fact_ems_incident[initial_call_type_key]` | `gold_dim_call_type[call_type_key]` | Active | Initial call-type analysis |
| `gold_fact_ems_incident[final_call_type_key]` | `gold_dim_call_type[call_type_key]` | Inactive | Final call-type analysis |
| `gold_fact_ems_incident[initial_severity_key]` | `gold_dim_severity[severity_key]` | Active | Initial severity analysis |
| `gold_fact_ems_incident[final_severity_key]` | `gold_dim_severity[severity_key]` | Inactive | Final severity analysis |
| `gold_agg_daily_performance[date_key]` | `gold_dim_date[date_key]` | Active | Daily performance trend analysis |
| `gold_agg_daily_performance[initial_severity_key]` | `gold_dim_severity[severity_key]` | Active | Daily performance by initial severity |

The aggregate retains `borough` as a grouping attribute rather than connecting
to `gold_dim_geography`, because its grain is borough-level while the geography
dimension contains more detailed combinations.

## Role-Playing Dimensions

`gold_dim_call_type` and `gold_dim_severity` serve both initial and final
classification roles.

The initial relationships are active and support the default report filter
context. Final-classification measures activate the required inactive
relationship with `USERELATIONSHIP()` and disable the corresponding active
initial relationship with `CROSSFILTER(..., NONE)`.

Example pattern:

```DAX
Final Call Type Incidents =
CALCULATE (
    [Total Incidents],
    USERELATIONSHIP (
        gold_fact_ems_incident[final_call_type_key],
        gold_dim_call_type[call_type_key]
    ),
    CROSSFILTER (
        gold_fact_ems_incident[initial_call_type_key],
        gold_dim_call_type[call_type_key],
        NONE
    )
)
```

This prevents a call-type or severity filter from reaching the incident fact
through both the initial and final relationships at the same time.

## Date and Time Configuration

`gold_dim_date` is marked as the model Date table using `full_date`.

Important display and sorting configuration includes:

| Display column | Sort column or configuration |
|---|---|
| `day_of_week_short_name` | `day_of_week_sort` (Monday = 1 through Sunday = 7) |
| `day_of_week_name` | Day-of-week numeric business order |
| `month_name` / `month_short_name` | `month_number` |
| `year_month_label` | `year_month_sort` |
| `month_start_date` | Date column formatted as `mmm yyyy`; used for continuous monthly axes |
| `hour_label` | `hour_number` |
| `time_of_day` | `time_of_day_sort` |

The report-specific calculated columns `month_start_date` and
`day_of_week_sort` support readable continuous month axes and Monday-to-Sunday
heatmap ordering.

## Other Business Sorting

| Display column | Sort column |
|---|---|
| `gold_dim_severity[severity_label]` | `severity_level_code` |
| `gold_dim_disposition[disposition_category]` | `disposition_category_sort` |

Technical sort columns are hidden from report consumers after configuration.

## Missing and Undocumented Members

The model preserves source-data quality conditions instead of silently
dropping them.

* Missing dimension values use controlled `UNKNOWN` members.
* Call-type codes absent from the official reference remain visible as
  `UNDOCUMENTED CALL TYPE`.
* Undocumented disposition codes remain visible with mapping-status metadata.
* `UNKNOWN` and undocumented values are kept separate.

The Emergency Demand report excludes the controlled `UNKNOWN` member from the
Top 10 documented initial call-type ranking. The underlying records remain in
the model and are available for data-quality analysis.

## Measure Table

All report calculations are explicit DAX measures stored in `_Measures`. The
placeholder `Measure Holder` column is hidden. Measures are organised into the
following display folders:

### 01 Core KPIs

Core incident totals, valid-response counts, valid-response coverage, and
supporting response totals.

Representative measures include:

* `Total Incidents`
* `Valid Dispatch Response Incidents`
* `Valid Incident Response Incidents`
* `Valid Incident Response Percentage`
* `Valid Travel Time Incidents`

### 02 Response Performance

Weighted dispatch, incident-response, and travel-time measures, including
common-sample component analysis.

Representative measures include:

* `Average Dispatch Response Minutes`
* `Average Incident Response Minutes`
* `Average Travel Time Minutes`
* `Complete Response Component Incidents`
* `Dispatch Time Share Percentage`
* `Travel Time Share Percentage`

### 03 Reliability

Distribution, variability, percentile, statistical-control, and anomaly
measures.

Representative measures include:

* `Population Std Dev Response Minutes`
* `Response Time Coefficient of Variation`
* `P90 Response Minutes`
* `P95 Response Minutes`
* `Response Center Line Minutes`
* `Upper Control Limit Minutes`
* `Lower Control Limit Minutes`

### 04 Demand

Time-intelligence, daily-demand, demand-share, threshold, and high-demand
measures.

Representative measures include:

* `Incidents Previous Year`
* `Incident YoY Change`
* `Incident YoY Change Percentage`
* `Average Daily Incidents`
* `Peak Daily Incidents`
* Demand percentages by borough, weekday, hour, time of day, and initial call
  type
* `P75 Daily Demand Threshold`
* `High Demand Slow Response Flag`

### 05 Outcomes

Disposition, transport, and non-transport measures.

Representative measures include:

* `Disposition Incident Percentage`
* `Transported Incidents`
* `Transported Incident Percentage`
* Non-transport incident counts and percentages

### 06 Classification

Initial-to-final call-type and severity change analysis, final-role
distributions, percentages, and ranks.

Representative measures include:

* `Call Type Changed Incidents`
* `Call Type Change Percentage`
* `Severity Changed Incidents`
* `Severity Change Percentage`
* `Final Call Type Incidents`
* `Final Severity Incidents`
* Initial and final call-type and severity ranking measures

### 07 Data Quality

Measures for valid coverage, analyzable groups, sample sufficiency, control
limit availability, and reportable anomalies.

## Calculation Principles

### Explicit Measures

Unsupported implicit summarisation is disabled. Keys, codes, years, durations,
flags, and technical numeric columns use `Summarize by: None` unless aggregation
is explicitly required. User-facing calculations use DAX measures.

### Weighted Averages

Response-time averages are recalculated from valid response-time sums and valid
record counts in the current filter context. Daily group averages are not
averaged without weighting.

### Percentages

Percentage denominators remove only the dimension filter required by the
business definition. Date, borough, severity, and other applicable report
filters remain active.

### Reliability Statistics

Population standard deviation uses `STDEVX.P`. Percentile measures use
`PERCENTILEX.INC` over validated response observations. Coefficient of
variation divides population standard deviation by the corresponding mean.

### Response Components

Dispatch and travel shares use a common sample containing all required response
components. This prevents percentages from being calculated from incompatible
record populations.

### Rankings and Totals

Rank measures use `ALLSELECTED()` so they respond to external filters while
comparing the complete visible category set. `ISINSCOPE()` prevents misleading
rank values on total rows. Year-over-year change measures suppress totals where
a single-year comparison is not meaningful.

## Field Visibility

The following field types are hidden from report consumers where appropriate:

* Surrogate primary keys
* Fact-table foreign keys
* Technical sort columns
* Intermediate calculation measures
* Raw second-based fields when a user-facing minute measure is available
* Aggregate sums and valid-record counts used only by weighted measures
* Measure-table placeholder column

Business-facing labels, categories, flags, dates, and explicit measures remain
visible.

## Validation and QA

A dedicated hidden QA report page validates the model before formal report
development.

Validated areas include:

* Unfiltered total of 10,881,496 incidents
* Fact-to-dimension filter propagation
* Active and inactive role-playing relationships
* Initial and final call-type and severity distributions
* Percentage reconciliation to 100 percent
* Ranking behaviour and total-row suppression
* Weighted response-time calculations
* Dispatch and travel component reconciliation
* Control-limit and anomaly availability
* High-demand and slow-response logic
* Date filtering and year-over-year calculations

Gold-layer SQL validation separately confirms populated foreign keys, zero
orphan dimension keys, aggregate reconciliation, unique analytical grain, and
consistent statistical calculations.

## Report Pages

### Hidden QA Page

Validates semantic-model behaviour and measure correctness. It is hidden from
report consumers.

### 01 Executive Overview

Summarises incident volume, response performance, operational pressure,
borough demand, initial-severity performance, and disposition outcomes.

### 02 Emergency Demand

Provides total, average daily, and peak daily demand; monthly incident trends;
a Monday-to-Sunday and hour-of-day heatmap; and the Top 10 documented initial
call types.

Additional response-performance, reliability, and outcomes/classification
pages remain in development.

## Direct Lake Considerations

The semantic model uses Direct Lake mode over OneLake Gold tables. This avoids
import duplication while providing Power BI semantic modelling and DAX.

The downloadable PBIX option is unavailable for this Direct Lake semantic
model in the current Fabric service workflow. Workspace item definitions are
version-controlled through Fabric Git integration in the repository's
`fabric/` directory.

## Current Status

Completed:

* Star-schema table configuration
* Ten one-to-many, single-direction relationships
* Active initial and inactive final role-playing relationships
* Date-table and business-sort configuration
* Field visibility and summarisation configuration
* Dedicated measure table and display folders
* DAX measures supporting BQ01–BQ12
* Hidden semantic-model QA page
* Executive Overview report page
* Emergency Demand report page

In progress:

* Response Performance report page
* Reliability report page
* Outcomes and Classification report page
* Final navigation, accessibility, interactions, and documentation
