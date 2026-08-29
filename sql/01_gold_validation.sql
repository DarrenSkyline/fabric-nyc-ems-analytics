/*
===============================================================================
NYC EMS Analytics
Gold Layer SQL Validation

Purpose:
    Independently validate the Gold tables through the Microsoft Fabric
    SQL Analytics Endpoint.

Validation scope:
    1. Confirm that all Gold tables are accessible
    2. Validate table row counts
    3. Validate dimension-table completeness
    4. Validate fact-table identifiers
    5. Validate foreign-key integrity
    6. Reconcile the daily aggregate with the fact table
    7. Validate the daily analytical grain
    8. Review the final Gold data-quality audit

Expected fact-table row count:
    10,881,496
===============================================================================
*/


-- ============================================================================
-- 01. Gold table row counts
-- ============================================================================

SELECT
    'gold_dim_date' AS table_name,
    COUNT_BIG(*) AS row_count
FROM dbo.gold_dim_date

UNION ALL

SELECT
    'gold_dim_time',
    COUNT_BIG(*)
FROM dbo.gold_dim_time

UNION ALL

SELECT
    'gold_dim_geography',
    COUNT_BIG(*)
FROM dbo.gold_dim_geography

UNION ALL

SELECT
    'gold_dim_call_type',
    COUNT_BIG(*)
FROM dbo.gold_dim_call_type

UNION ALL

SELECT
    'gold_dim_severity',
    COUNT_BIG(*)
FROM dbo.gold_dim_severity

UNION ALL

SELECT
    'gold_dim_disposition',
    COUNT_BIG(*)
FROM dbo.gold_dim_disposition

UNION ALL

SELECT
    'gold_fact_ems_incident',
    COUNT_BIG(*)
FROM dbo.gold_fact_ems_incident

UNION ALL

SELECT
    'gold_agg_daily_performance',
    COUNT_BIG(*)
FROM dbo.gold_agg_daily_performance

UNION ALL

SELECT
    'gold_ems_data_quality_audit',
    COUNT_BIG(*)
FROM dbo.gold_ems_data_quality_audit

ORDER BY table_name;


-- ============================================================================
-- 02. Validate the Gold fact-table row count
-- ============================================================================

SELECT
    COUNT_BIG(*) AS actual_fact_rows,
    CAST(10881496 AS BIGINT) AS expected_fact_rows,
    COUNT_BIG(*) - CAST(10881496 AS BIGINT) AS row_count_difference,
    CASE
        WHEN COUNT_BIG(*) = 10881496 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM dbo.gold_fact_ems_incident;


-- ============================================================================
-- 03. Inspect Gold table structures
-- ============================================================================

SELECT TOP (5) *
FROM dbo.gold_fact_ems_incident;


SELECT TOP (5) *
FROM dbo.gold_agg_daily_performance;


SELECT TOP (10) *
FROM dbo.gold_ems_data_quality_audit
ORDER BY audited_at DESC;


-- ============================================================================
-- 04. Validate fact-table incident identifiers
-- ============================================================================

SELECT
    COUNT_BIG(*) AS total_fact_rows,

    COUNT_BIG(
        CASE
            WHEN incident_id IS NULL THEN 1
        END
    ) AS missing_incident_id_rows,

    COUNT_BIG(DISTINCT incident_id)
        AS distinct_incident_ids,

    COUNT_BIG(*)
        - COUNT_BIG(DISTINCT incident_id)
        AS duplicate_incident_id_rows,

    CASE
        WHEN COUNT_BIG(
                 CASE
                     WHEN incident_id IS NULL THEN 1
                 END
             ) = 0
         AND COUNT_BIG(*) =
             COUNT_BIG(DISTINCT incident_id)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM dbo.gold_fact_ems_incident;


-- ============================================================================
-- 05. Identify duplicate incident identifiers
-- Expected result: no rows
-- ============================================================================

SELECT
    incident_id,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_fact_ems_incident
GROUP BY incident_id
HAVING COUNT_BIG(*) > 1
ORDER BY occurrence_count DESC;


-- ============================================================================
-- 06. Reconcile Silver and Gold fact-table row counts
-- ============================================================================

WITH silver_count AS
(
    SELECT
        COUNT_BIG(*) AS row_count
    FROM dbo.silver_ems_incidents
),
gold_count AS
(
    SELECT
        COUNT_BIG(*) AS row_count
    FROM dbo.gold_fact_ems_incident
)
SELECT
    s.row_count AS silver_rows,
    g.row_count AS gold_fact_rows,
    g.row_count - s.row_count AS row_count_difference,

    CASE
        WHEN s.row_count = g.row_count
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM silver_count AS s
CROSS JOIN gold_count AS g;


-- ============================================================================
-- 07. Reconcile Silver and Gold row counts by incident year
-- ============================================================================

WITH silver_yearly_counts AS
(
    SELECT
        incident_year,
        COUNT_BIG(*) AS silver_rows
    FROM dbo.silver_ems_incidents
    GROUP BY incident_year
),
gold_yearly_counts AS
(
    SELECT
        incident_year,
        COUNT_BIG(*) AS gold_rows
    FROM dbo.gold_fact_ems_incident
    GROUP BY incident_year
)
SELECT
    COALESCE(s.incident_year, g.incident_year)
        AS incident_year,

    COALESCE(s.silver_rows, 0)
        AS silver_rows,

    COALESCE(g.gold_rows, 0)
        AS gold_rows,

    COALESCE(g.gold_rows, 0)
        - COALESCE(s.silver_rows, 0)
        AS row_count_difference,

    CASE
        WHEN COALESCE(s.silver_rows, 0)
             = COALESCE(g.gold_rows, 0)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM silver_yearly_counts AS s

FULL OUTER JOIN gold_yearly_counts AS g
    ON s.incident_year = g.incident_year

ORDER BY incident_year;


-- ============================================================================
-- 08. Summarise yearly row-count reconciliation
-- ============================================================================

WITH silver_yearly_counts AS
(
    SELECT
        incident_year,
        COUNT_BIG(*) AS silver_rows
    FROM dbo.silver_ems_incidents
    GROUP BY incident_year
),
gold_yearly_counts AS
(
    SELECT
        incident_year,
        COUNT_BIG(*) AS gold_rows
    FROM dbo.gold_fact_ems_incident
    GROUP BY incident_year
),
yearly_comparison AS
(
    SELECT
        COALESCE(s.incident_year, g.incident_year)
            AS incident_year,

        COALESCE(s.silver_rows, 0)
            AS silver_rows,

        COALESCE(g.gold_rows, 0)
            AS gold_rows

    FROM silver_yearly_counts AS s

    FULL OUTER JOIN gold_yearly_counts AS g
        ON s.incident_year = g.incident_year
)
SELECT
    COUNT(*) AS compared_years,

    SUM(
        CASE
            WHEN silver_rows <> gold_rows THEN 1
            ELSE 0
        END
    ) AS failed_years,

    CASE
        WHEN COUNT(*) = 7
         AND SUM(
                 CASE
                     WHEN silver_rows <> gold_rows THEN 1
                     ELSE 0
                 END
             ) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM yearly_comparison;


-- ============================================================================
-- 09. Validate fact-table foreign-key completeness
-- Purpose:
--     Confirm that all eight dimension foreign keys are populated.
-- Expected result:
--     All null-key counts = 0
--     validation_status = PASS
-- ============================================================================

WITH foreign_key_completeness AS
(
    SELECT
        COUNT_BIG(*) AS total_fact_rows,

        SUM(CASE
            WHEN date_key IS NULL THEN 1
            ELSE 0
        END) AS null_date_keys,

        SUM(CASE
            WHEN time_key IS NULL THEN 1
            ELSE 0
        END) AS null_time_keys,

        SUM(CASE
            WHEN geography_key IS NULL THEN 1
            ELSE 0
        END) AS null_geography_keys,

        SUM(CASE
            WHEN initial_call_type_key IS NULL THEN 1
            ELSE 0
        END) AS null_initial_call_type_keys,

        SUM(CASE
            WHEN final_call_type_key IS NULL THEN 1
            ELSE 0
        END) AS null_final_call_type_keys,

        SUM(CASE
            WHEN initial_severity_key IS NULL THEN 1
            ELSE 0
        END) AS null_initial_severity_keys,

        SUM(CASE
            WHEN final_severity_key IS NULL THEN 1
            ELSE 0
        END) AS null_final_severity_keys,

        SUM(CASE
            WHEN disposition_key IS NULL THEN 1
            ELSE 0
        END) AS null_disposition_keys

    FROM dbo.gold_fact_ems_incident
)
SELECT
    fkc.total_fact_rows,
    fkc.null_date_keys,
    fkc.null_time_keys,
    fkc.null_geography_keys,
    fkc.null_initial_call_type_keys,
    fkc.null_final_call_type_keys,
    fkc.null_initial_severity_keys,
    fkc.null_final_severity_keys,
    fkc.null_disposition_keys,

    CASE
        WHEN fkc.null_date_keys = 0
         AND fkc.null_time_keys = 0
         AND fkc.null_geography_keys = 0
         AND fkc.null_initial_call_type_keys = 0
         AND fkc.null_final_call_type_keys = 0
         AND fkc.null_initial_severity_keys = 0
         AND fkc.null_final_severity_keys = 0
         AND fkc.null_disposition_keys = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM foreign_key_completeness AS fkc;


-- ============================================================================
-- 10. Validate orphan dimension keys
-- Purpose:
--     Confirm that every foreign key in the fact table matches a row in the
--     corresponding dimension table.
-- Expected result:
--     All orphan-key counts = 0
--     validation_status = PASS
-- ============================================================================

WITH orphan_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_fact_rows,

        SUM(CASE
            WHEN d.date_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_date_keys,

        SUM(CASE
            WHEN t.time_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_time_keys,

        SUM(CASE
            WHEN g.geography_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_geography_keys,

        SUM(CASE
            WHEN initial_ct.call_type_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_initial_call_type_keys,

        SUM(CASE
            WHEN final_ct.call_type_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_final_call_type_keys,

        SUM(CASE
            WHEN initial_sv.severity_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_initial_severity_keys,

        SUM(CASE
            WHEN final_sv.severity_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_final_severity_keys,

        SUM(CASE
            WHEN disp.disposition_key IS NULL THEN 1
            ELSE 0
        END) AS orphan_disposition_keys

    FROM dbo.gold_fact_ems_incident AS f

    LEFT JOIN dbo.gold_dim_date AS d
        ON f.date_key = d.date_key

    LEFT JOIN dbo.gold_dim_time AS t
        ON f.time_key = t.time_key

    LEFT JOIN dbo.gold_dim_geography AS g
        ON f.geography_key = g.geography_key

    LEFT JOIN dbo.gold_dim_call_type AS initial_ct
        ON f.initial_call_type_key =
           initial_ct.call_type_key

    LEFT JOIN dbo.gold_dim_call_type AS final_ct
        ON f.final_call_type_key =
           final_ct.call_type_key

    LEFT JOIN dbo.gold_dim_severity AS initial_sv
        ON f.initial_severity_key =
           initial_sv.severity_key

    LEFT JOIN dbo.gold_dim_severity AS final_sv
        ON f.final_severity_key =
           final_sv.severity_key

    LEFT JOIN dbo.gold_dim_disposition AS disp
        ON f.disposition_key =
           disp.disposition_key
)
SELECT
    ov.total_fact_rows,
    ov.orphan_date_keys,
    ov.orphan_time_keys,
    ov.orphan_geography_keys,
    ov.orphan_initial_call_type_keys,
    ov.orphan_final_call_type_keys,
    ov.orphan_initial_severity_keys,
    ov.orphan_final_severity_keys,
    ov.orphan_disposition_keys,

    CASE
        WHEN ov.orphan_date_keys = 0
         AND ov.orphan_time_keys = 0
         AND ov.orphan_geography_keys = 0
         AND ov.orphan_initial_call_type_keys = 0
         AND ov.orphan_final_call_type_keys = 0
         AND ov.orphan_initial_severity_keys = 0
         AND ov.orphan_final_severity_keys = 0
         AND ov.orphan_disposition_keys = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM orphan_validation AS ov;


-- ============================================================================
-- 11. Validate dimension primary-key completeness and uniqueness
-- Purpose:
--     Confirm that every dimension is non-empty and that its key contains
--     no null or duplicate values.
-- Expected result:
--     null_key_rows = 0
--     duplicate_key_rows = 0
--     validation_status = PASS
-- ============================================================================

WITH dimension_key_validation AS
(
    SELECT
        'gold_dim_date' AS table_name,
        COUNT_BIG(*) AS total_rows,
        COUNT_BIG(*) - COUNT_BIG(date_key)
            AS null_key_rows,
        COUNT_BIG(date_key)
            - COUNT_BIG(DISTINCT date_key)
            AS duplicate_key_rows
    FROM dbo.gold_dim_date

    UNION ALL

    SELECT
        'gold_dim_time',
        COUNT_BIG(*),
        COUNT_BIG(*) - COUNT_BIG(time_key),
        COUNT_BIG(time_key)
            - COUNT_BIG(DISTINCT time_key)
    FROM dbo.gold_dim_time

    UNION ALL

    SELECT
        'gold_dim_geography',
        COUNT_BIG(*),
        COUNT_BIG(*) - COUNT_BIG(geography_key),
        COUNT_BIG(geography_key)
            - COUNT_BIG(DISTINCT geography_key)
    FROM dbo.gold_dim_geography

    UNION ALL

    SELECT
        'gold_dim_call_type',
        COUNT_BIG(*),
        COUNT_BIG(*) - COUNT_BIG(call_type_key),
        COUNT_BIG(call_type_key)
            - COUNT_BIG(DISTINCT call_type_key)
    FROM dbo.gold_dim_call_type

    UNION ALL

    SELECT
        'gold_dim_severity',
        COUNT_BIG(*),
        COUNT_BIG(*) - COUNT_BIG(severity_key),
        COUNT_BIG(severity_key)
            - COUNT_BIG(DISTINCT severity_key)
    FROM dbo.gold_dim_severity

    UNION ALL

    SELECT
        'gold_dim_disposition',
        COUNT_BIG(*),
        COUNT_BIG(*) - COUNT_BIG(disposition_key),
        COUNT_BIG(disposition_key)
            - COUNT_BIG(DISTINCT disposition_key)
    FROM dbo.gold_dim_disposition
)
SELECT
    table_name,
    total_rows,
    null_key_rows,
    duplicate_key_rows,

    CASE
        WHEN total_rows > 0
         AND null_key_rows = 0
         AND duplicate_key_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM dimension_key_validation
ORDER BY table_name;


-- ============================================================================
-- 12. Identify duplicate dimension keys
-- Expected result:
--     All queries return no rows
-- ============================================================================


-- Date dimension

SELECT
    date_key,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_dim_date
GROUP BY date_key
HAVING COUNT_BIG(*) > 1;


-- Time dimension

SELECT
    time_key,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_dim_time
GROUP BY time_key
HAVING COUNT_BIG(*) > 1;


-- Geography dimension

SELECT
    geography_key,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_dim_geography
GROUP BY geography_key
HAVING COUNT_BIG(*) > 1;


-- Call-type dimension

SELECT
    call_type_key,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_dim_call_type
GROUP BY call_type_key
HAVING COUNT_BIG(*) > 1;


-- Severity dimension

SELECT
    severity_key,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_dim_severity
GROUP BY severity_key
HAVING COUNT_BIG(*) > 1;


-- Disposition dimension

SELECT
    disposition_key,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_dim_disposition
GROUP BY disposition_key
HAVING COUNT_BIG(*) > 1;


-- ============================================================================
-- 13. Validate daily analytical grain uniqueness
-- Purpose:
--     Confirm that the daily analytical table contains no duplicate rows at
--     the defined grain:
--         incident_date × borough × initial_severity_key
-- Expected result:
--     duplicate_grain_groups = 0
--     duplicate_excess_rows = 0
--     validation_status = PASS
-- ============================================================================

WITH duplicate_daily_grain AS
(
    SELECT
        incident_date,
        borough,
        initial_severity_key,
        COUNT_BIG(*) AS occurrence_count
    FROM dbo.gold_agg_daily_performance
    GROUP BY
        incident_date,
        borough,
        initial_severity_key
    HAVING COUNT_BIG(*) > 1
)
SELECT
    COUNT_BIG(*) AS duplicate_grain_groups,

    COALESCE(
        SUM(occurrence_count - 1),
        0
    ) AS duplicate_excess_rows,

    CASE
        WHEN COUNT_BIG(*) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM duplicate_daily_grain;

SELECT
    incident_date,
    borough,
    initial_severity_key,
    initial_severity_level_code,
    initial_severity_label,
    COUNT_BIG(*) AS occurrence_count
FROM dbo.gold_agg_daily_performance
GROUP BY
    incident_date,
    borough,
    initial_severity_key,
    initial_severity_level_code,
    initial_severity_label
HAVING COUNT_BIG(*) > 1
ORDER BY
    occurrence_count DESC,
    incident_date,
    borough,
    initial_severity_key;


-- ============================================================================
-- 14. Reconcile daily incident totals with the Gold fact table
-- Purpose:
--     Confirm that aggregation preserved every fact-table incident.
-- Expected result:
--     All totals = 10,881,496
--     All differences = 0
--     validation_status = PASS
-- ============================================================================

WITH fact_totals AS
(
    SELECT
        COUNT_BIG(*) AS fact_row_count,
        SUM(CAST(incident_count AS BIGINT))
            AS fact_incident_count
    FROM dbo.gold_fact_ems_incident
),
daily_totals AS
(
    SELECT
        SUM(total_incidents)
            AS daily_incident_count
    FROM dbo.gold_agg_daily_performance
)
SELECT
    f.fact_row_count,
    f.fact_incident_count,
    d.daily_incident_count,

    f.fact_incident_count
        - f.fact_row_count
        AS fact_count_difference,

    d.daily_incident_count
        - f.fact_incident_count
        AS daily_fact_difference,

    CASE
        WHEN f.fact_row_count = 10881496
         AND f.fact_incident_count = f.fact_row_count
         AND d.daily_incident_count = f.fact_incident_count
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM fact_totals AS f
CROSS JOIN daily_totals AS d;


-- ============================================================================
-- 15. Validate daily grouping-field completeness
-- Purpose:
--     Confirm that all fields defining or describing the daily analytical
--     grain are populated and internally consistent.
-- Expected result:
--     All issue counts = 0
--     validation_status = PASS
-- ============================================================================

WITH daily_grouping_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        SUM(CASE
            WHEN date_key IS NULL THEN 1
            ELSE 0
        END) AS null_date_key_rows,

        SUM(CASE
            WHEN incident_date IS NULL THEN 1
            ELSE 0
        END) AS null_incident_date_rows,

        SUM(CASE
            WHEN incident_year IS NULL THEN 1
            ELSE 0
        END) AS null_incident_year_rows,

        SUM(CASE
            WHEN borough IS NULL
              OR LTRIM(RTRIM(borough)) = ''
            THEN 1
            ELSE 0
        END) AS missing_borough_rows,

        SUM(CASE
            WHEN initial_severity_key IS NULL THEN 1
            ELSE 0
        END) AS null_initial_severity_key_rows,

        SUM(CASE
            WHEN initial_severity_level_code IS NULL THEN 1
            ELSE 0
        END) AS null_initial_severity_code_rows,

        SUM(CASE
            WHEN initial_severity_label IS NULL
              OR LTRIM(RTRIM(initial_severity_label)) = ''
            THEN 1
            ELSE 0
        END) AS missing_initial_severity_label_rows,

        SUM(CASE
            WHEN incident_date IS NOT NULL
             AND incident_year <> YEAR(incident_date)
            THEN 1
            ELSE 0
        END) AS incident_year_mismatch_rows,

        SUM(CASE
            WHEN incident_date IS NOT NULL
             AND date_key <>
                 YEAR(incident_date) * 10000
                 + MONTH(incident_date) * 100
                 + DAY(incident_date)
            THEN 1
            ELSE 0
        END) AS date_key_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    dgv.*,
    CASE
        WHEN dgv.total_daily_rows > 0
         AND dgv.null_date_key_rows = 0
         AND dgv.null_incident_date_rows = 0
         AND dgv.null_incident_year_rows = 0
         AND dgv.missing_borough_rows = 0
         AND dgv.null_initial_severity_key_rows = 0
         AND dgv.null_initial_severity_code_rows = 0
         AND dgv.missing_initial_severity_label_rows = 0
         AND dgv.incident_year_mismatch_rows = 0
         AND dgv.date_key_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM daily_grouping_validation AS dgv;


-- ============================================================================
-- 16. Validate daily response counts and valid-response rates
-- Purpose:
--     Confirm that:
--       1. Daily incident counts are positive
--       2. Valid-response counts are within valid ranges
--       3. Valid-response rate matches its source counts
--       4. Percentage conversion is correct
-- Expected result:
--     All issue counts = 0
--     validation_status = PASS
-- ============================================================================

WITH response_count_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        SUM(CASE
            WHEN total_incidents IS NULL
              OR total_incidents <= 0
            THEN 1
            ELSE 0
        END) AS invalid_total_incident_rows,

        SUM(CASE
            WHEN valid_dispatch_response_count IS NULL
              OR valid_dispatch_response_count < 0
              OR valid_dispatch_response_count > total_incidents
            THEN 1
            ELSE 0
        END) AS invalid_dispatch_count_rows,

        SUM(CASE
            WHEN valid_incident_response_count IS NULL
              OR valid_incident_response_count < 0
              OR valid_incident_response_count > total_incidents
            THEN 1
            ELSE 0
        END) AS invalid_incident_response_count_rows,

        SUM(CASE
            WHEN valid_travel_time_count IS NULL
              OR valid_travel_time_count < 0
              OR valid_travel_time_count > total_incidents
            THEN 1
            ELSE 0
        END) AS invalid_travel_count_rows,

        SUM(CASE
            WHEN valid_response_rate IS NULL
              OR valid_response_rate < 0
              OR valid_response_rate > 1
            THEN 1
            ELSE 0
        END) AS invalid_response_rate_rows,

        SUM(CASE
            WHEN valid_response_percentage IS NULL
              OR valid_response_percentage < 0
              OR valid_response_percentage > 100
            THEN 1
            ELSE 0
        END) AS invalid_response_percentage_rows,

        SUM(CASE
            WHEN total_incidents > 0
             AND ABS(
                    valid_response_rate
                    -
                    (
                        CAST(valid_incident_response_count AS FLOAT)
                        / CAST(total_incidents AS FLOAT)
                    )
                 ) > 0.0000001
            THEN 1
            ELSE 0
        END) AS response_rate_mismatch_rows,

        SUM(CASE
            WHEN valid_response_rate IS NOT NULL
             AND ABS(
                    valid_response_percentage
                    - ROUND(valid_response_rate * 100.0, 2)
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS response_percentage_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    rcv.total_daily_rows,
    rcv.invalid_total_incident_rows,
    rcv.invalid_dispatch_count_rows,
    rcv.invalid_incident_response_count_rows,
    rcv.invalid_travel_count_rows,
    rcv.invalid_response_rate_rows,
    rcv.invalid_response_percentage_rows,
    rcv.response_rate_mismatch_rows,
    rcv.response_percentage_mismatch_rows,

    CASE
        WHEN rcv.total_daily_rows > 0
         AND rcv.invalid_total_incident_rows = 0
         AND rcv.invalid_dispatch_count_rows = 0
         AND rcv.invalid_incident_response_count_rows = 0
         AND rcv.invalid_travel_count_rows = 0
         AND rcv.invalid_response_rate_rows = 0
         AND rcv.invalid_response_percentage_rows = 0
         AND rcv.response_rate_mismatch_rows = 0
         AND rcv.response_percentage_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM response_count_validation AS rcv;


-- ============================================================================
-- 17. Validate response-time averages, unit conversions and null consistency
-- Purpose:
--     Confirm that:
--       1. Average seconds equal total seconds divided by valid-record count
--       2. Average minutes use decimal arithmetic and correct rounding
--       3. Null measures are consistent with zero valid-record counts
--
-- Expected result:
--     All issue counts = 0
--     validation_status = PASS
-- ============================================================================

WITH response_measure_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        -- --------------------------------------------------------------------
        -- Dispatch-response null consistency
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_dispatch_response_count = 0
             AND (
                    total_dispatch_response_seconds IS NOT NULL
                 OR average_dispatch_response_seconds IS NOT NULL
                 OR average_dispatch_response_minutes IS NOT NULL
             )
            THEN 1
            ELSE 0
        END) AS dispatch_unexpected_value_rows,

        SUM(CASE
            WHEN valid_dispatch_response_count > 0
             AND (
                    total_dispatch_response_seconds IS NULL
                 OR average_dispatch_response_seconds IS NULL
                 OR average_dispatch_response_minutes IS NULL
             )
            THEN 1
            ELSE 0
        END) AS dispatch_missing_value_rows,

        -- --------------------------------------------------------------------
        -- Incident-response null consistency
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_incident_response_count = 0
             AND (
                    total_incident_response_seconds IS NOT NULL
                 OR average_incident_response_seconds IS NOT NULL
                 OR average_incident_response_minutes IS NOT NULL
             )
            THEN 1
            ELSE 0
        END) AS incident_response_unexpected_value_rows,

        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND (
                    total_incident_response_seconds IS NULL
                 OR average_incident_response_seconds IS NULL
                 OR average_incident_response_minutes IS NULL
             )
            THEN 1
            ELSE 0
        END) AS incident_response_missing_value_rows,

        -- --------------------------------------------------------------------
        -- Travel-time null consistency
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_travel_time_count = 0
             AND (
                    total_travel_time_seconds IS NOT NULL
                 OR average_travel_time_seconds IS NOT NULL
                 OR average_travel_time_minutes IS NOT NULL
             )
            THEN 1
            ELSE 0
        END) AS travel_unexpected_value_rows,

        SUM(CASE
            WHEN valid_travel_time_count > 0
             AND (
                    total_travel_time_seconds IS NULL
                 OR average_travel_time_seconds IS NULL
                 OR average_travel_time_minutes IS NULL
             )
            THEN 1
            ELSE 0
        END) AS travel_missing_value_rows,

        -- --------------------------------------------------------------------
        -- Average-seconds calculations
        -- average seconds = total seconds / valid count
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_dispatch_response_count > 0
             AND ABS(
                    CAST(
                        average_dispatch_response_seconds
                        AS DECIMAL(38, 10)
                    )
                    -
                    (
                        CAST(
                            total_dispatch_response_seconds
                            AS DECIMAL(38, 10)
                        )
                        /
                        NULLIF(
                            CAST(
                                valid_dispatch_response_count
                                AS DECIMAL(38, 10)
                            ),
                            0
                        )
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS dispatch_average_mismatch_rows,

        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND ABS(
                    CAST(
                        average_incident_response_seconds
                        AS DECIMAL(38, 10)
                    )
                    -
                    (
                        CAST(
                            total_incident_response_seconds
                            AS DECIMAL(38, 10)
                        )
                        /
                        NULLIF(
                            CAST(
                                valid_incident_response_count
                                AS DECIMAL(38, 10)
                            ),
                            0
                        )
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS incident_response_average_mismatch_rows,

        SUM(CASE
            WHEN valid_travel_time_count > 0
             AND ABS(
                    CAST(
                        average_travel_time_seconds
                        AS DECIMAL(38, 10)
                    )
                    -
                    (
                        CAST(
                            total_travel_time_seconds
                            AS DECIMAL(38, 10)
                        )
                        /
                        NULLIF(
                            CAST(
                                valid_travel_time_count
                                AS DECIMAL(38, 10)
                            ),
                            0
                        )
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS travel_average_mismatch_rows,

        -- --------------------------------------------------------------------
        -- Seconds-to-minutes conversions
        -- minutes = ROUND(total seconds / valid count / 60, 2)
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_dispatch_response_count > 0
             AND ABS(
                    CAST(
                        average_dispatch_response_minutes
                        AS DECIMAL(18, 6)
                    )
                    -
                    ROUND(
                        CAST(
                            total_dispatch_response_seconds
                            AS DECIMAL(38, 10)
                        )
                        /
                        NULLIF(
                            CAST(
                                valid_dispatch_response_count
                                AS DECIMAL(38, 10)
                            ),
                            0
                        )
                        / 60,
                        2
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS dispatch_minutes_mismatch_rows,

        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND ABS(
                    CAST(
                        average_incident_response_minutes
                        AS DECIMAL(18, 6)
                    )
                    -
                    ROUND(
                        CAST(
                            total_incident_response_seconds
                            AS DECIMAL(38, 10)
                        )
                        /
                        NULLIF(
                            CAST(
                                valid_incident_response_count
                                AS DECIMAL(38, 10)
                            ),
                            0
                        )
                        / 60,
                        2
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS incident_response_minutes_mismatch_rows,

        SUM(CASE
            WHEN valid_travel_time_count > 0
             AND ABS(
                    CAST(
                        average_travel_time_minutes
                        AS DECIMAL(18, 6)
                    )
                    -
                    ROUND(
                        CAST(
                            total_travel_time_seconds
                            AS DECIMAL(38, 10)
                        )
                        /
                        NULLIF(
                            CAST(
                                valid_travel_time_count
                                AS DECIMAL(38, 10)
                            ),
                            0
                        )
                        / 60,
                        2
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS travel_minutes_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    rmv.total_daily_rows,

    rmv.dispatch_unexpected_value_rows,
    rmv.dispatch_missing_value_rows,

    rmv.incident_response_unexpected_value_rows,
    rmv.incident_response_missing_value_rows,

    rmv.travel_unexpected_value_rows,
    rmv.travel_missing_value_rows,

    rmv.dispatch_average_mismatch_rows,
    rmv.incident_response_average_mismatch_rows,
    rmv.travel_average_mismatch_rows,

    rmv.dispatch_minutes_mismatch_rows,
    rmv.incident_response_minutes_mismatch_rows,
    rmv.travel_minutes_mismatch_rows,

    CASE
        WHEN rmv.total_daily_rows > 0
         AND rmv.dispatch_unexpected_value_rows = 0
         AND rmv.dispatch_missing_value_rows = 0
         AND rmv.incident_response_unexpected_value_rows = 0
         AND rmv.incident_response_missing_value_rows = 0
         AND rmv.travel_unexpected_value_rows = 0
         AND rmv.travel_missing_value_rows = 0
         AND rmv.dispatch_average_mismatch_rows = 0
         AND rmv.incident_response_average_mismatch_rows = 0
         AND rmv.travel_average_mismatch_rows = 0
         AND rmv.dispatch_minutes_mismatch_rows = 0
         AND rmv.incident_response_minutes_mismatch_rows = 0
         AND rmv.travel_minutes_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM response_measure_validation AS rmv;


-- ============================================================================
-- 18. Validate response-time statistical measures
-- Purpose:
--     Confirm that:
--       1. Statistical measures follow valid null rules
--       2. Response-time measures are non-negative
--       3. Percentiles follow the expected order
--       4. The average is within the observed range
--       5. The coefficient of variation is calculated correctly
--
-- Expected result:
--     All issue counts = 0
--     validation_status = PASS
-- ============================================================================

WITH statistical_measure_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        -- --------------------------------------------------------------------
        -- Null consistency
        -- No valid responses should produce null statistical measures
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_incident_response_count = 0
             AND (
                    average_incident_response_seconds IS NOT NULL
                 OR population_stddev_response_seconds IS NOT NULL
                 OR minimum_response_seconds IS NOT NULL
                 OR maximum_response_seconds IS NOT NULL
                 OR p50_response_seconds IS NOT NULL
                 OR p75_response_seconds IS NOT NULL
                 OR p90_response_seconds IS NOT NULL
                 OR p95_response_seconds IS NOT NULL
             )
            THEN 1
            ELSE 0
        END) AS unexpected_statistics_without_response_rows,

        -- Valid responses should produce populated statistical measures
        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND (
                    average_incident_response_seconds IS NULL
                 OR population_stddev_response_seconds IS NULL
                 OR minimum_response_seconds IS NULL
                 OR maximum_response_seconds IS NULL
                 OR p50_response_seconds IS NULL
                 OR p75_response_seconds IS NULL
                 OR p90_response_seconds IS NULL
                 OR p95_response_seconds IS NULL
             )
            THEN 1
            ELSE 0
        END) AS missing_statistics_with_response_rows,

        -- --------------------------------------------------------------------
        -- Non-negative statistical measures
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN average_incident_response_seconds < 0
              OR population_stddev_response_seconds < 0
              OR minimum_response_seconds < 0
              OR maximum_response_seconds < 0
              OR p50_response_seconds < 0
              OR p75_response_seconds < 0
              OR p90_response_seconds < 0
              OR p95_response_seconds < 0
            THEN 1
            ELSE 0
        END) AS negative_statistical_measure_rows,

        -- --------------------------------------------------------------------
        -- Minimum and maximum consistency
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND minimum_response_seconds
                 > maximum_response_seconds
            THEN 1
            ELSE 0
        END) AS invalid_minimum_maximum_rows,

        -- Average should be within the observed range
        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND (
                    average_incident_response_seconds
                        < minimum_response_seconds

                 OR average_incident_response_seconds
                        > maximum_response_seconds
             )
            THEN 1
            ELSE 0
        END) AS average_outside_range_rows,

        -- --------------------------------------------------------------------
        -- Percentile ordering
        -- minimum <= P50 <= P75 <= P90 <= P95 <= maximum
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN valid_incident_response_count > 0
             AND (
                    minimum_response_seconds
                        > p50_response_seconds

                 OR p50_response_seconds
                        > p75_response_seconds

                 OR p75_response_seconds
                        > p90_response_seconds

                 OR p90_response_seconds
                        > p95_response_seconds

                 OR p95_response_seconds
                        > maximum_response_seconds
             )
            THEN 1
            ELSE 0
        END) AS invalid_percentile_order_rows,

        -- --------------------------------------------------------------------
        -- Coefficient-of-variation null consistency
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    average_incident_response_seconds IS NULL
                 OR average_incident_response_seconds <= 0
                 )
             AND response_coefficient_of_variation IS NOT NULL
            THEN 1
            ELSE 0
        END) AS unexpected_cv_rows,

        SUM(CASE
            WHEN average_incident_response_seconds > 0
             AND (
                    response_coefficient_of_variation IS NULL
                 OR response_cv_percentage IS NULL
             )
            THEN 1
            ELSE 0
        END) AS missing_cv_rows,

        -- CV cannot be negative
        SUM(CASE
            WHEN response_coefficient_of_variation < 0
              OR response_cv_percentage < 0
            THEN 1
            ELSE 0
        END) AS negative_cv_rows,

        -- --------------------------------------------------------------------
        -- CV calculation
        -- CV = population standard deviation / average
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN average_incident_response_seconds > 0
             AND ABS(
                    response_coefficient_of_variation
                    -
                    (
                        population_stddev_response_seconds
                        /
                        average_incident_response_seconds
                    )
                 ) > 0.0000001
            THEN 1
            ELSE 0
        END) AS cv_calculation_mismatch_rows,

        -- CV percentage = ROUND(CV * 100, 2)
        SUM(CASE
            WHEN response_coefficient_of_variation IS NOT NULL
             AND ABS(
                    CAST(
                        response_cv_percentage
                        AS DECIMAL(18, 6)
                    )
                    -
                    ROUND(
                        CAST(
                            response_coefficient_of_variation
                            AS DECIMAL(38, 10)
                        ) * 100,
                        2
                    )
                 ) > 0.000001
            THEN 1
            ELSE 0
        END) AS cv_percentage_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    smv.total_daily_rows,

    smv.unexpected_statistics_without_response_rows,
    smv.missing_statistics_with_response_rows,
    smv.negative_statistical_measure_rows,

    smv.invalid_minimum_maximum_rows,
    smv.average_outside_range_rows,
    smv.invalid_percentile_order_rows,

    smv.unexpected_cv_rows,
    smv.missing_cv_rows,
    smv.negative_cv_rows,
    smv.cv_calculation_mismatch_rows,
    smv.cv_percentage_mismatch_rows,

    CASE
        WHEN smv.total_daily_rows > 0
         AND smv.unexpected_statistics_without_response_rows = 0
         AND smv.missing_statistics_with_response_rows = 0
         AND smv.negative_statistical_measure_rows = 0
         AND smv.invalid_minimum_maximum_rows = 0
         AND smv.average_outside_range_rows = 0
         AND smv.invalid_percentile_order_rows = 0
         AND smv.unexpected_cv_rows = 0
         AND smv.missing_cv_rows = 0
         AND smv.negative_cv_rows = 0
         AND smv.cv_calculation_mismatch_rows = 0
         AND smv.cv_percentage_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM statistical_measure_validation AS smv;


-- ============================================================================
-- 19. Validate statistical seconds-to-minutes conversions
-- Purpose:
--     Confirm that statistical reporting measures use the same decimal
--     conversion and rounding logic as the Gold analytics notebook.
--
-- Conversion rule:
--     ROUND(CAST(seconds AS DECIMAL(24,6)) / 60, 2)
--
-- Expected result:
--     All mismatch counts = 0
--     validation_status = PASS
-- ============================================================================

WITH statistical_minute_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        -- --------------------------------------------------------------------
        -- Population standard deviation
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    population_stddev_response_seconds IS NULL
                AND population_stddev_response_minutes IS NOT NULL
                 )
              OR (
                    population_stddev_response_seconds IS NOT NULL
                AND population_stddev_response_minutes IS NULL
                 )
              OR (
                    population_stddev_response_seconds IS NOT NULL
                AND population_stddev_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            population_stddev_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                population_stddev_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS stddev_minutes_mismatch_rows,

        -- --------------------------------------------------------------------
        -- P50
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    p50_response_seconds IS NULL
                AND p50_response_minutes IS NOT NULL
                 )
              OR (
                    p50_response_seconds IS NOT NULL
                AND p50_response_minutes IS NULL
                 )
              OR (
                    p50_response_seconds IS NOT NULL
                AND p50_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            p50_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                p50_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS p50_minutes_mismatch_rows,

        -- --------------------------------------------------------------------
        -- P75
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    p75_response_seconds IS NULL
                AND p75_response_minutes IS NOT NULL
                 )
              OR (
                    p75_response_seconds IS NOT NULL
                AND p75_response_minutes IS NULL
                 )
              OR (
                    p75_response_seconds IS NOT NULL
                AND p75_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            p75_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                p75_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS p75_minutes_mismatch_rows,

        -- --------------------------------------------------------------------
        -- P90
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    p90_response_seconds IS NULL
                AND p90_response_minutes IS NOT NULL
                 )
              OR (
                    p90_response_seconds IS NOT NULL
                AND p90_response_minutes IS NULL
                 )
              OR (
                    p90_response_seconds IS NOT NULL
                AND p90_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            p90_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                p90_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS p90_minutes_mismatch_rows,

        -- --------------------------------------------------------------------
        -- P95
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    p95_response_seconds IS NULL
                AND p95_response_minutes IS NOT NULL
                 )
              OR (
                    p95_response_seconds IS NOT NULL
                AND p95_response_minutes IS NULL
                 )
              OR (
                    p95_response_seconds IS NOT NULL
                AND p95_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            p95_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                p95_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS p95_minutes_mismatch_rows,

        -- --------------------------------------------------------------------
        -- Minimum response time
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    minimum_response_seconds IS NULL
                AND minimum_response_minutes IS NOT NULL
                 )
              OR (
                    minimum_response_seconds IS NOT NULL
                AND minimum_response_minutes IS NULL
                 )
              OR (
                    minimum_response_seconds IS NOT NULL
                AND minimum_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            minimum_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                minimum_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS minimum_minutes_mismatch_rows,

        -- --------------------------------------------------------------------
        -- Maximum response time
        -- --------------------------------------------------------------------

        SUM(CASE
            WHEN (
                    maximum_response_seconds IS NULL
                AND maximum_response_minutes IS NOT NULL
                 )
              OR (
                    maximum_response_seconds IS NOT NULL
                AND maximum_response_minutes IS NULL
                 )
              OR (
                    maximum_response_seconds IS NOT NULL
                AND maximum_response_minutes IS NOT NULL
                AND ABS(
                        CAST(
                            maximum_response_minutes
                            AS DECIMAL(18, 6)
                        )
                        -
                        ROUND(
                            CAST(
                                maximum_response_seconds
                                AS DECIMAL(24, 6)
                            ) / 60,
                            2
                        )
                    ) > 0.000001
                 )
            THEN 1
            ELSE 0
        END) AS maximum_minutes_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    smv.total_daily_rows,
    smv.stddev_minutes_mismatch_rows,
    smv.p50_minutes_mismatch_rows,
    smv.p75_minutes_mismatch_rows,
    smv.p90_minutes_mismatch_rows,
    smv.p95_minutes_mismatch_rows,
    smv.minimum_minutes_mismatch_rows,
    smv.maximum_minutes_mismatch_rows,

    CASE
        WHEN smv.total_daily_rows > 0
         AND smv.stddev_minutes_mismatch_rows = 0
         AND smv.p50_minutes_mismatch_rows = 0
         AND smv.p75_minutes_mismatch_rows = 0
         AND smv.p90_minutes_mismatch_rows = 0
         AND smv.p95_minutes_mismatch_rows = 0
         AND smv.minimum_minutes_mismatch_rows = 0
         AND smv.maximum_minutes_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM statistical_minute_validation AS smv;


-- ============================================================================
-- 20. Validate operational counts, rates and percentages
-- ============================================================================

WITH operational_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        SUM(CASE
            WHEN held_incident_count < 0
              OR held_incident_count > total_incidents
            THEN 1 ELSE 0
        END) AS invalid_held_count_rows,

        SUM(CASE
            WHEN special_event_count < 0
              OR special_event_count > total_incidents
            THEN 1 ELSE 0
        END) AS invalid_special_event_count_rows,

        SUM(CASE
            WHEN transfer_incident_count < 0
              OR transfer_incident_count > total_incidents
            THEN 1 ELSE 0
        END) AS invalid_transfer_count_rows,

        SUM(CASE
            WHEN ABS(
                    held_incident_rate
                    -
                    CAST(held_incident_count AS FLOAT)
                    / CAST(total_incidents AS FLOAT)
                 ) > 0.0000001
            THEN 1 ELSE 0
        END) AS held_rate_mismatch_rows,

        SUM(CASE
            WHEN ABS(
                    special_event_rate
                    -
                    CAST(special_event_count AS FLOAT)
                    / CAST(total_incidents AS FLOAT)
                 ) > 0.0000001
            THEN 1 ELSE 0
        END) AS special_event_rate_mismatch_rows,

        SUM(CASE
            WHEN ABS(
                    transfer_incident_rate
                    -
                    CAST(transfer_incident_count AS FLOAT)
                    / CAST(total_incidents AS FLOAT)
                 ) > 0.0000001
            THEN 1 ELSE 0
        END) AS transfer_rate_mismatch_rows,

        SUM(CASE
            WHEN ABS(
                    CAST(
                        held_incident_percentage
                        AS DECIMAL(18,6)
                    )
                    -
                    ROUND(
                        CAST(
                            held_incident_count
                            AS DECIMAL(38,10)
                        )
                        /
                        CAST(
                            total_incidents
                            AS DECIMAL(38,10)
                        ) * 100,
                        2
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS held_percentage_mismatch_rows,

        SUM(CASE
            WHEN ABS(
                    CAST(
                        special_event_percentage
                        AS DECIMAL(18,6)
                    )
                    -
                    ROUND(
                        CAST(
                            special_event_count
                            AS DECIMAL(38,10)
                        )
                        /
                        CAST(
                            total_incidents
                            AS DECIMAL(38,10)
                        ) * 100,
                        2
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS special_event_percentage_mismatch_rows,

        SUM(CASE
            WHEN ABS(
                    CAST(
                        transfer_incident_percentage
                        AS DECIMAL(18,6)
                    )
                    -
                    ROUND(
                        CAST(
                            transfer_incident_count
                            AS DECIMAL(38,10)
                        )
                        /
                        CAST(
                            total_incidents
                            AS DECIMAL(38,10)
                        ) * 100,
                        2
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS transfer_percentage_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    ov.total_daily_rows,
    ov.invalid_held_count_rows,
    ov.invalid_special_event_count_rows,
    ov.invalid_transfer_count_rows,
    ov.held_rate_mismatch_rows,
    ov.special_event_rate_mismatch_rows,
    ov.transfer_rate_mismatch_rows,
    ov.held_percentage_mismatch_rows,
    ov.special_event_percentage_mismatch_rows,
    ov.transfer_percentage_mismatch_rows,

    CASE
        WHEN ov.total_daily_rows > 0
         AND ov.invalid_held_count_rows = 0
         AND ov.invalid_special_event_count_rows = 0
         AND ov.invalid_transfer_count_rows = 0
         AND ov.held_rate_mismatch_rows = 0
         AND ov.special_event_rate_mismatch_rows = 0
         AND ov.transfer_rate_mismatch_rows = 0
         AND ov.held_percentage_mismatch_rows = 0
         AND ov.special_event_percentage_mismatch_rows = 0
         AND ov.transfer_percentage_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM operational_validation AS ov;


-- ============================================================================
-- 21. Validate sample-quality and baseline-eligibility rules
-- ============================================================================

WITH sample_quality_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        SUM(CASE
            WHEN has_sufficient_sample
                 <> CASE
                        WHEN valid_incident_response_count >= 30
                        THEN 1
                        ELSE 0
                    END
            THEN 1 ELSE 0
        END) AS sample_flag_mismatch_rows,

        SUM(CASE
            WHEN analytics_quality_status
                 <> CASE
                        WHEN valid_incident_response_count = 0
                            THEN 'NO_VALID_RESPONSE'
                        WHEN valid_incident_response_count < 30
                            THEN 'LOW_SAMPLE_SIZE'
                        ELSE 'SUFFICIENT_SAMPLE'
                    END
            THEN 1 ELSE 0
        END) AS quality_status_mismatch_rows,

        SUM(CASE
            WHEN historical_baseline_day_count < 0
              OR historical_baseline_day_count > 30
            THEN 1 ELSE 0
        END) AS invalid_baseline_count_rows,

        SUM(CASE
            WHEN has_sufficient_baseline
                 <> CASE
                        WHEN historical_baseline_day_count >= 20
                         AND daily_average_stddev_seconds IS NOT NULL
                        THEN 1
                        ELSE 0
                    END
            THEN 1 ELSE 0
        END) AS baseline_flag_mismatch_rows,

        SUM(CASE
            WHEN historical_baseline_day_count = 0
             AND (
                    response_center_line_seconds IS NOT NULL
                 OR daily_average_stddev_seconds IS NOT NULL
             )
            THEN 1 ELSE 0
        END) AS unexpected_empty_baseline_measure_rows,

        SUM(CASE
            WHEN historical_baseline_day_count > 0
             AND (
                    response_center_line_seconds IS NULL
                 OR daily_average_stddev_seconds IS NULL
             )
            THEN 1 ELSE 0
        END) AS missing_baseline_measure_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    sqv.total_daily_rows,
    sqv.sample_flag_mismatch_rows,
    sqv.quality_status_mismatch_rows,
    sqv.invalid_baseline_count_rows,
    sqv.baseline_flag_mismatch_rows,
    sqv.unexpected_empty_baseline_measure_rows,
    sqv.missing_baseline_measure_rows,

    CASE
        WHEN sqv.total_daily_rows > 0
         AND sqv.sample_flag_mismatch_rows = 0
         AND sqv.quality_status_mismatch_rows = 0
         AND sqv.invalid_baseline_count_rows = 0
         AND sqv.baseline_flag_mismatch_rows = 0
         AND sqv.unexpected_empty_baseline_measure_rows = 0
         AND sqv.missing_baseline_measure_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM sample_quality_validation AS sqv;


-- ============================================================================
-- 22. Validate statistical control limits
-- ============================================================================

WITH control_limit_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        SUM(CASE
            WHEN has_sufficient_baseline = 1
             AND (
                    response_center_line_seconds IS NULL
                 OR daily_average_stddev_seconds IS NULL
                 OR upper_control_limit_seconds IS NULL
                 OR lower_control_limit_seconds IS NULL
             )
            THEN 1 ELSE 0
        END) AS missing_control_limit_rows,

        SUM(CASE
            WHEN has_sufficient_baseline = 0
             AND (
                    upper_control_limit_seconds IS NOT NULL
                 OR lower_control_limit_seconds IS NOT NULL
             )
            THEN 1 ELSE 0
        END) AS unexpected_control_limit_rows,

        SUM(CASE
            WHEN lower_control_limit_seconds < 0
            THEN 1 ELSE 0
        END) AS negative_lower_control_limit_rows,

        SUM(CASE
            WHEN has_sufficient_baseline = 1
             AND (
                    lower_control_limit_seconds
                        > response_center_line_seconds
                 OR response_center_line_seconds
                        > upper_control_limit_seconds
             )
            THEN 1 ELSE 0
        END) AS invalid_control_limit_order_rows,

        SUM(CASE
            WHEN has_sufficient_baseline = 1
             AND ABS(
                    upper_control_limit_seconds
                    -
                    (
                        response_center_line_seconds
                        + 3.0 * daily_average_stddev_seconds
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS upper_control_limit_mismatch_rows,

        SUM(CASE
            WHEN has_sufficient_baseline = 1
             AND ABS(
                    lower_control_limit_seconds
                    -
                    CASE
                        WHEN response_center_line_seconds
                             - 3.0 * daily_average_stddev_seconds < 0
                        THEN 0.0
                        ELSE response_center_line_seconds
                             - 3.0 * daily_average_stddev_seconds
                    END
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS lower_control_limit_mismatch_rows,

        SUM(CASE
            WHEN response_center_line_seconds IS NOT NULL
             AND ABS(
                    CAST(
                        response_center_line_minutes
                        AS DECIMAL(18,6)
                    )
                    -
                    ROUND(
                        CAST(
                            response_center_line_seconds
                            AS DECIMAL(24,6)
                        ) / 60,
                        2
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS center_line_minutes_mismatch_rows,

        SUM(CASE
            WHEN upper_control_limit_seconds IS NOT NULL
             AND ABS(
                    CAST(
                        upper_control_limit_minutes
                        AS DECIMAL(18,6)
                    )
                    -
                    ROUND(
                        CAST(
                            upper_control_limit_seconds
                            AS DECIMAL(24,6)
                        ) / 60,
                        2
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS upper_limit_minutes_mismatch_rows,

        SUM(CASE
            WHEN lower_control_limit_seconds IS NOT NULL
             AND ABS(
                    CAST(
                        lower_control_limit_minutes
                        AS DECIMAL(18,6)
                    )
                    -
                    ROUND(
                        CAST(
                            lower_control_limit_seconds
                            AS DECIMAL(24,6)
                        ) / 60,
                        2
                    )
                 ) > 0.000001
            THEN 1 ELSE 0
        END) AS lower_limit_minutes_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    clv.total_daily_rows,
    clv.missing_control_limit_rows,
    clv.unexpected_control_limit_rows,
    clv.negative_lower_control_limit_rows,
    clv.invalid_control_limit_order_rows,
    clv.upper_control_limit_mismatch_rows,
    clv.lower_control_limit_mismatch_rows,
    clv.center_line_minutes_mismatch_rows,
    clv.upper_limit_minutes_mismatch_rows,
    clv.lower_limit_minutes_mismatch_rows,

    CASE
        WHEN clv.total_daily_rows > 0
         AND clv.missing_control_limit_rows = 0
         AND clv.unexpected_control_limit_rows = 0
         AND clv.negative_lower_control_limit_rows = 0
         AND clv.invalid_control_limit_order_rows = 0
         AND clv.upper_control_limit_mismatch_rows = 0
         AND clv.lower_control_limit_mismatch_rows = 0
         AND clv.center_line_minutes_mismatch_rows = 0
         AND clv.upper_limit_minutes_mismatch_rows = 0
         AND clv.lower_limit_minutes_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM control_limit_validation AS clv;


-- ============================================================================
-- 23. Validate response-anomaly rules
-- ============================================================================

WITH anomaly_validation AS
(
    SELECT
        COUNT_BIG(*) AS total_daily_rows,

        SUM(CASE
            WHEN is_above_upper_control_limit
                 <> CASE
                        WHEN has_sufficient_baseline = 1
                         AND average_incident_response_seconds
                             > upper_control_limit_seconds
                        THEN 1
                        ELSE 0
                    END
            THEN 1 ELSE 0
        END) AS above_upper_flag_mismatch_rows,

        SUM(CASE
            WHEN is_below_lower_control_limit
                 <> CASE
                        WHEN has_sufficient_baseline = 1
                         AND average_incident_response_seconds
                             < lower_control_limit_seconds
                        THEN 1
                        ELSE 0
                    END
            THEN 1 ELSE 0
        END) AS below_lower_flag_mismatch_rows,

        SUM(CASE
            WHEN is_response_anomaly
                 <> CASE
                        WHEN is_above_upper_control_limit = 1
                          OR is_below_lower_control_limit = 1
                        THEN 1
                        ELSE 0
                    END
            THEN 1 ELSE 0
        END) AS anomaly_flag_mismatch_rows,

        SUM(CASE
            WHEN response_anomaly_direction
                 <> CASE
                        WHEN has_sufficient_baseline = 0
                            THEN 'Insufficient Baseline'
                        WHEN is_above_upper_control_limit = 1
                            THEN 'Above UCL'
                        WHEN is_below_lower_control_limit = 1
                            THEN 'Below LCL'
                        ELSE 'Within Control Limits'
                    END
            THEN 1 ELSE 0
        END) AS anomaly_direction_mismatch_rows,

        SUM(CASE
            WHEN is_reportable_response_anomaly
                 <> CASE
                        WHEN has_sufficient_sample = 1
                         AND has_sufficient_baseline = 1
                         AND is_response_anomaly = 1
                        THEN 1
                        ELSE 0
                    END
            THEN 1 ELSE 0
        END) AS reportable_anomaly_mismatch_rows

    FROM dbo.gold_agg_daily_performance
)
SELECT
    av.total_daily_rows,
    av.above_upper_flag_mismatch_rows,
    av.below_lower_flag_mismatch_rows,
    av.anomaly_flag_mismatch_rows,
    av.anomaly_direction_mismatch_rows,
    av.reportable_anomaly_mismatch_rows,

    CASE
        WHEN av.total_daily_rows > 0
         AND av.above_upper_flag_mismatch_rows = 0
         AND av.below_lower_flag_mismatch_rows = 0
         AND av.anomaly_flag_mismatch_rows = 0
         AND av.anomaly_direction_mismatch_rows = 0
         AND av.reportable_anomaly_mismatch_rows = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status

FROM anomaly_validation AS av;


-- ============================================================================
-- 24. Validate the latest Gold data-quality audit
-- ============================================================================

WITH latest_gold_audit AS
(
    SELECT TOP (1)
        audit_run_id,
        audited_at,
        fact_row_count,
        distinct_incident_ids,
        missing_incident_ids,
        duplicate_incident_ids,
        daily_row_count,
        aggregated_incident_count,
        duplicate_daily_group_count,
        invalid_control_limit_count,
        invalid_reportable_anomaly_count,
        dimension_validation_passed,
        fact_identifier_validation_passed,
        fact_reconciliation_passed,
        daily_grain_validation_passed,
        control_limit_validation_passed,
        anomaly_validation_passed,
        validation_status
    FROM dbo.gold_ems_data_quality_audit
    ORDER BY audited_at DESC
)
SELECT
    lga.audit_run_id,
    lga.audited_at,
    lga.fact_row_count,
    lga.distinct_incident_ids,
    lga.missing_incident_ids,
    lga.duplicate_incident_ids,
    lga.daily_row_count,
    lga.aggregated_incident_count,
    lga.duplicate_daily_group_count,
    lga.invalid_control_limit_count,
    lga.invalid_reportable_anomaly_count,
    lga.validation_status AS stored_validation_status,

    CASE
        WHEN lga.fact_row_count = 10881496
         AND lga.distinct_incident_ids = 10881496
         AND lga.missing_incident_ids = 0
         AND lga.duplicate_incident_ids = 0
         AND lga.daily_row_count = 99786
         AND lga.aggregated_incident_count = 10881496
         AND lga.duplicate_daily_group_count = 0
         AND lga.invalid_control_limit_count = 0
         AND lga.invalid_reportable_anomaly_count = 0
         AND lga.dimension_validation_passed = 1
         AND lga.fact_identifier_validation_passed = 1
         AND lga.fact_reconciliation_passed = 1
         AND lga.daily_grain_validation_passed = 1
         AND lga.control_limit_validation_passed = 1
         AND lga.anomaly_validation_passed = 1
         AND lga.validation_status = 'PASS'
        THEN 'PASS'
        ELSE 'FAIL'
    END AS sql_validation_status

FROM latest_gold_audit AS lga;