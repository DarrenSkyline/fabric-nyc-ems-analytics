/*
===============================================================================
NYC EMS Analytics
Response Performance Analysis

File:
    sql/03_response_performance_analysis.sql

Business questions:
    BQ05. How does EMS response time vary by initial severity level?
    BQ06. How does EMS response performance differ across NYC boroughs?
    BQ07. Which areas have the greatest variability in EMS response times?
    BQ08. Do held incidents experience longer or more variable response times than incidents that are immediately assigned?
    BQ09. How much of total incident response time is associated with dispatch delay versus travel time?
    BQ10. Which periods experience both high EMS demand and slower response performance?

Notes:
    Weighted averages use response-time totals and valid-record counts.
    Invalid response-time records remain excluded.
===============================================================================
*/


-- ============================================================================
-- BQ05:
-- 01. Response performance by initial severity level
-- Purpose:
--     Compare incident volume, response coverage, weighted response time
--     and pooled population standard deviation by severity.
-- ============================================================================

WITH severity_base AS
(
    SELECT
        initial_severity_key,
        initial_severity_level_code,
        initial_severity_label,

        SUM(total_incidents)
            AS total_incidents,

        SUM(valid_incident_response_count)
            AS valid_response_count,

        SUM(total_incident_response_seconds)
            AS total_response_seconds,

        SUM(
            CAST(
                valid_incident_response_count
                AS FLOAT
            )
            *
            (
                POWER(
                    population_stddev_response_seconds,
                    2
                )
                +
                POWER(
                    average_incident_response_seconds,
                    2
                )
            )
        ) AS pooled_second_moment_total

    FROM dbo.gold_agg_daily_performance

    GROUP BY
        initial_severity_key,
        initial_severity_level_code,
        initial_severity_label
),
severity_statistics AS
(
    SELECT
        initial_severity_key,
        initial_severity_level_code,
        initial_severity_label,
        total_incidents,
        valid_response_count,
        total_response_seconds,

        total_response_seconds
            / NULLIF(
                CAST(valid_response_count AS FLOAT),
                0
            ) AS weighted_average_response_seconds,

        pooled_second_moment_total
            / NULLIF(
                CAST(valid_response_count AS FLOAT),
                0
            )
            -
            POWER(
                total_response_seconds
                / NULLIF(
                    CAST(valid_response_count AS FLOAT),
                    0
                ),
                2
            ) AS pooled_variance_seconds

    FROM severity_base
)
SELECT
    initial_severity_key,
    initial_severity_level_code,
    initial_severity_label,
    total_incidents,
    valid_response_count,

    CAST(
        ROUND(
            CAST(valid_response_count AS DECIMAL(20,8))
            /
            NULLIF(
                CAST(total_incidents AS DECIMAL(20,8)),
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS valid_response_percentage,

    CAST(
        ROUND(
            CAST(
                weighted_average_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS weighted_average_response_minutes,

    CAST(
        ROUND(
            CAST(
                SQRT(
                    CASE
                        WHEN pooled_variance_seconds < 0
                            THEN 0
                        ELSE pooled_variance_seconds
                    END
                )
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS pooled_population_stddev_minutes,

    CAST(
        ROUND(
            SQRT(
                CASE
                    WHEN pooled_variance_seconds < 0
                        THEN 0
                    ELSE pooled_variance_seconds
                END
            )
            /
            NULLIF(
                weighted_average_response_seconds,
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS response_cv_percentage

FROM severity_statistics
ORDER BY initial_severity_level_code;


-- ============================================================================
-- BQ06:
-- 02. Response performance by borough
-- ============================================================================

WITH borough_base AS
(
    SELECT
        borough,

        SUM(total_incidents)
            AS total_incidents,

        SUM(valid_incident_response_count)
            AS valid_response_count,

        SUM(total_incident_response_seconds)
            AS total_response_seconds,

        SUM(
            CAST(
                valid_incident_response_count
                AS FLOAT
            )
            *
            (
                POWER(
                    population_stddev_response_seconds,
                    2
                )
                +
                POWER(
                    average_incident_response_seconds,
                    2
                )
            )
        ) AS pooled_second_moment_total

    FROM dbo.gold_agg_daily_performance

    GROUP BY borough
),
borough_statistics AS
(
    SELECT
        borough,
        total_incidents,
        valid_response_count,

        total_response_seconds
            / NULLIF(
                CAST(valid_response_count AS FLOAT),
                0
            ) AS weighted_average_response_seconds,

        pooled_second_moment_total
            / NULLIF(
                CAST(valid_response_count AS FLOAT),
                0
            )
            -
            POWER(
                total_response_seconds
                / NULLIF(
                    CAST(valid_response_count AS FLOAT),
                    0
                ),
                2
            ) AS pooled_variance_seconds

    FROM borough_base
)
SELECT
    borough,
    total_incidents,
    valid_response_count,

    CAST(
        ROUND(
            CAST(valid_response_count AS DECIMAL(20,8))
            /
            NULLIF(
                CAST(total_incidents AS DECIMAL(20,8)),
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS valid_response_percentage,

    CAST(
        ROUND(
            CAST(
                weighted_average_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS weighted_average_response_minutes,

    CAST(
        ROUND(
            CAST(
                SQRT(
                    CASE
                        WHEN pooled_variance_seconds < 0
                            THEN 0
                        ELSE pooled_variance_seconds
                    END
                )
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS pooled_population_stddev_minutes,

    CAST(
        ROUND(
            SQRT(
                CASE
                    WHEN pooled_variance_seconds < 0
                        THEN 0
                    ELSE pooled_variance_seconds
                END
            )
            /
            NULLIF(
                weighted_average_response_seconds,
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS response_cv_percentage,

    RANK() OVER
    (
        ORDER BY weighted_average_response_seconds DESC
    ) AS slowest_response_rank

FROM borough_statistics

ORDER BY
    weighted_average_response_seconds DESC,
    borough;


-- ============================================================================
-- BQ07:
-- 03. Response variability by borough and severity
-- ============================================================================

WITH variability_base AS
(
    SELECT
        borough,
        initial_severity_level_code,
        initial_severity_label,

        SUM(total_incidents)
            AS total_incidents,

        SUM(valid_incident_response_count)
            AS valid_response_count,

        SUM(total_incident_response_seconds)
            AS total_response_seconds,

        SUM(
            CAST(
                valid_incident_response_count
                AS FLOAT
            )
            *
            (
                POWER(
                    population_stddev_response_seconds,
                    2
                )
                +
                POWER(
                    average_incident_response_seconds,
                    2
                )
            )
        ) AS pooled_second_moment_total

    FROM dbo.gold_agg_daily_performance

    GROUP BY
        borough,
        initial_severity_level_code,
        initial_severity_label
),
variability_statistics AS
(
    SELECT
        borough,
        initial_severity_level_code,
        initial_severity_label,
        total_incidents,
        valid_response_count,

        total_response_seconds
            / NULLIF(
                CAST(valid_response_count AS FLOAT),
                0
            ) AS weighted_average_response_seconds,

        pooled_second_moment_total
            / NULLIF(
                CAST(valid_response_count AS FLOAT),
                0
            )
            -
            POWER(
                total_response_seconds
                / NULLIF(
                    CAST(valid_response_count AS FLOAT),
                    0
                ),
                2
            ) AS pooled_variance_seconds

    FROM variability_base
    WHERE valid_response_count >= 1000
),
variability_results AS
(
    SELECT
        borough,
        initial_severity_level_code,
        initial_severity_label,
        total_incidents,
        valid_response_count,
        weighted_average_response_seconds,

        SQRT(
            CASE
                WHEN pooled_variance_seconds < 0
                    THEN 0
                ELSE pooled_variance_seconds
            END
        ) AS pooled_stddev_seconds

    FROM variability_statistics
)
SELECT
    borough,
    initial_severity_level_code,
    initial_severity_label,
    total_incidents,
    valid_response_count,

    CAST(
        ROUND(
            CAST(
                weighted_average_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS weighted_average_response_minutes,

    CAST(
        ROUND(
            CAST(
                pooled_stddev_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS pooled_population_stddev_minutes,

    CAST(
        ROUND(
            pooled_stddev_seconds
            /
            NULLIF(
                weighted_average_response_seconds,
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS response_cv_percentage,

    RANK() OVER
    (
        ORDER BY pooled_stddev_seconds DESC
    ) AS variability_rank

FROM variability_results

ORDER BY
    pooled_stddev_seconds DESC,
    borough,
    initial_severity_level_code;


-- ============================================================================
-- BQ08:
-- 04. Held versus non-held incident response performance
-- ============================================================================

SELECT
    CASE
        WHEN is_held = 1
            THEN 'Held'
        ELSE 'Not Held'
    END AS held_status,

    COUNT_BIG(*) AS total_incidents,

    COUNT_BIG(
        valid_incident_response_seconds
    ) AS valid_response_count,

    CAST(
        ROUND(
            CAST(
                COUNT_BIG(
                    valid_incident_response_seconds
                )
                AS DECIMAL(20,8)
            )
            /
            NULLIF(
                CAST(
                    COUNT_BIG(*)
                    AS DECIMAL(20,8)
                ),
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS valid_response_percentage,

    CAST(
        ROUND(
            CAST(
                AVG(
                    valid_incident_response_seconds
                )
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS average_response_minutes,

    CAST(
        ROUND(
            CAST(
                STDEVP(
                    valid_incident_response_seconds
                )
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS population_stddev_minutes,

    CAST(
        ROUND(
            STDEVP(
                valid_incident_response_seconds
            )
            /
            NULLIF(
                AVG(
                    valid_incident_response_seconds
                ),
                0
            ) * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS response_cv_percentage

FROM dbo.gold_fact_ems_incident

GROUP BY
    CASE
        WHEN is_held = 1
            THEN 'Held'
        ELSE 'Not Held'
    END

ORDER BY held_status;


-- ============================================================================
-- BQ09:
-- 05. Dispatch-delay and travel-time contribution
-- ============================================================================

WITH response_components AS
(
    SELECT
        COUNT_BIG(*) AS valid_component_records,

        AVG(valid_dispatch_response_seconds)
            AS average_dispatch_seconds,

        AVG(valid_travel_time_seconds)
            AS average_travel_seconds,

        AVG(valid_incident_response_seconds)
            AS average_incident_response_seconds

    FROM dbo.gold_fact_ems_incident

    WHERE valid_dispatch_response_seconds IS NOT NULL
      AND valid_travel_time_seconds IS NOT NULL
      AND valid_incident_response_seconds IS NOT NULL
)
SELECT
    valid_component_records,

    CAST(
        ROUND(
            CAST(
                average_dispatch_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS average_dispatch_minutes,

    CAST(
        ROUND(
            CAST(
                average_travel_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS average_travel_minutes,

    CAST(
        ROUND(
            CAST(
                average_incident_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS average_incident_response_minutes,

    CAST(
        ROUND(
            average_dispatch_seconds
            /
            NULLIF(
                average_incident_response_seconds,
                0
            ) * 100,
            6
        )
        AS DECIMAL(12,6)
    ) AS dispatch_share_percentage,

    CAST(
        ROUND(
            average_travel_seconds
            /
            NULLIF(
                average_incident_response_seconds,
                0
            ) * 100,
            6
        )
        AS DECIMAL(12,6)
    ) AS travel_share_percentage,

    CAST(
        ROUND(
            CAST(
                (
                    average_incident_response_seconds
                    -
                    average_dispatch_seconds
                    -
                    average_travel_seconds
                )
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS component_reconciliation_difference_minutes

FROM response_components;

-- Validate the additive response-time relationship

SELECT
    COUNT_BIG(*) AS compared_records,

    SUM(
        CASE
            WHEN ABS(
                    valid_incident_response_seconds
                    -
                    valid_dispatch_response_seconds
                    -
                    valid_travel_time_seconds
                 ) > 0.000001
            THEN 1
            ELSE 0
        END
    ) AS component_mismatch_records,

    MIN(
        valid_incident_response_seconds
        -
        valid_dispatch_response_seconds
        -
        valid_travel_time_seconds
    ) AS minimum_difference_seconds,

    MAX(
        valid_incident_response_seconds
        -
        valid_dispatch_response_seconds
        -
        valid_travel_time_seconds
    ) AS maximum_difference_seconds,

    AVG(
        valid_incident_response_seconds
        -
        valid_dispatch_response_seconds
        -
        valid_travel_time_seconds
    ) AS average_difference_seconds

FROM dbo.gold_fact_ems_incident

WHERE valid_dispatch_response_seconds IS NOT NULL
  AND valid_travel_time_seconds IS NOT NULL
  AND valid_incident_response_seconds IS NOT NULL;


-- ============================================================================
-- BQ10:
-- 06. High-demand and slower-response months
-- ============================================================================

WITH monthly_performance AS
(
    SELECT
        incident_year,
        MONTH(incident_date) AS month_number,
        FORMAT(incident_date, 'yyyy-MM')
            AS year_month,

        SUM(total_incidents)
            AS total_incidents,

        SUM(valid_incident_response_count)
            AS valid_response_count,

        SUM(total_incident_response_seconds)
            /
            NULLIF(
                CAST(
                    SUM(valid_incident_response_count)
                    AS FLOAT
                ),
                0
            ) AS weighted_average_response_seconds

    FROM dbo.gold_agg_daily_performance

    GROUP BY
        incident_year,
        MONTH(incident_date),
        FORMAT(incident_date, 'yyyy-MM')
),
monthly_benchmarks AS
(
    SELECT
        AVG(
            CAST(total_incidents AS FLOAT)
        ) AS average_monthly_incidents,

        AVG(
            weighted_average_response_seconds
        ) AS average_monthly_response_seconds

    FROM monthly_performance
)
SELECT
    mp.incident_year,
    mp.month_number,
    mp.year_month,
    mp.total_incidents,
    mp.valid_response_count,

    CAST(
        ROUND(
            CAST(
                mp.weighted_average_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS weighted_average_response_minutes,

    CAST(
        ROUND(
            mb.average_monthly_incidents,
            2
        )
        AS DECIMAL(14,2)
    ) AS monthly_incident_benchmark,

    CAST(
        ROUND(
            CAST(
                mb.average_monthly_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS response_time_benchmark_minutes,

    CASE
        WHEN mp.total_incidents
                > mb.average_monthly_incidents
         AND mp.weighted_average_response_seconds
                > mb.average_monthly_response_seconds
        THEN 1
        ELSE 0
    END AS is_high_demand_slow_response_month

FROM monthly_performance AS mp
CROSS JOIN monthly_benchmarks AS mb

ORDER BY
    is_high_demand_slow_response_month DESC,
    mp.year_month;


-- ============================================================================
-- BQ10:
-- 07. High-demand and slower-response hours
-- ============================================================================

WITH hourly_performance AS
(
    SELECT
        t.hour_number,
        t.hour_label,

        COUNT_BIG(*) AS total_incidents,

        COUNT_BIG(
            f.valid_incident_response_seconds
        ) AS valid_response_count,

        AVG(
            f.valid_incident_response_seconds
        ) AS average_response_seconds

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_time AS t
        ON f.time_key = t.time_key

    GROUP BY
        t.hour_number,
        t.hour_label
),
hourly_benchmarks AS
(
    SELECT
        AVG(
            CAST(total_incidents AS FLOAT)
        ) AS average_hourly_incidents,

        AVG(
            average_response_seconds
        ) AS average_hourly_response_seconds

    FROM hourly_performance
)
SELECT
    hp.hour_number,
    hp.hour_label,
    hp.total_incidents,
    hp.valid_response_count,

    CAST(
        ROUND(
            CAST(
                hp.average_response_seconds
                AS DECIMAL(24,6)
            ) / 60,
            2
        )
        AS DECIMAL(12,2)
    ) AS average_response_minutes,

    CASE
        WHEN hp.total_incidents
                > hb.average_hourly_incidents
         AND hp.average_response_seconds
                > hb.average_hourly_response_seconds
        THEN 1
        ELSE 0
    END AS is_high_demand_slow_response_hour,

    RANK() OVER
    (
        ORDER BY
            hp.total_incidents DESC,
            hp.average_response_seconds DESC
    ) AS combined_demand_rank

FROM hourly_performance AS hp
CROSS JOIN hourly_benchmarks AS hb

ORDER BY hp.hour_number;


-- ============================================================================
-- BQ10:
-- 08. Reportable response anomalies by borough and severity
-- ============================================================================

SELECT
    borough,
    initial_severity_level_code,
    initial_severity_label,

    COUNT_BIG(*) AS eligible_daily_groups,

    SUM(
        CASE
            WHEN is_reportable_response_anomaly = 1
                THEN 1
            ELSE 0
        END
    ) AS reportable_anomaly_days,

    SUM(
        CASE
            WHEN response_anomaly_direction = 'Above UCL'
                THEN 1
            ELSE 0
        END
    ) AS above_ucl_days,

    SUM(
        CASE
            WHEN response_anomaly_direction = 'Below LCL'
                THEN 1
            ELSE 0
        END
    ) AS below_lcl_days,

    CAST(
        ROUND(
            CAST(
                SUM(
                    CASE
                        WHEN is_reportable_response_anomaly = 1
                            THEN 1
                        ELSE 0
                    END
                )
                AS DECIMAL(20,8)
            )
            /
            NULLIF(
                CAST(
                    COUNT_BIG(*)
                    AS DECIMAL(20,8)
                ),
                0
            ) * 100,
            6
        )
        AS DECIMAL(12,6)
    ) AS anomaly_rate_percentage

FROM dbo.gold_agg_daily_performance

WHERE has_sufficient_sample = 1
  AND has_sufficient_baseline = 1

GROUP BY
    borough,
    initial_severity_level_code,
    initial_severity_label

ORDER BY
    reportable_anomaly_days DESC,
    borough,
    initial_severity_level_code;