/*
===============================================================================
NYC EMS Analytics
Incident Demand Analysis

File:
    sql/02_incident_demand_analysis.sql

Purpose:
    Analyse EMS incident demand across time, geography and call type.

Business questions:
    BQ01. How has EMS incident volume changed over time from 2019 to 2025?
    BQ02. At what times of day and days of the week is EMS demand highest?
    BQ03. Which NYC boroughs experience the highest EMS incident volumes?
    BQ04. What are the most common EMS call types?

Data source:
    gold_fact_ems_incident
    gold_dim_date
    gold_dim_time
    gold_dim_geography
    gold_dim_call_type
===============================================================================
*/

-- ============================================================================
-- BQ01:
-- 01. Annual EMS incident trend
-- Purpose:
--     Measure annual incident volume and year-over-year change.
-- ============================================================================

WITH annual_incidents AS
(
    SELECT
        d.year AS incident_year,
        SUM(f.incident_count) AS total_incidents
    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_date AS d
        ON f.date_key = d.date_key

    GROUP BY
        d.year
),
annual_with_previous_year AS
(
    SELECT
        incident_year,
        total_incidents,

        LAG(total_incidents) OVER
        (
            ORDER BY incident_year
        ) AS previous_year_incidents

    FROM annual_incidents
)
SELECT
    incident_year,
    total_incidents,
    previous_year_incidents,

    total_incidents
        - previous_year_incidents
        AS year_over_year_change,

    CAST(
        ROUND(
            (
                CAST(
                    total_incidents
                    AS DECIMAL(20,4)
                )
                -
                CAST(
                    previous_year_incidents
                    AS DECIMAL(20,4)
                )
            )
            /
            NULLIF(
                CAST(
                    previous_year_incidents
                    AS DECIMAL(20,4)
                ),
                0
            ) * 100,
            6
        ) AS DECIMAL(20,6)
    ) AS year_over_year_change_percentage

FROM annual_with_previous_year
ORDER BY incident_year;


-- ============================================================================
-- BQ01:
-- 02. Monthly EMS incident trend
-- Purpose:
--     Analyse monthly incident volume from 2019 to 2025.
-- ============================================================================

WITH monthly_incidents AS
(
    SELECT
        d.year,
        d.month_number,
        d.month_name,
        d.year_month,
        d.year_month_label,
        d.year_month_sort,

        SUM(f.incident_count)
            AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_date AS d
        ON f.date_key = d.date_key

    GROUP BY
        d.year,
        d.month_number,
        d.month_name,
        d.year_month,
        d.year_month_label,
        d.year_month_sort
),
monthly_with_previous_month AS
(
    SELECT
        year,
        month_number,
        month_name,
        year_month,
        year_month_label,
        year_month_sort,
        total_incidents,

        LAG(total_incidents) OVER
        (
            ORDER BY year_month_sort
        ) AS previous_month_incidents

    FROM monthly_incidents
)
SELECT
    year,
    month_number,
    month_name,
    year_month,
    year_month_label,
    year_month_sort,
    total_incidents,
    previous_month_incidents,

    total_incidents
        - previous_month_incidents
        AS month_over_month_change,

    CAST(
        ROUND(
            (
                CAST(
                    total_incidents
                    AS DECIMAL(20,4)
                )
                -
                CAST(
                    previous_month_incidents
                    AS DECIMAL(20,4)
                )
            )
            /
            NULLIF(
                CAST(
                    previous_month_incidents
                    AS DECIMAL(20,4)
                ),
                0
            ) * 100,
            6
        ) AS DECIMAL(20,6)
    ) AS month_over_month_change_percentage

FROM monthly_with_previous_month
ORDER BY year_month_sort;


-- ============================================================================
-- BQ02:
-- 03. EMS demand by day of week
-- Purpose:
--     Identify which days of the week have the highest average demand.
-- ============================================================================

WITH daily_incidents AS
(
    SELECT
        d.full_date,
        d.day_of_week_number,
        d.day_of_week_name,
        d.day_of_week_short_name,
        d.is_weekend,

        SUM(f.incident_count)
            AS daily_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_date AS d
        ON f.date_key = d.date_key

    GROUP BY
        d.full_date,
        d.day_of_week_number,
        d.day_of_week_name,
        d.day_of_week_short_name,
        d.is_weekend
)
SELECT
    day_of_week_number,
    day_of_week_name,
    day_of_week_short_name,
    is_weekend,

    COUNT_BIG(*) AS days_observed,

    SUM(daily_incidents)
        AS total_incidents,

    CAST(
        ROUND(
            AVG(
                CAST(
                    daily_incidents
                    AS DECIMAL(20,4)
                )
            ),
            2
        ) AS DECIMAL(20,2)
    ) AS average_daily_incidents

FROM daily_incidents

GROUP BY
    day_of_week_number,
    day_of_week_name,
    day_of_week_short_name,
    is_weekend

ORDER BY day_of_week_number;


-- ============================================================================
-- BQ02:
-- 04. EMS demand by hour of day
-- Purpose:
--     Identify the hours with the highest incident demand.
-- ============================================================================

WITH hourly_incidents AS
(
    SELECT
        t.hour_number,
        t.hour_label,

        SUM(f.incident_count)
            AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_time AS t
        ON f.time_key = t.time_key

    GROUP BY
        t.hour_number,
        t.hour_label
)
SELECT
    hour_number,
    hour_label,
    total_incidents,

    CAST(
        ROUND(
            CAST(
                total_incidents
                AS DECIMAL(20,4)
            )
            /
            SUM(total_incidents) OVER ()
            * 100,
            6
        ) AS DECIMAL(20,6)
    ) AS incident_percentage,

    RANK() OVER
    (
        ORDER BY total_incidents DESC
    ) AS demand_rank

FROM hourly_incidents
ORDER BY hour_number;


-- ============================================================================
-- BQ02:
-- 05. EMS demand by time of day
-- Purpose:
--     Compare broader operational time periods.
-- ============================================================================

WITH time_period_incidents AS
(
    SELECT
        t.time_of_day,
        t.time_of_day_sort,

        SUM(f.incident_count)
            AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_time AS t
        ON f.time_key = t.time_key

    GROUP BY
        t.time_of_day,
        t.time_of_day_sort
)
SELECT
    time_of_day,
    time_of_day_sort,
    total_incidents,

    CAST(
        ROUND(
            CAST(
                total_incidents
                AS DECIMAL(20,4)
            )
            /
            SUM(total_incidents) OVER ()
            * 100,
            2
        ) AS DECIMAL(20,2)
    ) AS incident_percentage,

    RANK() OVER
    (
        ORDER BY total_incidents DESC
    ) AS demand_rank

FROM time_period_incidents
ORDER BY time_of_day_sort;


-- ============================================================================
-- BQ03:
-- 06. EMS incident volume by borough
-- Purpose:
--     Identify boroughs with the highest EMS incident demand.
-- ============================================================================

WITH borough_incidents AS
(
    SELECT
        g.borough,

        SUM(f.incident_count)
            AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_geography AS g
        ON f.geography_key = g.geography_key

    GROUP BY
        g.borough
)
SELECT
    borough,
    total_incidents,

    CAST(
        ROUND(
            CAST(
                total_incidents
                AS DECIMAL(20,4)
            )
            /
            SUM(total_incidents) OVER ()
            * 100,
            6
        )
        AS DECIMAL(20,6)
    )AS incident_percentage,

    RANK() OVER
    (
        ORDER BY total_incidents DESC
    ) AS incident_volume_rank

FROM borough_incidents
ORDER BY
    total_incidents DESC,
    borough;


-- ============================================================================
-- BQ04:
-- 07. Most common initial EMS call types
-- Purpose:
--     Rank initial dispatch classifications by incident volume.
-- Note:
--     Call types are dispatch classifications and should not be interpreted
--     as confirmed clinical diagnoses.
-- ============================================================================

WITH initial_call_type_incidents AS
(
    SELECT
        ct.call_type,
        ct.call_type_description,

        SUM(f.incident_count)
            AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_call_type AS ct
        ON f.initial_call_type_key = ct.call_type_key

    GROUP BY
        ct.call_type,
        ct.call_type_description
)
SELECT
    call_type,
    call_type_description,
    total_incidents,

    CAST(
        ROUND(
            CAST(
                total_incidents
                AS DECIMAL(20,8)
            )
            /
            NULLIF(
                CAST(
                    SUM(total_incidents) OVER ()
                    AS DECIMAL(20,8)
                ),
                0
            )
            * 100,
            6
        )
        AS DECIMAL(20,6)
    ) AS incident_percentage,

    RANK() OVER
    (
        ORDER BY total_incidents DESC
    ) AS call_type_rank

FROM initial_call_type_incidents
ORDER BY
    total_incidents DESC,
    call_type;


-- ============================================================================
-- BQ04:
-- 08. Initial versus final EMS call-type volume
-- Purpose:
--     Compare how frequently each call type appears as an initial and final
--     dispatch classification.
-- ============================================================================

WITH initial_call_types AS
(
    SELECT
        ct.call_type,
        ct.call_type_description,

        SUM(f.incident_count)
            AS initial_incident_count

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_call_type AS ct
        ON f.initial_call_type_key = ct.call_type_key

    GROUP BY
        ct.call_type,
        ct.call_type_description
),
final_call_types AS
(
    SELECT
        ct.call_type,
        ct.call_type_description,

        SUM(f.incident_count)
            AS final_incident_count

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_call_type AS ct
        ON f.final_call_type_key = ct.call_type_key

    GROUP BY
        ct.call_type,
        ct.call_type_description
)
SELECT
    COALESCE(
        initial_ct.call_type,
        final_ct.call_type
    ) AS call_type,

    COALESCE(
        initial_ct.call_type_description,
        final_ct.call_type_description
    ) AS call_type_description,

    COALESCE(
        initial_ct.initial_incident_count,
        0
    ) AS initial_incident_count,

    COALESCE(
        final_ct.final_incident_count,
        0
    ) AS final_incident_count,

    COALESCE(
        final_ct.final_incident_count,
        0
    )
    -
    COALESCE(
        initial_ct.initial_incident_count,
        0
    ) AS final_minus_initial_difference

FROM initial_call_types AS initial_ct

FULL OUTER JOIN final_call_types AS final_ct
    ON initial_ct.call_type = final_ct.call_type

ORDER BY
    initial_incident_count DESC,
    call_type;