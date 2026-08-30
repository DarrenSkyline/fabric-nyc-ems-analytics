/*
===============================================================================
 File:
     04_outcomes_classification_analysis.sql

 Project:
     fabric-nyc-ems-analytics

 Purpose:
     Analyze EMS incident dispositions, call-type reclassification,
     severity changes, and operational outcome flags.

 Main analytical areas:
     1. Final disposition distribution
     2. Documented, undocumented, and unknown disposition records
     3. Initial-to-final call-type changes
     4. Initial-to-final severity changes
     5. Severity escalation and de-escalation
     6. Held, reopened, standby, transfer, and special-event incidents
     7. Response-time differences across classifications and outcomes

 Primary tables:
     dbo.gold_fact_ems_incident
     dbo.gold_dim_date
     dbo.gold_dim_geography
     dbo.gold_dim_call_type
     dbo.gold_dim_severity
     dbo.gold_dim_disposition

 Dimension fields used:

     gold_dim_disposition:
         disposition_key
         disposition_code
         disposition_description
         disposition_category
         disposition_category_sort
         mapping_status
         is_documented_code
         is_unknown

     gold_dim_severity:
         severity_key
         severity_level_code
         severity_label
         appears_as_initial
         appears_as_final

 Important interpretation notes:

     - disposition_category provides the standardized analytical grouping.

     - disposition_code and disposition_description preserve the original
       operational disposition detail.

     - Undocumented disposition codes are retained as a separate category
       and must not be silently grouped with unknown dispositions.

     - Unknown / No Disposition represents records where no meaningful
       disposition outcome was available.

     - Final disposition represents the recorded EMS operational outcome,
       not the patient's final clinical or hospital outcome.

     - Call types are dispatch classifications and are not confirmed
       clinical diagnoses.

     - Call-type changes do not necessarily indicate that the initial
       classification was incorrect.

     - Severity-change direction must be determined by comparing
       severity_level_code values. Surrogate severity_key values must
       never be used to determine escalation or de-escalation.

     - Analytical results are descriptive and do not establish causation.

 SQL environment:
     Microsoft Fabric Lakehouse SQL Analytics Endpoint
===============================================================================
*/


-- =============================================================================
-- CQ01:
-- 01. Final-disposition category distribution
--
-- Business Question:
--     BQ11. What are the most common EMS incident dispositions, 
--     including patient transport and non-transport outcomes?
--
-- Purpose:
--     Measure incident volume and percentage by standardized final-disposition
--     category.
--
-- Interpretation:
--     Final disposition represents the recorded EMS operational outcome,
--     not the patient's final clinical or hospital outcome.
-- =============================================================================

WITH disposition_category_counts AS
(
    SELECT
        dd.disposition_category,
        dd.disposition_category_sort,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_disposition AS dd
        ON f.disposition_key = dd.disposition_key

    GROUP BY
        dd.disposition_category,
        dd.disposition_category_sort
),
all_incident_count AS
(
    SELECT
        SUM(total_incidents) AS all_incidents

    FROM disposition_category_counts
)
SELECT
    dcc.disposition_category,
    dcc.total_incidents,

    CAST(
        CAST(dcc.total_incidents AS DECIMAL(38, 10))
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(aic.all_incidents AS DECIMAL(38, 10)),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS incident_percentage

FROM disposition_category_counts AS dcc

CROSS JOIN all_incident_count AS aic

ORDER BY
    dcc.disposition_category_sort,
    dcc.disposition_category;


-- =============================================================================
-- CQ02:
-- 02. Disposition-code detail and mapping quality
--
-- Business Question:
--     BQ11. What are the most common EMS incident dispositions, 
--     including patient transport and non-transport outcomes?
--
-- Purpose:
--     Examine incident volume and percentage at the original disposition-code
--     level, while showing how each code was mapped into a standardized
--     disposition category.
--
--     This query also identifies documented, undocumented and unknown
--     disposition records.
--
-- Interpretation:
--     Undocumented disposition codes are present in the source data but do not
--     have a confirmed description in the available documentation.
--
--     Unknown / No Disposition means that no meaningful final disposition was
--     available. It must not be combined with undocumented codes.
-- =============================================================================

WITH disposition_code_counts AS
(
    SELECT
        dd.disposition_code,
        dd.disposition_description,
        dd.disposition_category,
        dd.disposition_category_sort,
        dd.mapping_status,
        dd.is_documented_code,
        dd.is_unknown,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_disposition AS dd
        ON f.disposition_key = dd.disposition_key

    GROUP BY
        dd.disposition_code,
        dd.disposition_description,
        dd.disposition_category,
        dd.disposition_category_sort,
        dd.mapping_status,
        dd.is_documented_code,
        dd.is_unknown
),
all_incident_count AS
(
    SELECT
        SUM(total_incidents) AS all_incidents

    FROM disposition_code_counts
)
SELECT
    dcc.disposition_code,
    dcc.disposition_description,
    dcc.disposition_category,
    dcc.mapping_status,
    dcc.is_documented_code,
    dcc.is_unknown,
    dcc.total_incidents,

    CAST(
        CAST(dcc.total_incidents AS DECIMAL(38, 10))
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(aic.all_incidents AS DECIMAL(38, 10)),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS incident_percentage

FROM disposition_code_counts AS dcc

CROSS JOIN all_incident_count AS aic

ORDER BY
    dcc.disposition_category_sort,
    dcc.total_incidents DESC,
    dcc.disposition_code;


-- =============================================================================
-- CQ03:
-- 03. Final-disposition distribution within each borough
--
-- Business Question:
--     BQ11. What are the most common EMS incident dispositions, 
--     including patient transport and non-transport outcomes?
--
-- Purpose:
--     Measure incident volume and percentage by borough and standardized
--     final-disposition category.
--
--     The percentage is calculated within each borough, not against the
--     citywide incident total.
--
-- Interpretation:
--     Differences between boroughs are descriptive. They may be influenced by
--     incident severity, call-type mix, population, service demand and other
--     operational factors.
-- =============================================================================

WITH borough_disposition_counts AS
(
    SELECT
        dg.borough,
        dd.disposition_category,
        dd.disposition_category_sort,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_geography AS dg
        ON f.geography_key = dg.geography_key

    INNER JOIN dbo.gold_dim_disposition AS dd
        ON f.disposition_key = dd.disposition_key

    GROUP BY
        dg.borough,
        dd.disposition_category,
        dd.disposition_category_sort
),
borough_incident_totals AS
(
    SELECT
        borough,

        SUM(total_incidents) AS borough_incidents

    FROM borough_disposition_counts

    GROUP BY
        borough
)
SELECT
    bdc.borough,
    bdc.disposition_category,
    bdc.total_incidents,
    bit.borough_incidents,

    CAST(
        CAST(bdc.total_incidents AS DECIMAL(38, 10))
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(bit.borough_incidents AS DECIMAL(38, 10)),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS borough_percentage

FROM borough_disposition_counts AS bdc

INNER JOIN borough_incident_totals AS bit
    ON bdc.borough = bit.borough

ORDER BY
    bdc.borough,
    bdc.disposition_category_sort,
    bdc.disposition_category;


-- =============================================================================
-- CQ04:
-- 04. Call-type change rate by initial call type
--
-- Business Question:
--     BQ12. How frequently do call type and severity classifications change 
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Calculate the number and percentage of incidents whose call type changed,
--     grouped by the initial call type.
--
--     Only initial call types with at least 1,000 incidents are included in
--     the ranking to reduce the effect of very small sample sizes.
--
-- Interpretation:
--     A call-type change does not necessarily mean that the initial dispatch
--     classification was incorrect. It may reflect additional information
--     obtained during call handling or incident response.
-- =============================================================================

WITH initial_call_type_change_counts AS
(
    SELECT
        ict.call_type AS initial_call_type,
        ict.call_type_description
            AS initial_call_type_description,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents,

        SUM(
            CASE
                WHEN f.call_type_changed = 1
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS changed_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_call_type AS ict
        ON f.initial_call_type_key = ict.call_type_key

    GROUP BY
        ict.call_type,
        ict.call_type_description
),
initial_call_type_change_rates AS
(
    SELECT
        initial_call_type,
        initial_call_type_description,
        total_incidents,
        changed_incidents,

        total_incidents
            - changed_incidents
            AS unchanged_incidents,

        CAST(
            CAST(changed_incidents AS DECIMAL(38, 10))
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(total_incidents AS DECIMAL(38, 10)),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS call_type_change_percentage

    FROM initial_call_type_change_counts

    WHERE total_incidents >= 1000
)
SELECT
    initial_call_type,
    total_incidents,
    changed_incidents,
    unchanged_incidents,
    call_type_change_percentage,

    RANK() OVER
    (
        ORDER BY call_type_change_percentage DESC
    ) AS change_rate_rank

FROM initial_call_type_change_rates

ORDER BY
    change_rate_rank,
    total_incidents DESC,
    initial_call_type;


-- =============================================================================
-- CQ05:
-- 05. Most common initial-to-final call-type transitions
--
-- Business Question:
--     BQ12. How frequently do call type and severity classifications change 
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Identify the most common transitions from initial call type to final
--     call type among incidents whose call type changed.
--
--     The query calculates:
--     - Transition incident count
--     - Percentage within the initial call type's changed incidents
--     - Percentage of all call-type-changed incidents
--     - Transition rank within each initial call type
--
-- Interpretation:
--     A call-type transition does not necessarily indicate an incorrect
--     initial classification. It may reflect additional information obtained
--     during call handling or incident response.
--
--     Call types are dispatch classifications and must not be interpreted as
--     confirmed clinical diagnoses.
-- =============================================================================

WITH call_type_transition_counts AS
(
    SELECT
        ict.call_type AS initial_call_type,
        ict.call_type_description
            AS initial_call_type_description,

        fct.call_type AS final_call_type,
        fct.call_type_description
            AS final_call_type_description,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS transition_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_call_type AS ict
        ON f.initial_call_type_key
         = ict.call_type_key

    INNER JOIN dbo.gold_dim_call_type AS fct
        ON f.final_call_type_key
         = fct.call_type_key

    WHERE f.call_type_changed = 1

    GROUP BY
        ict.call_type,
        ict.call_type_description,
        fct.call_type,
        fct.call_type_description
),
initial_call_type_changed_totals AS
(
    SELECT
        initial_call_type,

        SUM(
            transition_incidents
        ) AS initial_changed_incidents

    FROM call_type_transition_counts

    GROUP BY
        initial_call_type
),
all_changed_incident_count AS
(
    SELECT
        SUM(
            transition_incidents
        ) AS all_changed_incidents

    FROM call_type_transition_counts
),
call_type_transition_statistics AS
(
    SELECT
        cttc.initial_call_type,
        cttc.initial_call_type_description,
        cttc.final_call_type,
        cttc.final_call_type_description,
        cttc.transition_incidents,
        ictt.initial_changed_incidents,

        CAST(
            CAST(
                cttc.transition_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    ictt.initial_changed_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS percentage_within_initial_changes,

        CAST(
            CAST(
                cttc.transition_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    acic.all_changed_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS percentage_of_all_changes,

        RANK() OVER
        (
            PARTITION BY
                cttc.initial_call_type

            ORDER BY
                cttc.transition_incidents DESC
        ) AS transition_rank_within_initial

    FROM call_type_transition_counts AS cttc

    INNER JOIN initial_call_type_changed_totals AS ictt
        ON cttc.initial_call_type
         = ictt.initial_call_type

    CROSS JOIN all_changed_incident_count AS acic
)
SELECT TOP (100)
    initial_call_type,
    initial_call_type_description,
    final_call_type,
    final_call_type_description,
    transition_incidents,
    initial_changed_incidents,
    percentage_within_initial_changes,
    percentage_of_all_changes,
    transition_rank_within_initial

FROM call_type_transition_statistics

ORDER BY
    transition_incidents DESC,
    initial_call_type,
    final_call_type;


-- =============================================================================
-- CQ06:
-- 06. Severity-change rate by initial severity
--
-- Business Question:
--     BQ12. How frequently do call type and severity classifications change
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Calculate the number and percentage of incidents whose severity
--     classification changed, grouped by the initial severity level.
--
--     Identify which initial severity levels have the highest classification-
--     change rates.
--
-- Interpretation:
--     A severity change means that the initial and final severity-level codes
--     are different.
--
--     This query does not classify a change as escalation or de-escalation.
--     The direction of a severity change must not be inferred solely from the
--     numeric severity-level codes without documented NYC EMS priority rules.
-- =============================================================================

WITH initial_severity_change_counts AS
(
    SELECT
        ise.severity_level_code
            AS initial_severity_level_code,

        ise.severity_label
            AS initial_severity_label,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents,

        SUM(
            CASE
                WHEN f.severity_changed = 1
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS changed_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_severity AS ise
        ON f.initial_severity_key
         = ise.severity_key

    GROUP BY
        ise.severity_level_code,
        ise.severity_label
),
initial_severity_change_statistics AS
(
    SELECT
        initial_severity_level_code,
        initial_severity_label,
        total_incidents,
        changed_incidents,

        total_incidents
            - changed_incidents
            AS unchanged_incidents,

        CAST(
            CAST(
                changed_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    total_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS severity_change_percentage

    FROM initial_severity_change_counts
)
SELECT
    initial_severity_level_code,
    initial_severity_label,
    total_incidents,
    changed_incidents,
    unchanged_incidents,
    severity_change_percentage,

    RANK() OVER
    (
        ORDER BY
            severity_change_percentage DESC
    ) AS change_rate_rank

FROM initial_severity_change_statistics

ORDER BY
    change_rate_rank,
    initial_severity_level_code;


-- =============================================================================
-- CQ07:
-- 07. Initial-to-final severity transition matrix
--
-- Business Question:
--     BQ12. How frequently do call type and severity classifications change
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Identify the specific initial-to-final severity transition paths among
--     incidents whose severity classification changed.
--
--     The query calculates:
--     - Incident count for each severity transition
--     - Percentage within the initial severity's changed incidents
--     - Percentage of all severity-changed incidents
--     - Transition rank within each initial severity level
--
-- Interpretation:
--     This query reports the direction of movement between severity-level
--     codes, but does not label the movement as escalation or de-escalation.
--
--     The operational meaning and priority order of the numeric severity codes
--     must not be inferred without documented NYC EMS severity definitions.
-- =============================================================================

WITH severity_transition_counts AS
(
    SELECT
        ise.severity_level_code
            AS initial_severity_level_code,

        ise.severity_label
            AS initial_severity_label,

        fse.severity_level_code
            AS final_severity_level_code,

        fse.severity_label
            AS final_severity_label,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS transition_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_severity AS ise
        ON f.initial_severity_key
         = ise.severity_key

    INNER JOIN dbo.gold_dim_severity AS fse
        ON f.final_severity_key
         = fse.severity_key

    WHERE f.severity_changed = 1

    GROUP BY
        ise.severity_level_code,
        ise.severity_label,
        fse.severity_level_code,
        fse.severity_label
),
initial_severity_changed_totals AS
(
    SELECT
        initial_severity_level_code,

        SUM(
            transition_incidents
        ) AS initial_changed_incidents

    FROM severity_transition_counts

    GROUP BY
        initial_severity_level_code
),
all_severity_changed_count AS
(
    SELECT
        SUM(
            transition_incidents
        ) AS all_changed_incidents

    FROM severity_transition_counts
),
severity_transition_statistics AS
(
    SELECT
        stc.initial_severity_level_code,
        stc.initial_severity_label,
        stc.final_severity_level_code,
        stc.final_severity_label,
        stc.transition_incidents,
        isct.initial_changed_incidents,

        CAST(
            CAST(
                stc.transition_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    isct.initial_changed_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS percentage_within_initial_changes,

        CAST(
            CAST(
                stc.transition_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    ascc.all_changed_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS percentage_of_all_changes,

        RANK() OVER
        (
            PARTITION BY
                stc.initial_severity_level_code

            ORDER BY
                stc.transition_incidents DESC
        ) AS transition_rank_within_initial

    FROM severity_transition_counts AS stc

    INNER JOIN initial_severity_changed_totals AS isct
        ON stc.initial_severity_level_code
         = isct.initial_severity_level_code

    CROSS JOIN all_severity_changed_count AS ascc
)
SELECT
    initial_severity_level_code,
    initial_severity_label,
    final_severity_level_code,
    final_severity_label,
    transition_incidents,
    initial_changed_incidents,
    percentage_within_initial_changes,
    percentage_of_all_changes,
    transition_rank_within_initial

FROM severity_transition_statistics

ORDER BY
    initial_severity_level_code,
    transition_rank_within_initial,
    final_severity_level_code;


-- =============================================================================
-- CQ08:
-- 08. Overlap between call-type and severity classification changes
--
-- Business Question:
--     BQ12. How frequently do call type and severity classifications change
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Divide incidents into four classification-change groups:
--
--     1. Both call type and severity changed
--     2. Only call type changed
--     3. Only severity changed
--     4. Neither classification changed
--
--     The query measures each group's incident volume, percentage of all
--     incidents, and share of incidents with at least one classification
--     change.
--
-- Interpretation:
--     Classification changes may reflect additional information obtained
--     during call handling or incident response. They do not necessarily
--     indicate that the initial classification was incorrect.
-- =============================================================================

WITH classification_change_groups AS
(
    SELECT
        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 'Unknown change status'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 'Both call type and severity changed'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 'Call type changed only'

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 'Severity changed only'

            ELSE 'Neither classification changed'
        END AS classification_change_group,

        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 5

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 1

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 2

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 3

            ELSE 4
        END AS classification_change_sort,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    GROUP BY
        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 'Unknown change status'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 'Both call type and severity changed'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 'Call type changed only'

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 'Severity changed only'

            ELSE 'Neither classification changed'
        END,

        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 5

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 1

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 2

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 3

            ELSE 4
        END
),
classification_change_totals AS
(
    SELECT
        SUM(
            total_incidents
        ) AS all_incidents,

        SUM(
            CASE
                WHEN classification_change_sort
                     IN (1, 2, 3)
                THEN total_incidents
                ELSE CAST(0 AS BIGINT)
            END
        ) AS incidents_with_any_change

    FROM classification_change_groups
)
SELECT
    ccg.classification_change_group,
    ccg.total_incidents,

    CAST(
        CAST(
            ccg.total_incidents
            AS DECIMAL(38, 10)
        )
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(
                cct.all_incidents
                AS DECIMAL(38, 10)
            ),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS percentage_of_all_incidents,

    CASE
        WHEN ccg.classification_change_sort
             IN (1, 2, 3)
        THEN
            CAST(
                CAST(
                    ccg.total_incidents
                    AS DECIMAL(38, 10)
                )
                * CAST(100 AS DECIMAL(38, 10))
                /
                NULLIF(
                    CAST(
                        cct.incidents_with_any_change
                        AS DECIMAL(38, 10)
                    ),
                    CAST(0 AS DECIMAL(38, 10))
                )
                AS DECIMAL(18, 6)
            )
        ELSE NULL
    END AS percentage_of_changed_incidents

FROM classification_change_groups AS ccg

CROSS JOIN classification_change_totals AS cct

ORDER BY
    ccg.classification_change_sort;


-- =============================================================================
-- CQ09:
-- 09. Annual trend in classification-change rates
--
-- Business Question:
--     BQ12. How frequently do call type and severity classifications change
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Measure annual incident volume and annual classification-change rates
--     from 2019 to 2025.
--
--     The query separately calculates:
--     - Call-type changes
--     - Severity changes
--     - Incidents where both classifications changed
--     - Incidents where at least one classification changed
--
-- Interpretation:
--     Changes over time may reflect operational processes, documentation
--     practices, dispatch protocols, incident mix or source-data changes.
--     They should not automatically be interpreted as improvements or errors.
-- =============================================================================

WITH annual_classification_change_counts AS
(
    SELECT
        f.incident_year,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents,

        SUM(
            CASE
                WHEN f.call_type_changed = 1
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS call_type_changed_incidents,

        SUM(
            CASE
                WHEN f.severity_changed = 1
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS severity_changed_incidents,

        SUM(
            CASE
                WHEN f.call_type_changed = 1
                 AND f.severity_changed = 1
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS both_changed_incidents,

        SUM(
            CASE
                WHEN f.call_type_changed = 1
                  OR f.severity_changed = 1
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS any_classification_changed_incidents,

        SUM(
            CASE
                WHEN f.call_type_changed IS NULL
                  OR f.severity_changed IS NULL
                THEN CAST(f.incident_count AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END
        ) AS unknown_change_status_incidents

    FROM dbo.gold_fact_ems_incident AS f

    GROUP BY
        f.incident_year
)
SELECT
    incident_year,
    total_incidents,
    call_type_changed_incidents,
    severity_changed_incidents,
    both_changed_incidents,
    any_classification_changed_incidents,
    unknown_change_status_incidents,

    CAST(
        CAST(
            call_type_changed_incidents
            AS DECIMAL(38, 10)
        )
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(
                total_incidents
                AS DECIMAL(38, 10)
            ),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS call_type_change_percentage,

    CAST(
        CAST(
            severity_changed_incidents
            AS DECIMAL(38, 10)
        )
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(
                total_incidents
                AS DECIMAL(38, 10)
            ),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS severity_change_percentage,

    CAST(
        CAST(
            both_changed_incidents
            AS DECIMAL(38, 10)
        )
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(
                total_incidents
                AS DECIMAL(38, 10)
            ),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS both_changed_percentage,

    CAST(
        CAST(
            any_classification_changed_incidents
            AS DECIMAL(38, 10)
        )
        * CAST(100 AS DECIMAL(38, 10))
        /
        NULLIF(
            CAST(
                total_incidents
                AS DECIMAL(38, 10)
            ),
            CAST(0 AS DECIMAL(38, 10))
        )
        AS DECIMAL(18, 6)
    ) AS any_classification_change_percentage

FROM annual_classification_change_counts

ORDER BY
    incident_year;


-- =============================================================================
-- CQ10:
-- 10. Final-disposition distribution by initial severity
--
-- Business Question:
--     BQ11. What are the most common EMS incident dispositions, including
--     patient transport and non-transport outcomes?
--
-- Purpose:
--     Compare the standardized final-disposition distribution across initial
--     severity levels.
--
--     The query calculates:
--     - Incident volume for each severity/disposition combination
--     - Percentage within each initial severity level
--     - Disposition rank within each initial severity level
--
-- Interpretation:
--     Final disposition represents the recorded EMS operational outcome,
--     not the patient's final clinical or hospital outcome.
--
--     Differences between severity levels are descriptive and should not be
--     interpreted as proof that initial severity caused a particular outcome.
-- =============================================================================

WITH severity_disposition_counts AS
(
    SELECT
        ise.severity_level_code
            AS initial_severity_level_code,

        ise.severity_label
            AS initial_severity_label,

        dd.disposition_category,
        dd.disposition_category_sort,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_severity AS ise
        ON f.initial_severity_key
         = ise.severity_key

    INNER JOIN dbo.gold_dim_disposition AS dd
        ON f.disposition_key
         = dd.disposition_key

    GROUP BY
        ise.severity_level_code,
        ise.severity_label,
        dd.disposition_category,
        dd.disposition_category_sort
),
initial_severity_totals AS
(
    SELECT
        initial_severity_level_code,

        SUM(
            total_incidents
        ) AS initial_severity_incidents

    FROM severity_disposition_counts

    GROUP BY
        initial_severity_level_code
),
severity_disposition_statistics AS
(
    SELECT
        sdc.initial_severity_level_code,
        sdc.initial_severity_label,
        sdc.disposition_category,
        sdc.disposition_category_sort,
        sdc.total_incidents,
        ist.initial_severity_incidents,

        CAST(
            CAST(
                sdc.total_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    ist.initial_severity_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS percentage_within_initial_severity,

        RANK() OVER
        (
            PARTITION BY
                sdc.initial_severity_level_code

            ORDER BY
                sdc.total_incidents DESC
        ) AS disposition_rank_within_severity

    FROM severity_disposition_counts AS sdc

    INNER JOIN initial_severity_totals AS ist
        ON sdc.initial_severity_level_code
         = ist.initial_severity_level_code
)
SELECT
    initial_severity_level_code,
    initial_severity_label,
    disposition_category,
    total_incidents,
    initial_severity_incidents,
    percentage_within_initial_severity,
    disposition_rank_within_severity

FROM severity_disposition_statistics

ORDER BY
    initial_severity_level_code,
    disposition_rank_within_severity,
    disposition_category_sort,
    disposition_category;


-- =============================================================================
-- CQ11:
-- 11. Annual trend in final-disposition distribution
--
-- Business Question:
--     BQ11. What are the most common EMS incident dispositions, including
--     patient transport and non-transport outcomes?
--
-- Purpose:
--     Measure annual incident volume and annual percentage by standardized
--     final-disposition category.
--
--     The query also calculates the year-over-year percentage-point change
--     in each disposition category's share of annual EMS incidents.
--
-- Interpretation:
--     Changes in disposition distribution may reflect incident mix,
--     operational practices, documentation changes or external events.
--     The results are descriptive and do not establish causation.
-- =============================================================================

WITH annual_disposition_counts AS
(
    SELECT
        f.incident_year,
        dd.disposition_category,
        dd.disposition_category_sort,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_disposition AS dd
        ON f.disposition_key
         = dd.disposition_key

    GROUP BY
        f.incident_year,
        dd.disposition_category,
        dd.disposition_category_sort
),
annual_incident_totals AS
(
    SELECT
        incident_year,

        SUM(
            total_incidents
        ) AS annual_incidents

    FROM annual_disposition_counts

    GROUP BY
        incident_year
),
annual_disposition_percentages AS
(
    SELECT
        adc.incident_year,
        adc.disposition_category,
        adc.disposition_category_sort,
        adc.total_incidents,
        ait.annual_incidents,

        CAST(
            CAST(
                adc.total_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    ait.annual_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS annual_disposition_percentage

    FROM annual_disposition_counts AS adc

    INNER JOIN annual_incident_totals AS ait
        ON adc.incident_year
         = ait.incident_year
),
annual_disposition_trends AS
(
    SELECT
        incident_year,
        disposition_category,
        disposition_category_sort,
        total_incidents,
        annual_incidents,
        annual_disposition_percentage,

        LAG(
            annual_disposition_percentage
        ) OVER
        (
            PARTITION BY
                disposition_category

            ORDER BY
                incident_year
        ) AS previous_year_percentage

    FROM annual_disposition_percentages
)
SELECT
    incident_year,
    disposition_category,
    total_incidents,
    annual_incidents,
    annual_disposition_percentage,
    previous_year_percentage,

    CAST(
        annual_disposition_percentage
        - previous_year_percentage
        AS DECIMAL(18, 6)
    ) AS year_over_year_percentage_point_change

FROM annual_disposition_trends

ORDER BY
    incident_year,
    disposition_category_sort,
    disposition_category;


-- =============================================================================
-- CQ12:
-- 12. Final disposition by classification-change group
--
-- Business Questions:
--     BQ11. What are the most common EMS incident dispositions, including
--     patient transport and non-transport outcomes?
--
--     BQ12. How frequently do call type and severity classifications change
--     between the initial and final EMS dispatch records?
--
-- Purpose:
--     Compare the final-disposition distribution across four classification-
--     change groups:
--
--     1. Both call type and severity changed
--     2. Call type changed only
--     3. Severity changed only
--     4. Neither classification changed
--
--     The query calculates incident volume, percentage within each change
--     group and disposition rank within each change group.
--
-- Interpretation:
--     Differences between groups represent associations only. They do not
--     prove that classification changes caused a particular disposition.
--
--     Classification changes may reflect additional information obtained
--     during call handling or incident response.
-- =============================================================================

WITH classification_disposition_counts AS
(
    SELECT
        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 'Unknown change status'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 'Both call type and severity changed'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 'Call type changed only'

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 'Severity changed only'

            ELSE 'Neither classification changed'
        END AS classification_change_group,

        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 5

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 1

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 2

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 3

            ELSE 4
        END AS classification_change_sort,

        dd.disposition_category,
        dd.disposition_category_sort,

        SUM(
            CAST(f.incident_count AS BIGINT)
        ) AS total_incidents

    FROM dbo.gold_fact_ems_incident AS f

    INNER JOIN dbo.gold_dim_disposition AS dd
        ON f.disposition_key
         = dd.disposition_key

    GROUP BY
        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 'Unknown change status'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 'Both call type and severity changed'

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 'Call type changed only'

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 'Severity changed only'

            ELSE 'Neither classification changed'
        END,

        CASE
            WHEN f.call_type_changed IS NULL
              OR f.severity_changed IS NULL
            THEN 5

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 1
            THEN 1

            WHEN f.call_type_changed = 1
             AND f.severity_changed = 0
            THEN 2

            WHEN f.call_type_changed = 0
             AND f.severity_changed = 1
            THEN 3

            ELSE 4
        END,

        dd.disposition_category,
        dd.disposition_category_sort
),
classification_change_totals AS
(
    SELECT
        classification_change_group,
        classification_change_sort,

        SUM(
            total_incidents
        ) AS classification_group_incidents

    FROM classification_disposition_counts

    GROUP BY
        classification_change_group,
        classification_change_sort
),
classification_disposition_statistics AS
(
    SELECT
        cdc.classification_change_group,
        cdc.classification_change_sort,
        cdc.disposition_category,
        cdc.disposition_category_sort,
        cdc.total_incidents,
        cct.classification_group_incidents,

        CAST(
            CAST(
                cdc.total_incidents
                AS DECIMAL(38, 10)
            )
            * CAST(100 AS DECIMAL(38, 10))
            /
            NULLIF(
                CAST(
                    cct.classification_group_incidents
                    AS DECIMAL(38, 10)
                ),
                CAST(0 AS DECIMAL(38, 10))
            )
            AS DECIMAL(18, 6)
        ) AS percentage_within_change_group,

        RANK() OVER
        (
            PARTITION BY
                cdc.classification_change_group

            ORDER BY
                cdc.total_incidents DESC
        ) AS disposition_rank_within_change_group

    FROM classification_disposition_counts AS cdc

    INNER JOIN classification_change_totals AS cct
        ON cdc.classification_change_group
         = cct.classification_change_group
)
SELECT
    classification_change_group,
    disposition_category,
    total_incidents,
    classification_group_incidents,
    percentage_within_change_group,
    disposition_rank_within_change_group

FROM classification_disposition_statistics

ORDER BY
    classification_change_sort,
    disposition_rank_within_change_group,
    disposition_category_sort,
    disposition_category;