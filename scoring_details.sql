WITH

-- Most recent fast track record per customer.
-- Fast track records have a $.fastTrack field but no $.score field.
fast_track AS (
    SELECT
        customer_id,
        created_at                                                                AS ft_created_at,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.fastTrack'))                          AS fast_track,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.reason'))                             AS ft_fail_reason,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.canScore'))                           AS can_score,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.estimatedSalary'))       AS ft_est_salary,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.requireCreditScore'))    AS require_credit_score,

        -- Categorise known fast track fail reasons for easy filtering/grouping.
        -- Add new WHEN branches here as new reason strings are introduced upstream.
        CASE
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.reason')) LIKE '%Tenure%'           THEN 'tenure'
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.reason')) LIKE '%Credit score%'     THEN 'credit_score_required'
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.reason')) LIKE '%Not full-time%'    THEN 'not_full_time'
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.reason')) LIKE '%Not active%'       THEN 'not_active'
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.reason')) LIKE '%Salary%'           THEN 'salary_below_criteria'
            ELSE 'other'
        END                                                                      AS ft_fail_reason_category,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY created_at DESC
        ) AS rn
    FROM account.customer_score
    WHERE JSON_EXTRACT(data, '$.fastTrack') IS NOT NULL
      AND JSON_EXTRACT(data, '$.score') IS NULL
),

-- Most recent full score record per customer.
-- Full score records have a $.score field.
scoring AS (
    SELECT
        customer_id,
        created_at                                                                AS scoring_created_at,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.score'))                              AS score,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.tier.is_approved'))                   AS is_approved,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.creditLimit'))                        AS scored_credit_limit,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.tier.model_type'))                    AS scoring_model,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.tier.down_payment_percent'))          AS down_payment_percent,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.tier.max_term'))                      AS max_term,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.tier.scoring_tier_id'))               AS scoring_tier_id,

        -- Employment inputs captured at the time of scoring.
        -- employment_type is NULLIF'd because the field is sometimes an empty string
        -- rather than NULL when the value is unknown.
        NULLIF(JSON_UNQUOTE(JSON_EXTRACT(data, '$.request.employment_type')), '') AS employment_type_at_scoring,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.request.employment_length'))          AS employment_length_at_scoring,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.request.aba_on_bad_list'))            AS aba_on_bad_list,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.request.bin_on_bad_list'))            AS bin_on_bad_list,

        -- Paystub fields — only populated on paystub_model records.
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.estimatedSalary'))       AS est_salary,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.payFrequency'))          AS pay_frequency,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.denied'))                AS paystub_denied,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.startDate'))             AS paystub_start_date,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.hasTenure'))             AS has_tenure,
        JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.hasGoodYTD'))            AS has_good_ytd,

        -- -------------------------------------------------------------------------
        -- Data quality flags
        -- Each flag is 1 = issue present, 0 = no issue.
        -- All flags are also combined into data_quality_issues in the final SELECT.
        -- -------------------------------------------------------------------------

        -- Approved but the credit limit resolved to zero — likely a downstream issue.
        CASE
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.tier.is_approved')) = 'true'
             AND CAST(JSON_EXTRACT(data, '$.creditLimit') AS DECIMAL) = 0
            THEN 1 ELSE 0
        END                                                                      AS approved_but_zero_limit,

        -- Salary > $500k on a paystub-based record is anomalous and likely erroneous.
        -- Note: salaries derived from self-reported income (no real paystubs) are
        -- captured separately by used_reported_income_flag below, and will frequently
        -- also trigger this flag. If the two populations need to be distinguished,
        -- filter on used_reported_income_flag = 0 to isolate genuine paystub anomalies.
        CASE
            WHEN CAST(JSON_EXTRACT(data, '$.paycheckData.estimatedSalary') AS DECIMAL) > 500000
            THEN 1 ELSE 0
        END                                                                      AS suspect_salary,

        -- No real paystubs were processed; salary was estimated from self-reported income.
        -- Source log message: "Using 75% of reported income to estimate net salary"
        CASE
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.log')) LIKE '%reported income%'
            THEN 1 ELSE 0
        END                                                                      AS used_reported_income_flag,

        -- Paystub processing confirmed employment tenure is below the minimum threshold.
        -- Source log message: "Employment tenure is below minimum threshold"
        CASE
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.log')) LIKE '%below minimum threshold%'
            THEN 1 ELSE 0
        END                                                                      AS paystub_tenure_flag,

        -- 2 or more paychecks were below 75% of the monthly average, indicating
        -- inconsistent income that may affect repayment reliability.
        -- Source log message: "2 or more checks are below 75% of monthly average (N)"
        CASE
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.log')) LIKE '%checks are below 75%'
            THEN 1 ELSE 0
        END                                                                      AS paystub_inconsistent_income_flag,

        -- The paystub-derived start date was used instead of the manifest start date,
        -- indicating a discrepancy between the two sources.
        -- Source log message: "Using paystub date for employment start date"
        CASE
            WHEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.paycheckData.log')) LIKE '%paystub date for employment start%'
            THEN 1 ELSE 0
        END                                                                      AS paystub_start_date_override_flag,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY created_at DESC
        ) AS rn
    FROM account.customer_score
    WHERE JSON_EXTRACT(data, '$.score') IS NOT NULL
),

-- Most relevant employment record per customer.
-- Priority order: Active > Leave of Absence > Terminated > anything else,
-- then most recently updated within each status tier.
employment AS (
    SELECT
        em.*,
        ROW_NUMBER() OVER (
            PARTITION BY em.customer_id
            ORDER BY
                IF(em.employment_status = 'Active',           1,
                IF(em.employment_status = 'Leave of Absence', 2,
                IF(em.employment_status = 'Terminated',       3, 4))),
                em.updated_at DESC
        ) AS rn
    FROM bme.employee_manifest em
    WHERE em.employer_id = 227
      AND em.customer_id IS NOT NULL
      AND em.customer_id != ''
)

-- Final select
SELECT
    -- Customer identity
    ce.entity_id,
    ce.firstname,
    ce.lastname,
    ce.email,
    ce.dob,
    ce.status                                               AS ce_status,
    ce.credit_limit                                         AS ce_credit_limit,

    -- Employment manifest
    em.start_date,
    TIMESTAMPDIFF(MONTH, em.start_date, CURDATE())          AS tenure_months,
    em.salary,
    em.employment_status,
    em.employment_schedule,
    em.pay_frequency,

    -- Fast track results
    ft.fast_track,
    ft.ft_fail_reason,
    ft.ft_fail_reason_category,
    ft.can_score,
    ft.require_credit_score,
    ft.ft_est_salary,
    ft.ft_created_at,

    -- Full score results
    sc.score,
    sc.scoring_model,
    sc.scoring_created_at,
    sc.is_approved,
    sc.scored_credit_limit,
    sc.down_payment_percent,
    sc.max_term,
    sc.scoring_tier_id,
    sc.employment_type_at_scoring,
    sc.employment_length_at_scoring,
    sc.aba_on_bad_list,
    sc.bin_on_bad_list,

    -- Paystub detail (paystub_model records only)
    sc.est_salary,
    sc.pay_frequency                                        AS scoring_pay_frequency,
    sc.paystub_denied,
    sc.paystub_start_date,
    sc.has_tenure,
    sc.has_good_ytd,

    -- Data quality flags (individual — 1 = issue present, 0 = no issue)
    sc.approved_but_zero_limit,
    sc.suspect_salary,
    sc.used_reported_income_flag,
    sc.paystub_tenure_flag,
    sc.paystub_inconsistent_income_flag,
    sc.paystub_start_date_override_flag,

    -- Data quality summary — comma-separated list of all active flags on this record.
    -- Null if no issues detected. Useful for quick scanning in BI tools / spreadsheets.
    NULLIF(CONCAT_WS(', ',
        IF(sc.approved_but_zero_limit = 1,              'approved_zero_limit',       NULL),
        IF(sc.suspect_salary = 1,                       'suspect_salary',            NULL),
        IF(sc.used_reported_income_flag = 1,            'reported_income',           NULL),
        IF(sc.paystub_tenure_flag = 1,                  'tenure_below_threshold',    NULL),
        IF(sc.paystub_inconsistent_income_flag = 1,     'inconsistent_income',       NULL),
        IF(sc.paystub_start_date_override_flag = 1,     'start_date_override',       NULL)
    ), '')                                                  AS data_quality_issues

FROM bme.customer_entity ce

LEFT JOIN employment em  ON em.customer_id = ce.entity_id AND em.rn = 1
LEFT JOIN fast_track ft  ON ft.customer_id = ce.entity_id AND ft.rn = 1
LEFT JOIN scoring sc     ON sc.customer_id = ce.entity_id AND sc.rn = 1

WHERE ce.employer_id = 227


ORDER BY em.start_date ASC, em.salary DESC;


select count(1) from bme.customer_entity
where employer_id = 227
