with v_nums as (
SELECT (t.n*100 + u.n*10 + o.n) AS n
FROM (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7) t
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) u
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) o  
),
agreements as (

select
  a.customer_id,
  a.id as agreement_id,
  a.term,
  a.date_created,
  em.id as employee_manifest_id,
  em.pay_frequency,
  a.total as total_amount,
  (select min(pay_date) from bme.employee_paystubs where employee_manifest_id = em.id and pay_date > a.date_created) as first_pay_date,
  (select max(pay_date) from bme.employee_paystubs where employee_manifest_id = em.id and pay_date <= a.date_created) as most_recent_pay_date
  
from
  bme.agreements a
  inner join bme.employee_manifest em on a.customer_id = em.customer_id
where
  a.employer_id = 227
  
),
base AS (
  SELECT
    a.agreement_id,
    a.customer_id,
    a.date_created,
    a.pay_frequency,
    a.term,
    a.first_pay_date,
    a.most_recent_pay_date,
    a.total_amount,
    pft.num_periods,
    COALESCE(
      a.first_pay_date,
      DATE_ADD(a.most_recent_pay_date,
               INTERVAL (CASE a.pay_frequency
                           WHEN 'Weekly' THEN 7 WHEN 'Bi-Weekly' THEN 14
                           WHEN 'Semi-Monthly' THEN 15 WHEN 'Monthly' THEN 30
                         END) DAY),
      DATE_ADD(a.date_created,
               INTERVAL (CASE a.pay_frequency
                           WHEN 'Weekly' THEN 7 WHEN 'Bi-Weekly' THEN 14
                           WHEN 'Semi-Monthly' THEN 15 WHEN 'Monthly' THEN 30
                         END) DAY)
    ) AS anchor_date,
    -- per-installment amount in pennies, rounded down
    FLOOR(a.total_amount * 100 / pft.num_periods) / 100 AS base_amount,
    -- pennies left over, all dumped into the final installment
    (ROUND(a.total_amount * 100, 0)
       - FLOOR(a.total_amount * 100 / pft.num_periods) * pft.num_periods) / 100
       AS remainder_amount,
    -- per-row diagnostics, NULL components dropped automatically by CONCAT_WS
    CONCAT_WS('; ',
      CASE
        WHEN a.first_pay_date       IS NOT NULL THEN 'anchor=first_pay_date'
        WHEN a.most_recent_pay_date IS NOT NULL THEN 'anchor=most_recent_pay_date+1cycle'
        ELSE 'anchor=date_created+1cycle (NO_ANCHOR_DATA)'
      END,
      CASE WHEN a.first_pay_date IS NULL
                AND a.most_recent_pay_date IS NOT NULL
                AND DATEDIFF(a.date_created, a.most_recent_pay_date) > 60
           THEN CONCAT('STALE_PAYSTUB(', DATEDIFF(a.date_created, a.most_recent_pay_date), 'd)')
      END,
      CASE WHEN a.first_pay_date IS NOT NULL
                AND DATEDIFF(a.first_pay_date, a.date_created) > 35
           THEN CONCAT('LONG_FIRST_PAY_GAP(', DATEDIFF(a.first_pay_date, a.date_created), 'd)')
      END,
      CASE WHEN a.first_pay_date IS NOT NULL AND a.first_pay_date < a.date_created
           THEN 'FIRST_PAY_BEFORE_CREATED'
      END,
      CASE WHEN a.pay_frequency = 'Semi-Monthly'
                AND a.first_pay_date IS NOT NULL
                AND DAY(a.first_pay_date) > 16
           THEN CONCAT('SEMIMONTHLY_AMBIGUOUS_ANCHOR(day=', DAY(a.first_pay_date), ')')
      END
    ) AS diagnostics
  FROM agreements a
  JOIN pay_frequency_terms pft
    ON pft.term = a.term AND pft.pay_frequency = a.pay_frequency
)
SELECT
  b.agreement_id,
  b.pay_frequency,
  b.term,
  CASE b.pay_frequency
    WHEN 'Weekly'    THEN DATE_ADD(b.anchor_date, INTERVAL  7 * n.n DAY)
    WHEN 'Bi-Weekly' THEN DATE_ADD(b.anchor_date, INTERVAL 14 * n.n DAY)
    WHEN 'Monthly'   THEN DATE_ADD(b.anchor_date, INTERVAL n.n MONTH)
    WHEN 'Semi-Monthly' THEN
      CASE WHEN MOD(n.n, 2) = 0
           THEN DATE_ADD(b.anchor_date, INTERVAL (n.n / 2) MONTH)
           ELSE DATE_ADD(DATE_ADD(b.anchor_date, INTERVAL FLOOR(n.n / 2) MONTH), INTERVAL 15 DAY)
      END
  END AS schedule_date,
  CASE
    WHEN n.n + 1 = b.num_periods
    THEN b.base_amount + b.remainder_amount
    ELSE b.base_amount
  END AS amount,
  b.diagnostics
FROM base b
JOIN v_nums n
  ON n.n < b.num_periods
