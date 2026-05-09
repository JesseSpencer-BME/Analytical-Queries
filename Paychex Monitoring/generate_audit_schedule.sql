WITH v_nums AS (
  SELECT seq AS n FROM seq_0_to_103
),
agreements AS (
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
    pft.approx_pay_cycle_days,
    COALESCE(
      a.first_pay_date,
      DATE_ADD(a.most_recent_pay_date, INTERVAL pft.approx_pay_cycle_days DAY),
      DATE_ADD(a.date_created,        INTERVAL pft.approx_pay_cycle_days DAY)
    ) AS anchor_date,
    FLOOR(a.total_amount * 100 / pft.num_periods) / 100 AS base_amount,
    (ROUND(a.total_amount * 100, 0)
       - FLOOR(a.total_amount * 100 / pft.num_periods) * pft.num_periods) / 100
       AS remainder_amount,
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
