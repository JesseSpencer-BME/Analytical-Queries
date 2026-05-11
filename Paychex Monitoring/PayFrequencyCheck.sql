with paystubs as (
select ep.*
from bme.employee_paystubs ep
  inner join bme.employee_manifest em
    on ep.employee_manifest_id = em.id  
where employer_id = 227
 -- and employee_manifest_id = 1676975
),
ordered AS (
  SELECT
    employee_manifest_id,
    pay_date,
    LAG(pay_date) OVER (PARTITION BY employee_manifest_id ORDER BY pay_date) AS prev_date
  FROM paystubs
),
gaps AS (
  SELECT employee_manifest_id, DATEDIFF(pay_date, prev_date) AS gap_days
  FROM ordered
  WHERE prev_date IS NOT NULL
  ),
  
  gap_counts AS (
  SELECT
    employee_manifest_id, gap_days, COUNT(*) AS freq,
    ROW_NUMBER() OVER (
      PARTITION BY employee_manifest_id
      ORDER BY COUNT(*) DESC, gap_days ASC
    ) AS rk
  FROM gaps
  GROUP BY employee_manifest_id, gap_days
),
modal AS (
  SELECT employee_manifest_id, gap_days AS modal_gap
  FROM gap_counts WHERE rk = 1
),
summary AS (
  SELECT
    p.employee_manifest_id,
    COUNT(*)                          AS n_paystubs,
    MIN(p.pay_date)                   AS first_pay,
    MAX(p.pay_date)                   AS last_pay,
    DATEDIFF(MAX(p.pay_date), MIN(p.pay_date)) AS span_days,
    COUNT(DISTINCT DAY(p.pay_date))   AS distinct_dom
  FROM paystubs p
  GROUP BY p.employee_manifest_id
),
gap_stats AS (
  SELECT
    employee_manifest_id,
    MIN(gap_days)                  AS min_gap,
    MAX(gap_days)                  AS max_gap,
    ROUND(AVG(gap_days), 1)        AS avg_gap,
    ROUND(STDDEV_POP(gap_days), 2) AS sd_gap
  FROM gaps
  GROUP BY employee_manifest_id
)
SELECT
  e.id as employee_manifest_id,
  e.pay_frequency,
  s.n_paystubs,
  s.first_pay, s.last_pay, s.span_days,
  gs.min_gap, m.modal_gap, gs.avg_gap, gs.max_gap, gs.sd_gap,
  s.distinct_dom,
  ROUND(s.n_paystubs / NULLIF(s.span_days, 0) * 365, 1) AS implied_per_year,
  CASE
    WHEN m.modal_gap BETWEEN 6 AND 8                                  THEN 'Weekly'
    WHEN m.modal_gap BETWEEN 13 AND 16 AND s.distinct_dom <= 3        THEN 'Semi-Monthly'
    WHEN m.modal_gap BETWEEN 13 AND 15 AND gs.sd_gap < 2              THEN 'Bi-Weekly'
    WHEN m.modal_gap BETWEEN 27 AND 32                                THEN 'Monthly'
    ELSE 'Unknown'
  END                                            AS pay_frequency_inferred,
  e.pay_frequency as pay_frequency_actual,
  CASE
    WHEN s.n_paystubs < 4 THEN 'INSUFFICIENT_DATA'
    ELSE NULL
  END                                            AS data_quality_flag
FROM bme.employee_manifest e
inner join financials.v_customer_entity_summary ces on e.customer_id = ces.entity_id
LEFT JOIN summary   s  ON s.employee_manifest_id  = e.id
LEFT JOIN gap_stats gs ON gs.employee_manifest_id = e.id
LEFT JOIN modal     m  ON m.employee_manifest_id  = e.id
where 
  e.employer_id = 227
  and ces.total_orders > 0
  -- and e.id = 1676975
  ORDER BY e.id

