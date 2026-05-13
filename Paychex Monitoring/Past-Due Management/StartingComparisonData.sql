with
  ledger_first_date as (
    select
      customer_id,
      agreement_id,
      min(transaction_date) as first_schedule_per_ledger
    from
      bme.ledger l
      inner join bme.agreements a on l.agreement_id = a.id
      inner join bme.ledger_statuses ls on l.status = ls.status
    where
      a.employer_id = 227
      and l.status in ('scheduled', 'paid_payroll_deduction', 'past_due') -- These are statuses which are or could have been a schedule date
    group by
      customer_id,
      agreement_id
  ),
  schedule_audit_first_date as (
    select
      agreement_id,
      min(schedule_date) as first_schedule_per_model,
      sum(case when schedule_date < date(sysdate()) then amount end) as expected_per_model
    from
      v_paychex_schedule_audit
    group by
      agreement_id
  ),
  paid as (
    select
      l.agreement_id,
      sum(l.amount) as paid_amount
    from
      bme.ledger l
      inner join bme.agreements a on l.agreement_id = a.id
      inner join bme.employee_manifest em on a.customer_id = em.customer_id
    where
      a.employer_id = 227
      and l.status like '%paid%'
      and l.cancelled_at is null
    group by
      l.agreement_id
  )
select
  l.*,
  s.*,
  a.magento_order_number,
  a.term,
  a.sale_date,
  a.date_created as agreement_date,
  em.pay_frequency,
  a.past_due_amount as past_due_per_agreement,
  paid.paid_amount,
  coalesce(s.expected_per_model,0) + coalesce(paid.paid_amount,0) as past_due_per_model,
  case
    when first_schedule_per_model = first_schedule_per_ledger then 'schedules agree'
    else 'schedules disagree'
  end as schedule_starts_agree
from
  ledger_first_date l
  left join schedule_audit_first_date s on l.agreement_id = s.agreement_id
  left join paid on l.agreement_id = paid.agreement_id
  inner join bme.agreements a on l.agreement_id = a.id
  inner join bme.employee_manifest em on a.customer_id = em.customer_id
