with core as (
select
  l.*,
  concat_ws(' ', first_name, last_name) as employee_name,
  em.employment_status,
  em.location as location,
  em.id as employee_manifest_id,
  e.name as employer_name,
  next_schedule.next_scheduled_date,
  a.magento_order_number,
  lsa.ledger_stage,
  lsa.include_in_balance_calculation,
  lsa.ledger_stage_category,
  sed.sub_employer_name,
  sed.sub_employer_status,
  em.worker_id,
  a.past_due_amount,
  apd.amount as past_due_agreement_amount
from
  bme.ledger l
  left join bme.agreements a on l.agreement_id = a.id
  left join bme.customer_entity c on a.customer_id = c.entity_id
  left join bme.employee_manifest em on c.entity_id = em.customer_id
  left join bme.employer e on c.employer_id = e.employer_id
  left join financials.v_ledger_status_attributes lsa on l.status = lsa.status
  left join financials.v_employer_subemployer_detail sed on em.employer_id = sed.employer_id and em.company_code = sed.company_code
  left join bme.agreements_past_due apd on l.agreement_id = apd.agreement_id

  -- Get the next schedule date  
  left join (
    select
      agreement_id,
      min(
        case
          when transaction_date >= date(sysdate())
          and status = 'scheduled' then transaction_date
        end
      ) as next_scheduled_date
    from
      bme.ledger
    group by
      agreement_id
  ) next_schedule on l.agreement_id = next_schedule.agreement_id
where
  e.employer_id = 227
  and l.cancelled_at is null
), past_due_employees as (
select distinct employee_manifest_id from core
  where status = 'past_due'
)
select core.*,
  case 
    when sub_employer_status = 'disconnected' then 'Employer Disconnected'
    when sub_employer_status is null then 'Employer Status Unknown'
    when employment_status = 'terminated' then 'Employee Terminated'
    when past_due_employees.employee_manifest_id is not null then 'Employee past-due'
    else 'Low Risk'    
  end as risk_bucket,
  case when status = 'past_due' then -amount end as past_due_amt,
  case when status = 'paid_payroll_deduction' then -amount end as paid_payroll_deduction_amt,
  case when status in ('past_due','paid_payroll_deduction') then -amount end as total_deduction_requested
from  core 
left join past_due_employees on core.employee_manifest_id = past_due_employees.employee_manifest_id
