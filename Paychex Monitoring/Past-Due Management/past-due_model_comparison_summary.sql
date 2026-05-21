with
  core as (
    select
      customer_id,
      round(coalesce(total_customer_past_due, 0), 2) as past_due_per_agreements,
      sum(scheduled_amt_model),
      sum(scheduled_amt_ledger),
      sum(paid_amount),
      round(
        coalesce(sum(scheduled_amt_model), 0) + coalesce(sum(paid_amount), 0),
        2
      ) as past_due_per_model,
      round(
        coalesce(sum(scheduled_amt_ledger), 0) + coalesce(sum(paid_amount), 0),
        2
      ) as past_due_per_ledgers
    from
      financials.v_paychex_schedule_comparison_detail
    where
      binary(transaction_timeframe) = binary('current_transactions')
    group by
      customer_id
  )
select
  core.*,
  case when ed.status = 'disconnected' then 'disconnected'
  else em.employment_status
  end as employee_status,  
  case
    when past_due_per_agreements = past_due_per_model
    and past_due_per_agreements = past_due_per_ledgers then 'Clean - all models agree'
    when past_due_per_model = past_due_per_ledgers then 'Model aligns with Ledgers'
    when past_due_per_agreements = past_due_per_ledgers then 'Ledgers Align with Agreements'
    else 'Keep Investigating'
  end as analysis_status,
  case
    when past_due_per_ledgers > 0 then 'has_past_due_ledgers'
    when past_due_per_model > 0 then 'has_past_due_per_model'
    when past_due_per_agreements > 0 then 'has_past_due_per_agreements'
    else 'not_past_due'
  end as evaluate_for_past_due,
  case
    when past_due_per_ledgers > 0 then 1
    else 0
  end as past_due_customer_per_ledger,
  case
    when past_due_per_model > 0 then 1
    else 0
  end as past_due_customer_per_model,
  case
    when past_due_per_agreements > 0 then 1
    else 0
  end as past_due_customer_per_agreements
from
  core
  left join bme.employee_manifest em on core.customer_id = em.customer_id
  left join bme.employer_department ed on em.company_code = ed.department_prefix
