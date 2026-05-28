-- create or replace view financials.v_paychex_customer_status as
with core_data as (
select
  em.id,
  em.customer_id,
  employee_id,
  em.company_code,
  em.pay_frequency,
  concat_ws(' ',first_name, last_name) as name,
  fd.first_pay_component_deduction_sent as first_pay_component_deduction_sent_raw,
  case
    when weekday(fd.first_pay_component_deduction_sent) = 5 then fd.first_pay_component_deduction_sent + interval 2 day -- Saturday
    when weekday(fd.first_pay_component_deduction_sent) = 6 then fd.first_pay_component_deduction_sent + interval 1 day -- Sunday
    else fd.first_pay_component_deduction_sent
  end as first_pay_component_deduction_sent,
  total_agreements,
  first_purchase_date,
  purchase_total,
  total_paystubs,
  all_pay_dates,      
  first_pay_date,
  last_pay_date,  
  all_paydates_with_deduction,
  first_pay_date_with_deduction,
  last_pay_date_with_deduction,
  ed.status as connection_status,
  ed.employer_disconnect_date,
  em.employment_status,
  em.termination_date,
  case when 
    em.employment_status = 'blocked' then 'blocked'
    else null
  end as is_blocked,
  past_due_amount,
  open_balance,
  case when past_due_days > 0 then past_due_days end as past_due_days,  
  paid_payroll_deduction_amt,
  paid_payroll_deduction_dates,
  paid_via_card_voluntarily,
  paid_via_card_collection,
  pay_frequency_cycle_days,
  paycycle_scheduled_payment,
  DATEDIFF(date(sysdate()),first_pay_component_deduction_sent) as days_since_first_deduction_sent,
  (DATEDIFF(date(sysdate()),first_pay_component_deduction_sent)) / pay_frequency_cycle_days as pay_cycles_since_first_deduction_sent,
  c.next_pay_date,
  deductions_per_paystub,  
  last_pay_run_status.employee_manifest_id,
  last_pay_run_status.pay_period_id_paycheck,
  last_pay_run_status.pay_period_id_payperiod,
  last_pay_run_status.status,
  last_pay_run_status.paycycle_closed_at,
  last_pay_run_status.last_pay_interval,
  last_pay_run_status.last_pay_description,
  last_pay_run_status.last_pay_period_startDate,
  last_pay_run_status.last_pay_period_endDate,
  last_pay_run_status.last_pay_period_submitByDate,
  last_pay_run_status.deduction_comparison_date,
  last_pay_run_status.next_deduction_comparison_date,
  last_pay_run_status.date_selection_reason,
  last_pay_run_status.next_deduction_date_selection_reason,
  TIMESTAMPDIFF(HOUR, paycycle_closed_at,first_pay_component_deduction_sent) as hours_between_component_send_and_payroll_close,
  last_company_run_by_paycycle.company_last_scheduled_check_cycle,
  last_company_run_by_paycycle.company_last_completed_check_cycle,
  last_company_run_by_paycycle.company_last_webhook,
  cs.paychex_research_notes,
  datediff(sysdate(),last_pay_date) as days_since_last_check,
  datediff(sysdate(),last_pay_date) / pay_frequency_cycle_days as cycles_since_last_check,
  model_past_due.past_due_balance_modelled,

  case when ed.employer_disconnect_date is not null then ed.employer_disconnect_date else em.termination_date end as last_active_date,  

  date(case when first_pay_component_deduction_sent > next_deduction_comparison_date -- check if deduction happened after our last-known comparison date
      then  first_pay_component_deduction_sent + interval pay_frequency_cycle_days day
    else next_deduction_comparison_date + interval pay_frequency_cycle_days day
  end) as next_expected_deduction_date,
  
  case when first_pay_component_deduction_sent > next_deduction_comparison_date -- check if deduction happened after our last-known comparison date
      then  'Deduction after last known paystub data - used deduction + offset'
    else 'Used last known paystub + offset'
  end as next_expected_deduction_date_reason
  
  
from bme.employee_manifest em
  left join bme.customer_entity c on em.customer_id = c.entity_id
  left join financials.customer_supplemental cs on em.customer_id = cs.customer_id

  -- Employer Disconnect Data
  left join (
      select
      ed.entity_id as employee_department_id,
      ed.department_prefix,
      ed.employer_id,
      ed.status,
      ed.updated_at,
      first_disconnect.first_disconnect,
      datediff(ed.updated_at, first_disconnect.first_disconnect),
      coalesce(first_disconnect.first_disconnect,ed.updated_at) as employer_disconnect_date
    from
      bme.employer_department ed
      left join (
        select
          department_prefix,
          min(created_at) as first_disconnect
        from
          bme.employer_department_history
        where
          status = 'disconnected'
        group by
          department_prefix
      ) first_disconnect on ed.department_prefix = first_disconnect.department_prefix
    where
      ed.employer_id = 227
      and ed.status = 'disconnected'      
    ) ed on ed.department_prefix = em.company_code and ed.employer_id = em.employer_id

  
  -- All agreements
  inner join (
    select
    customer_id,
    count(1) as total_agreements,
    sum(total) as purchase_total,
    min(date_created) as first_purchase_date,
    sum(payments) as paycycle_scheduled_payment,
    sum(past_due_amount) as past_due_amount,
    sum(balance) as open_balance,
    max(past_due_days) as past_due_days
  from
    bme.agreements
  where
    employer_id = 227
  group by customer_id
  ) customer_orders on em.customer_id = customer_orders.customer_id

  -- Blocked Employees
  left join (
    select distinct employee_manifest_id, 'blocked' as is_blocked from bme.employee_paystubs
    where json_value(additional_data, '$.deductions[0].isBlocked') = '1'
    and employee_manifest_id in (select id from bme.employee_manifest where employer_id = 227 and customer_id is not null)
  ) blocked on em.id = blocked.employee_manifest_id

  -- Get first deduction date API'd to Paychex
  left join (
    select
      worker_id,
      min(sent_at) as first_pay_component_deduction_sent
    from
      employers.customer_pay_components
    group by
      worker_id
  ) fd on em.employee_id = fd.worker_id

  -- Get all Paystubs with deductions
  left join (
    select
        employee_manifest_id,
        count(1) as total_paystubs,
        group_concat(pay_date order by pay_date) as all_pay_dates,      
        max(pay_date) as last_pay_date,
        min(pay_date) as first_pay_date,
        group_concat(case when deductions>0 then pay_date end order by pay_date asc) as all_paydates_with_deduction,
        min(case when deductions > 0 then pay_date end) as first_pay_date_with_deduction,
        max(case when deductions > 0 then pay_date end) as last_pay_date_with_deduction,
        sum(deductions) as deductions_per_paystub        
        from
        bme.employee_manifest em
        left join bme.employee_paystubs ep on em.id = ep.employee_manifest_id
      where
        em.employer_id = 227
        and customer_id is not null
        and pay_date >= '2025-04-01'
      group by employee_manifest_id
  
  ) paystub_details on em.id = paystub_details.employee_manifest_id

  -- Get Ledger Summary Data
  left join(
      select
        a.customer_id,
        sum(case when l.status = 'paid_payroll_deduction' then (- amount) end) as paid_payroll_deduction_amt,
        group_concat( distinct case when l.status = 'paid_payroll_deduction' then l.transaction_date end) as paid_payroll_deduction_dates,
        sum( case when l.status = 'paid_self_card_payment' then (- amount) end ) as paid_via_card_voluntarily,
        sum( case when l.status = 'paid_auto_card_collection' then (- amount) end ) as paid_via_card_collection
      from
        bme.ledger l
        inner join bme.agreements a on l.agreement_id = a.id
      where
        l.cancelled_at is null
        and a.employer_id = 227
      group by
        customer_id  
  ) ledger_summary on em.customer_id = ledger_summary.customer_id

  -- Pay Frequency Days
    left join
    (
      select 'Bi-Weekly' as pay_frequency, 14 as pay_frequency_cycle_days
      union
      select 'Weekly' as pay_frequency, 7 as pay_frequency_cycle_days
      union
      select 'Semi-Monthly' as pay_frequency, 15 as pay_frequency_cycle_days
      union
      select 'Monthly' as pay_frequency, 31 as pay_frequency_cycle_days
    ) pf on em.pay_frequency = pf.pay_frequency

  -- last pay run data from paychex
  left join (    
    WITH last_paystub_per_employee AS (
        -- Get the most recent paystub per employee for employer 227
        SELECT
            em.id AS employee_manifest_id,
            JSON_VALUE(ep.additional_data, '$.payPeriodId') AS pay_period_id,
            em.customer_id,
            em.employer_id,
            ROW_NUMBER() OVER (
                PARTITION BY ep.employee_manifest_id
                ORDER BY ep.pay_date DESC
            ) AS rn
        FROM bme.employee_paystubs ep
        INNER JOIN bme.employee_manifest em
            ON ep.employee_manifest_id = em.id
        INNER JOIN bme.agreements ag
            ON ag.customer_id = em.customer_id
            AND ag.employer_id = em.employer_id
        WHERE em.employer_id = 227
    ),
    
    pay_period_details AS (
      -- Pull the relevant pay period attributes for the latest paystub
        SELECT
            lp.employee_manifest_id,
            lp.pay_period_id                                AS pay_period_id_paycheck,
            cp.pay_period_id                                AS pay_period_id_payperiod,
            cp.status,
            cp.completed_at                                 AS paycycle_closed_at,
            JSON_VALUE(cp.raw_data, '$.intervalCode')       AS last_pay_interval,
            JSON_VALUE(cp.raw_data, '$.description')        AS last_pay_description,
            DATE(STR_TO_DATE(JSON_VALUE(cp.raw_data, '$.startDate'),     '%Y-%m-%dT%H:%i:%sZ')) AS last_pay_period_startDate,
            DATE(STR_TO_DATE(JSON_VALUE(cp.raw_data, '$.endDate'),       '%Y-%m-%dT%H:%i:%sZ')) AS last_pay_period_endDate,
            DATE(STR_TO_DATE(JSON_VALUE(cp.raw_data, '$.submitByDate'),  '%Y-%m-%dT%H:%i:%sZ')) AS last_pay_period_submitByDate
        FROM last_paystub_per_employee lp
        LEFT JOIN employers.company_payperiods cp
            ON lp.pay_period_id = cp.pay_period_id
        WHERE lp.rn = 1
    ),
    
    with_comparison_dates AS (
        SELECT
            pd.*,
            LEAST(
                COALESCE(last_pay_period_endDate,       '9999-12-31'),
                COALESCE(paycycle_closed_at,            '9999-12-31'),
                COALESCE(last_pay_period_submitByDate,  '9999-12-31')
            ) AS deduction_comparison_date,
            GREATEST(
                COALESCE(last_pay_period_endDate,       '2026-01-01'),
                COALESCE(paycycle_closed_at,            '2026-01-01'),
                COALESCE(last_pay_period_submitByDate,  '2026-01-01')
            ) AS next_deduction_comparison_date
        FROM pay_period_details pd
    )
    
    SELECT
        c.*,
        CASE deduction_comparison_date
            WHEN DATE '9999-12-31'                  THEN 'NONE'
            WHEN last_pay_period_endDate            THEN 'last_pay_period_endDate'
            WHEN paycycle_closed_at                 THEN 'last_paycycle_closed_at'
            WHEN last_pay_period_submitByDate       THEN 'last_pay_period_submitByDate'
        END AS date_selection_reason,
    
        CASE next_deduction_comparison_date
            WHEN DATE '2026-01-01'                  THEN 'NONE'
            WHEN last_pay_period_endDate            THEN 'last_pay_period_endDate'
            WHEN paycycle_closed_at                 THEN 'last_paycycle_closed_at'
            WHEN last_pay_period_submitByDate       THEN 'last_pay_period_submitByDate'
        END AS next_deduction_date_selection_reason
    FROM with_comparison_dates c
    ) last_pay_run_status on em.id = last_pay_run_status.employee_manifest_id

-- get the most recent paycycle date closed for the employer
  left join (
      select
        company_id,
        JSON_VALUE(cp.raw_data, '$.description') as pay_description,
        max(check_date) as company_last_scheduled_check_cycle,
        max(case when status = 'COMPLETED' then check_date end) as company_last_completed_check_cycle,
        max(completed_at) as company_last_webhook
      from
        employers.company_payperiods cp        
      group by
        company_id,
        JSON_VALUE(cp.raw_data, '$.description')
    ) last_company_run_by_paycycle on em.company_code = last_company_run_by_paycycle.company_id
      and last_pay_run_status.last_pay_description = last_company_run_by_paycycle.pay_description

  left join
  -- Get "modelled" past-due
  (
    with 
    
      -- Schedules
    schedule_amount as (
    select
      a.customer_id,
      sum(amount) as expected_amount
    from
      financials.v_paychex_schedule_audit s -- temp_schedule
      inner join bme.agreements a on s.agreement_id = a.id
      where schedule_date < date(sysdate())
    group by a.customer_id
    ),
    
    -- Paid amount
    paid_amount as 
      (
    select
      a.customer_id,
      sum(l.amount) as paid
    from
      bme.ledger l
      inner join bme.agreements a on l.agreement_id = a.id
      inner join bme.employee_manifest em on a.customer_id = em.customer_id
    where
      a.employer_id = 227
      and l.status like '%paid%'
      and l.cancelled_at is null
    group by a.customer_id
      )
    select 
      s.customer_id,
      round(expected_amount,0) as expected_amount,
      round(paid,0) as paid_amount,
      case 
        when round(expected_amount + coalesce(paid,0),0) > 0 then 
        round(expected_amount + coalesce(paid,0),0) 
      else 0
      end as past_due_balance_modelled  
       from schedule_amount s
    left join paid_amount p 
      on s.customer_id = p.customer_id
  ) model_past_due on em.customer_id = model_past_due.customer_id

-- Overall Filters for whole query
where em.employer_id = 227 and em.customer_id is not null 
)

select *,
1 as total_customer_count,
TIMESTAMPDIFF(HOUR, deduction_comparison_date,first_pay_component_deduction_sent) as hours_between_component_send_and_deduction_comparison_date,


floor((to_days(sysdate()) - to_days(last_active_date)) / pay_frequency_cycle_days) as cycles_since_last_active,


case when past_due_amount> 0 then 1 else 0 end as customer_past_due_count,
case when past_due_amount> 0 then open_balance end as past_due_open_balance,
case when past_due_amount> 0 then purchase_total end as past_due_purchase_total,
  

case 
  when first_pay_component_deduction_sent is null then 'No First Deduction Found'
  else 'First Deduction Sent'
end as first_deduction_status,
  
case

    -- Categories to stop evaluating others (handle these first):
    when connection_status = 'disconnected' then 'company disconnected'
    when employment_status = 'Terminated' then 'employee terminated'
    when is_blocked = 'blocked' then 'blocked'
    when first_pay_date_with_deduction is not null then 'at least one deduction received (paystub)'
    when paid_payroll_deduction_amt > 0 then 'at least one deduction received (ledger)'
    when first_pay_component_deduction_sent is null then 'No Pay Component Found Sent to Paychex'
    when first_pay_date is null then 'no paystubs found for employee'

    when (date_selection_reason != 'NONE' and first_pay_component_deduction_sent < (deduction_comparison_date - interval 1 day))
      then 'Deduction Sucessfully Submitted 24 Hours Before Cutoff - Research Issue'

    when next_expected_deduction_date < date(sysdate())
      then 'First Expected Deduction Date has Passed - Research Issue' 

    when next_expected_deduction_date >= date(sysdate()) and cycles_since_last_check > 2
      then 'First Expected Deduction Date in the Future: 2 cycles since paycheck'

    when next_expected_deduction_date >= date(sysdate()) and cycles_since_last_check > 1
      then 'First Expected Deduction Date in the Future: 1 cycles since paycheck'

    when next_expected_deduction_date >= date(sysdate()) and cycles_since_last_check >= -1 -- have cases where we get "payroll" dates in the future, or have to handle for weekends
        then 'First Expected Deduction Date in the Future: in active cycle'  
  
    when last_pay_date < date(sysdate() - INTERVAL 33 day) then 'employee not paid - no paystub for over 1 month'
    -- when paycycle_closed_at is null then 'No Found Payroll Completion Date for Employee'
    when first_pay_component_deduction_sent > deduction_comparison_date 
      and ((next_deduction_comparison_date + interval pay_frequency_cycle_days-1 DAY) < DATE(SYSDATE()))
      then 'Deduction sent after Earliest PayDate, Submit Date, or Close Date: Past Due'

    when first_pay_component_deduction_sent > deduction_comparison_date 
        and  (next_deduction_comparison_date + interval pay_frequency_cycle_days-1 DAY) >= DATE(SYSDATE())
        then 'Deduction sent after Earliest PayDate, Submit Date, or Close Date: Still Expected'
  
    when first_pay_component_deduction_sent > last_pay_period_endDate then 'Pay component sent after last pay period end date'

  
    when TIMESTAMPDIFF(HOUR, deduction_comparison_date,first_pay_component_deduction_sent) between -24 and 0 then 'Deduction submitted within 24 hours of deduction send cutoff'
    when TIMESTAMPDIFF(HOUR, deduction_comparison_date,first_pay_component_deduction_sent) < -24 then 'Deduction submitted more than 24 hours before deduction send cutoff'
    -- when hours_between_component_send_and_payroll_close between -24 and 0 then 'Deduction submitted within 24 hours of pay-cycle cutoff'
    -- when hours_between_component_send_and_payroll_close < -24 then 'Deduction submitted more than 24 hours before pay-cycle cutoff'
    when paycycle_closed_at < first_pay_component_deduction_sent then 'Payroll closed before first deduction sent'
    else 'unknown'
  end as risk_driver,
  
case 
    when first_pay_component_deduction_sent is not null then 'Valid pay components recorded w/ Paychex'
    when first_purchase_date >= '2026-04-17' and first_pay_component_deduction_sent is null then 'No valid pay component recorded w/ Paychex'
    when first_purchase_date < '2026-04-17' then 'Purchased before logged pay component data (26-04-17)'
  end as paychex_deduction_component_status,
  
case 
  when first_purchase_date >= (date(sysdate()) - interval pay_frequency_cycle_days day) then 'first purchase made within last paycycle'
  else 'first purchase made before last paycycle' 
end as first_purchase_within_last_cycle,

  case 
    when pay_cycles_since_first_deduction_sent > 2 then 'More than two pay-cycles have past'
    else 'Fewer than two pay-cycles have past' 
  end as cycles_since_first_deduction,
  
  case 
    when paid_payroll_deduction_amt > 0 and first_pay_date_with_deduction is null then 'Have Ledger Deductions, but no Paycheck'
  end as ledger_deductions_without_paystub,

  case 
    when paycycle_closed_at is null then 'No Found Payroll Completion Date'  
    when first_pay_component_deduction_sent is null then 'No Found Deduction Component Sent'
    when paycycle_closed_at < first_pay_component_deduction_sent then 'Payroll closed before first deduction sent'
    else 'Payroll closed AFTER deduction sent'
  end as payroll_vs_pay_component_status

from core_data
