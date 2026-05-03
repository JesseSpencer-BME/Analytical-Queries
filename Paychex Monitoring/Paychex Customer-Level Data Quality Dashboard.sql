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
  em.employment_status,
  blocked.is_blocked,
  paid_payroll_deduction_amt,
  paid_payroll_deduction_dates,
  paid_via_card_voluntarily,
  paid_via_card_collection,
  pay_frequency_cycle_days,
  paycycle_scheduled_payment,
  DATEDIFF(date(sysdate()),first_pay_date_with_deduction) as days_since_first_deduction_sent,
  (DATEDIFF(date(sysdate()),first_pay_date_with_deduction)) / pay_frequency_cycle_days as pay_cycles_since_first_deduction_sent,
  c.next_pay_date,
  deductions_per_paystub,
  last_pay_run_status.*,
  TIMESTAMPDIFF(HOUR, paycycle_closed_at,first_pay_component_deduction_sent) as hours_between_component_send_and_payroll_close,
  least(
    coalesce(last_pay_period_endDate,      '9999-12-31'),
    coalesce(paycycle_closed_at,           '9999-12-31'),
    coalesce(last_pay_period_submitByDate, '9999-12-31')
  ) as deduction_comparison_date,
  greatest(
    coalesce(last_pay_period_endDate,      '2026-01-01'),
    coalesce(paycycle_closed_at,           '2026-01-01'),
    coalesce(last_pay_period_submitByDate, '2026-01-01')
  ) as next_deduction_comparison_date,

  last_company_run_by_paycycle.company_last_scheduled_check_cycle,
  last_company_run_by_paycycle.company_last_completed_check_cycle,
  last_company_run_by_paycycle.company_last_webhook,
  cs.paychex_research_notes
  
from bme.employee_manifest em
  left join bme.employer_department ed on ed.department_prefix = em.company_code and ed.employer_id = em.employer_id
  left join bme.customer_entity c on em.customer_id = c.entity_id
  left join financials.customer_supplemental cs on em.customer_id = cs.customer_id
  
  -- All agreements
  inner join (
    select
    customer_id,
    count(1) as total_agreements,
    sum(total) as purchase_total,
    min(date_created) as first_purchase_date,
    sum(payments) as paycycle_scheduled_payment
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
   select
        a.employee_manifest_id,
        a.payPeriodId as pay_period_id_paycheck,
        company_payperiods.pay_period_id as pay_period_id_payperiod,
        company_payperiods.status,
        company_payperiods.completed_at as paycycle_closed_at,
        JSON_VALUE(company_payperiods.raw_data, '$.intervalCode') as last_pay_interval,
        JSON_VALUE(company_payperiods.raw_data, '$.description') as last_pay_description,
        DATE(
          STR_TO_DATE(
            JSON_VALUE(company_payperiods.raw_data, '$.startDate'),
            '%Y-%m-%dT%H:%i:%sZ'
          )
        ) as last_pay_period_startDate,
        DATE(
          STR_TO_DATE(
            JSON_VALUE(company_payperiods.raw_data, '$.endDate'),
            '%Y-%m-%dT%H:%i:%sZ'
          )
        ) as last_pay_period_endDate,
        DATE(
          STR_TO_DATE(
            JSON_VALUE(company_payperiods.raw_data, '$.submitByDate'),
            '%Y-%m-%dT%H:%i:%sZ'
          )
        ) as last_pay_period_submitByDate
      from
        (
          SELECT
            employee_manifest_id,
            payPeriodId
          FROM
            ( -- Get the last paystub period id for employee
              SELECT
                em.id as employee_manifest_id,
                json_value(ep.additional_data, '$.payPeriodId') as payPeriodId,
                em.customer_id,
                em.employer_id,
                ROW_NUMBER() OVER (
                  PARTITION BY
                    ep.employee_manifest_id
                  ORDER BY
                    ep.pay_date DESC
                ) AS rn
              FROM
                bme.employee_paystubs ep
                INNER JOIN bme.employee_manifest em ON ep.employee_manifest_id = em.id
              WHERE
                em.employer_id = 227
                AND em.customer_id IN (
                  SELECT DISTINCT
                    customer_id
                  FROM
                    bme.agreements
                  WHERE
                    employer_id = 227
                )
            ) ranked
          WHERE
            rn = 1
        ) a
        left join employers.company_payperiods on a.payPeriodId = company_payperiods.pay_period_id
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
 

-- Overall Filters for whole query
where em.employer_id = 227 and em.customer_id is not null )

select *,

TIMESTAMPDIFF(HOUR, deduction_comparison_date,first_pay_component_deduction_sent) as hours_between_component_send_and_deduction_comparison_date,
  
case deduction_comparison_date
    when '9999-12-31'    then 'NONE'
    when last_pay_period_endDate    then 'last_pay_period_endDate'
    when paycycle_closed_at       then 'last_paycycle_closed_at'
    when last_pay_period_submitByDate then 'last_pay_period_submitByDate'
  end as date_selection_reason,

next_deduction_comparison_date + interval pay_frequency_cycle_days day as next_expected_deduction_date,

case next_deduction_comparison_date
    when '2026-01-01'    then 'NONE'
    when last_pay_period_endDate    then 'last_pay_period_endDate'
    when paycycle_closed_at       then 'last_paycycle_closed_at'
    when last_pay_period_submitByDate then 'last_pay_period_submitByDate'
  end as next_deduction_date_selection_reason,  
case 
  when first_pay_component_deduction_sent is null then 'No First Deduction Found'
  else 'First Deduction Sent'
end as first_deduction_status,
  
case 
    when connection_status = 'disconnected' then 'company disconnected'
    when employment_status = 'Terminated' then 'employee terminated'
    when is_blocked = 'blocked' then 'blocked'
    when first_pay_date_with_deduction is not null then 'at least one deduction received (paystub)'
    when paid_payroll_deduction_amt > 0 then 'at least one deduction received (ledger)'
    when first_pay_component_deduction_sent is null then 'No Pay Component Found Sent to Paychex'    
    when first_pay_date is null then 'no paystubs found for employee'
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
