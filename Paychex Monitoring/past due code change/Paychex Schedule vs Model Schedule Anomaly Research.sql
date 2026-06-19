with schedule_adjustments as (

with base as (
  select  
    l.agreement_id,
    a.customer_id,
    a.status as agreement_status,
    round(sum(l.amount),2) as ledger_total,
    round(sum(a.payments),2) as scheduled_payment_total,
    a.payments as periodic_schedule_amount,
    group_concat(l.id) as ledger_ids,
    group_concat(concat(l.id,'-',l.amount) separator ' | ' ) as ledger_details
  from
    bme.ledger l
    inner join bme.agreements a on l.agreement_id = a.id
  where
    a.employer_id = 227
    and l.status = 'scheduled'
    and l.transaction_date < date(sysdate())
    and l.cancelled_at is null
    and a.payments - amount > 1
    and a.status not in ('Paid Off','Cancelled')
    
  group by l.agreement_id
)
select b.*,
  round(b.ledger_total / b.scheduled_payment_total,4) as pct_of_schedule,
  b.scheduled_payment_total - b.ledger_total as variance
  from base b
  order by  pct_of_schedule asc
), 
#-------------------------------------------
returns as (
  select agreement_id, round(sum(amount),2) as return_amount
    from bme.ledger
    where status = 'return'
    and cancelled_at is null
  group by agreement_id
  ),
#-------------------------------------------
ledger_less_than_schedule as (
    with sched_vars as (
    select  
      l.id as ledger_id,
      l.agreement_id,
      l.amount as ledger_amount,
      a.payments as update_ledger_to_amount
    from
      bme.ledger l
      inner join bme.agreements a on l.agreement_id = a.id
    where
      a.employer_id = 227
      and l.status = 'scheduled'
      and l.transaction_date < date(sysdate())
      and l.cancelled_at is null
      and a.payments - amount > 1
    )
    select agreement_id, sum(ledger_amount), sum(update_ledger_to_amount), sum(update_ledger_to_amount) - sum(ledger_amount) as ledger_less_than_schedule_update_amt
    from sched_vars
    group by agreement_id
),
#-------------------------------------------
schedule_gaps as (
  with basis as (
  select
          l.id,
          l.agreement_id,
          l.transaction_date,
          lag(
            transaction_date) over
            (partition by
              agreement_id
            order by
              transaction_date asc
          ) as prior_date
        from
          bme.ledger l
          inner join bme.agreements a on l.agreement_id = a.id
        where
          l.cancelled_at is null
          and l.status = 'scheduled'
          and a.employer_id = 227
    ), date_gaps as (
    select b.*, datediff(transaction_date,prior_date) as date_gap from basis b
    ), paycycle_anomalies as (
  select dg.*, approx_pay_cycle_days from date_gaps dg
  inner join bme.agreements a on dg.agreement_id = a.id
  inner join bme.employee_manifest em on a.customer_id = em.customer_id
  left join pay_frequency_terms pft on a.term = pft.term and em.pay_frequency = pft.pay_frequency
  where date_gap > (approx_pay_cycle_days + 3)
    and transaction_date < date(sysdate())
    )
  select agreement_id, group_concat(concat(prior_date,'-',transaction_date,' (',date_gap,')') separator ' | ') as schedule_gap_list from paycycle_anomalies
  group by agreement_id
  ),
#-------------------------------------------
duplicated_schedules as (

  select 
        current_gaps.link,
        current_gaps.customer_id, 
        current_gaps.agreement_id,
        current_gaps.magento_order_number, 
        group_concat(concat(transaction_date,' -',prior_date) separator ' | ') as compressed_schedules,
    count(1) as num_schedules
    from (
      
      with basis as (
      select
        l.id,
        l.agreement_id,
        a.magento_order_number,
        a.customer_id,
        concat('https://ledgers.corp.benefitsme.com/customer/',a.customer_id,'/ledgers?agreementId=',a.id) as url,
        l.transaction_date,
        lag(
          transaction_date) over
          (partition by
            agreement_id
          order by
            transaction_date asc
        ) as prior_date
      from
        bme.ledger l
        inner join bme.agreements a on l.agreement_id = a.id
      where
        l.cancelled_at is null
        and l.status = 'scheduled'
        and a.employer_id = 227
      ), analysis as (
      select b.*, datediff(transaction_date,prior_date) as date_gap from basis b  
      )
      select 
          CONCAT('<a href="', url, '">', 'link', '</a>') as link,
          analysis.* from analysis where date_gap <= 3
        and transaction_date <= date(sysdate())
        ) current_gaps
      group by current_gaps.agreement_id  
), 
#-------------------------------------------  
analysis as (
select 
  s.*, 
  sa.variance as variance_fixed_by_ledgers,
  case when sa.agreement_id is not null then 'Has Ledger amounts too low' else 'No ledger amounts too low' end as has_schedule_amount_too_low,
  schedule_amt_variance - sa.variance as variance_remaining_after_ledger_corrections ,
  case when abs((schedule_amt_variance - sa.variance)) < 1 then 'Ledger Correction Fixes all Variance' else 'Ledger Correction does NOT fix all Variance' end as fixes_entire_variance,
  case when ds.compressed_schedules is not null then 'has duplicated schedules' else 'no duplicated schedules' end as has_duplicated_schedules,
  ds.compressed_schedules,
  em.pay_frequency as pay_frequency_current,
  ol.pay_frequency as pay_frequency_at_order,
  case when em.pay_frequency = ol.pay_frequency then 'Pay frequencies Match' else "Pay frequencies don't match" end as pay_frequency_matches,
  case when r.return_amount is not null then 'Return Made' else "No return Made" end as has_return,
  so.discount_amount,
  case when abs(so.discount_amount) > 0 then 'Had Discount' else 'No Discount' end as has_discount,
  spv.payments_per_agreement,
  spv.calc_payment_after_ip,
  spv.variance as scheduled_payment_variance,
  case when spv.variance is not null then 'Has Scheduled Payment Variance' else 'No Schedule Payment Variance' end as has_schedule_payment_variance,
  sg.schedule_gap_list,
  case when schedule_gap_list is not null then 'Has missing schedule gap' else 'No missing pay schedules' end as has_schedule_gap,
  lls.ledger_less_than_schedule_update_amt,
  case when lls.ledger_less_than_schedule_update_amt is not null then 'Schedule Amount too Low - Update Required' else 'Schedule Amounts not too low' end as ledger_schedules_less_than_expected
  
from financials.v_paychex_schedule_audit_v_ledger_schedule s
left join schedule_adjustments sa on s.agreement_id = sa.agreement_id
left join duplicated_schedules ds on s.agreement_id = ds.agreement_id
left join bme.agreements a on s.agreement_id = a.id
left join bme.employee_manifest em on a.customer_id = em.customer_id
left join bme.employee_manifest_order_log ol on a.order_id = ol.order_id
left join returns r on s.agreement_id = r.agreement_id
left join bme.sales_order so on a.order_id = so.entity_id
left join financials.v_agreement_scheduled_payment_variances spv on s.agreement_id = spv.agreement_id
left join schedule_gaps sg on s.agreement_id = sg.agreement_id
left join ledger_less_than_schedule lls on s.agreement_id = lls.agreement_id
)
#-------------------------------------------
select a.*,
case 
  when schedule_alignment_assessment = 'Schedules Agree' then 'Schedules Agree'
  when agreement_open_status = 'agreement_paid_off' then 'Agreement is fully paid off'
  when has_discount = "Had Discount" then "Agreement had discount"
  when has_schedule_payment_variance = 'Has Scheduled Payment Variance' then 'Has scheduled payment variance'
  when pay_frequency_matches = "Pay frequencies don't match" then "Pay frequencies don't match"
  when has_return = "Return Made" then "Agreement had return"  
  when has_schedule_gap = 'Has missing schedule gap' then 'Has missing schedule gap'  
  else 'Schedule mismatch - no known error'
end as primary_error 
  from analysis a
