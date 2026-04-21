with base_data as(
select   
  concat(c.firstname,' ',c.lastname) as customer_name,
  a.date_created as agreement_created,
  c.employer_name as partner,
  c.employer_detail_name,
  c.salary,
  c.employment_schedule as ft_pt,
  TIMESTAMPDIFF(MONTH, c.start_date, NOW()) as tenure_months,  
  cc.clarity_score,
  cc.clear_credit_risk_score,
  cc.vantage_4_score,
  a.magento_order_number,
  a.status as agreement_status,
  c.credit_limit as spending_limit,
  c.credit_limit - balances.current_balance as customer_spending_limit_remaining,
  a.total as purchase_amount,
  las.order_total,
  las.payments,
  las.adjustments,
  las.pending,
  las.scheduled_future,
  a.term as term,
  cast(a.term as unsigned) as term_months,
  a.payments as payment_amt,
  c.first_order,
  c.total_orders-1 as number_prior_purchases,
  date(c.created_at) as registered_date,
  c.customer_score as bme_score,
  balances.current_balance as customer_current_balance,
  las.current_balance as agreement_current_balance,
  las.last_payment_received,
  a.id as agreement_id,
  c.entity_id as customer_id,
  e.scoring_model_type,
  e.go_live_date,
  csu.salary as salary_self_reported,
  csu.tenure as tenure_self_reported,
  csu.employer_name as employer_self_reported,
  initial_payments.initial_payment,
  order_sequence.cust_order_sequence,
  last_payment_received as last_payment_received_date,
  DATEDIFF(CURDATE(), last_payment_received) AS days_since_payment,
  apd.amount as past_due_amount,
  datediff(current_date(),apd.due_date) as days_past_due,
  -initial_payments.initial_payment / a.total as ip_pct,
  c.start_date as employee_start_date,
  TIMESTAMPDIFF(MONTH, c.start_date, a.date_created) as tenure_months_at_order,
  1 as total_orders,
  case when apd.amount>0 then 1 else 0 end as orders_past_due,
  c.employment_status,
  bank_accounts.bank_routings,
  bank_accounts.bank_accounts,
  cycle_days.cycle_type,
  cycle_days.cycle_max_days,
  c.deduction_due_days,
  lpd.amount as last_payment_amount,
  lpd.status as last_payment_type,
  lpd.comment as last_payment_comment
from financials.v_customer_entity_summary c
  inner join bme.agreements a on c.entity_id = a.customer_id
  left join financials.v_scoring_clarity_score cc on c.entity_id = cc.customer_id
  left join bme.employer e on c.employer_id = e.employer_id
  left join financials.customer_supplemental cs on c.entity_id = cs.customer_id
  left join bme.customer_signups csu on cs.customer_signup_id = csu.id
  left join financials.v_ledger_agreement_summary las on a.id = las.agreement_id
  left join bme.agreements_past_due apd on a.id = apd.agreement_id
  left join financials.v_ledger_last_payment_detail lpd on a.id = lpd.agreement_id

-- get agreement balance data
  left join (
    select
        customer_id,
        c.email,
        sum(current_balance) as current_balance,
        count(1)
      from
        financials.v_ledger_agreement_summary las
        inner join bme.agreements a on las.agreement_id = a.id
        inner join bme.customer_entity c on a.customer_id = c.entity_id
      group by
        customer_id  
    ) balances
on a.customer_id = balances.customer_id  

-- Get Initial Payment Data
left join (
    select agreement_id, round(sum(amount),2) as initial_payment from bme.ledger
      where status = 'initial_payment'
      and cancelled_at is null
      group by agreement_id  
  ) initial_payments
  on a.id = initial_payments.agreement_id

-- Get order sequence data
  left join (
    SELECT
        customer_id,
        id,
        ROW_NUMBER() OVER (
          PARTITION BY
            customer_id
          ORDER BY
            id
        ) AS cust_order_sequence
      FROM
        bme.agreements   
    ) order_sequence
  on a.id = order_sequence.id  

-- Get Bank Account Data
  left join (
    select 
      c.entity_id as customer_id, 
      group_concat(distinct(bank_routing_number)) as bank_routings,
      group_concat(distinct bank_account_number) as bank_accounts,
      count(1) as account_count 
    from bme.customer_entity c
      inner join bme.employee_manifest em
        on c.entity_id = em.customer_id
      inner join bme.customer_bank cb
        on em.employee_id = cb.employee_id
        and em.employer_id = cb.employer_id
    group by c.entity_id  
  ) bank_accounts
    on a.customer_id = bank_accounts.customer_id  

-- Get pay_cycle offset days
  
left join (
    select
      cycle_type,
      num_employees,
      case cycle_type
        when 'Bi-Weekly' then 14
        when 'Monthly' then 31
        when 'Semi-Monthly' then 15
        when 'Weekly' then 7
      end as cycle_max_days
    from
      (
        select
          substring(pay_cycle, 1, locate(':', pay_cycle) -1) as cycle_type,
          count(1) as num_employees
        from
          bme.employee_manifest
        where
          customer_id is not null
        group by
          substring(pay_cycle, 1, locate(':', pay_cycle) -1)
      ) base_data
  ) cycle_days 
    on substring(c.pay_cycle, 1, locate(':', c.pay_cycle) -1) = cycle_days.cycle_type
  
order by a.id desc
  ),
ip_bands as (
select base_data.*,
  case 
    when ip_pct between .095 and .105 then '10_pct'
    when ip_pct between .245 and .255 then '25_pct'
    when ip_pct between .495 and .505 then '50_pct'
  end as ip_band
  from base_data
), bands as(
select ip_bands.*,
  case 
    when ip_band is null then 'fast_track'
    else ip_band 
  end as fast_track_status,
  case 
    when tenure_months_at_order between 0 and 6 then '0-6'
    when tenure_months_at_order between 7 and 9 then '7-9'
    when tenure_months_at_order between 10 and 12 then '10-12'
    else '12+' end as tenure_band_at_order,
  case
    when scoring_model_type = 'paystub_model' then 'paystub_model'
    when clear_credit_risk_score is null then 'base_model - no score required'
    when clear_credit_risk_score <= 500 then '0-500'
    when clear_credit_risk_score <= 550 then '501-550'
    when clear_credit_risk_score <= 575 then '551-575'
    when clear_credit_risk_score <= 600 then '576-600'
    when clear_credit_risk_score > 600 then '600+'
  end as clear_credit_risk_band,
  case
    when agreement_created > date_sub(curdate(), INTERVAL 15 DAY) then 'agreement within last 15 days'
    when days_since_payment > 120 then '121+'
    when days_since_payment > 90 then '91-120'
    when days_since_payment > 60 then '61-90'
    when days_since_payment > 30 then '31-60'
    when days_since_payment > 15 then '15-30'
    when days_since_payment > 0 then '0-30'
    else 'no payments made'
  end as days_since_payment_band,
  cycle_max_days + deduction_due_days as max_agreement_duration,
  date_add(agreement_created, interval (cycle_max_days + deduction_due_days) day) as agreement_latest_start,
  date_add(date_add(agreement_created, interval (cycle_max_days + deduction_due_days) day), interval term_months month) as agreement_latest_end
from ip_bands
  ), all_data as(
select bands.*,
  case 
    when agreement_latest_end < date(financials.cst(sysdate()))
      then 'Agreement Period Complete'
    else 'Agreement Period Still Open'
  end as agreement_period_evaluation,
  CASE
    WHEN agreement_latest_end <= agreement_created THEN NULL
    WHEN DATE(financials.cst(SYSDATE())) >= agreement_latest_end THEN 1
    WHEN DATE(financials.cst(SYSDATE())) <= agreement_created THEN 0
    ELSE
        DATEDIFF(
            DATE(financials.cst(SYSDATE())),
            agreement_created
        ) /
        DATEDIFF(
            agreement_latest_end,
            agreement_created
        )
END AS pct_agreement_duration_elapsed,
  -(payments+adjustments) / order_total as pct_order_paid_off,
  case 
    when go_live_date < (date(sysdate()) - interval 30 day) then 'employer go-live within last 30 days'
    else 'employer go-live over 30 days ago'
  end as employer_go_live_category
 from bands)
select all_data.*,
  case 
    when employment_status = 'terminated' then agreement_current_balance
    when agreement_period_evaluation = 'Agreement Period Complete' then agreement_current_balance
    when days_since_payment > (cycle_max_days*2) then agreement_current_balance
  end as past_due_amount_adjusted,
  case 
    when employment_status = 'terminated' then 'terminated employee'
    when agreement_period_evaluation = 'Agreement Period Complete' then 'Agreement Period Complete'
    when days_since_payment > (cycle_max_days*2) then 'No payment in last two pay cycles'
  end as past_due_amount_adjusted_reason
  
  
  from all_data
