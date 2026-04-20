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
  c.credit_limit - balances.current_balance as spending_limit_remaining,
  a.total as purchase_amount, 
  a.term as term,
  a.payments as payment_amt,
  c.first_order,
  c.total_orders-1 as number_prior_purchases,
  date(c.created_at) as registered_date,
  c.customer_score as bme_score,
  balances.current_balance,
  las.current_balance as agreement_current_balance,
  las.last_payment_received,
  a.id as agreement_id,
  c.entity_id as customer_id,
  e.scoring_model_type,
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
  bank_accounts.bank_accounts
from financials.v_customer_entity_summary c
  inner join bme.agreements a on c.entity_id = a.customer_id
  left join financials.v_scoring_clarity_score cc on c.entity_id = cc.customer_id
  left join bme.employer e on c.employer_id = e.employer_id
  left join financials.customer_supplemental cs on c.entity_id = cs.customer_id
  left join bme.customer_signups csu on cs.customer_signup_id = csu.id
  left join financials.v_ledger_agreement_summary las on a.id = las.agreement_id
  left join bme.agreements_past_due apd on a.id = apd.agreement_id
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
  
left join (
    select agreement_id, round(sum(amount),2) as initial_payment from bme.ledger
      where status = 'initial_payment'
      and cancelled_at is null
      group by agreement_id  
  ) initial_payments
  on a.id = initial_payments.agreement_id

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
)
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
  end as clear_credit_risk_band  
from ip_bands
