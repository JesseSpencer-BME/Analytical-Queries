with
  core_model as (
    with
      base as (
        select
          *
        from
          financials.v_paychex_schedule_comparison_detail_ledger
          where transaction_type != 'initial_payment'
      ),
      last_payment as (
        select
          agreement_id,
          max(transaction_date) as last_payment_date
        from
          base
        where
          paid_amount is not null
        group by
          agreement_id
      )
    select
      b.*,
      case
        when b.transaction_date < date(sysdate()) then 'current' -- all transactions before today
        when b.transaction_date <= lp.last_payment_date then 'current' -- also include any schedules related to a payment in the future
        else 'future'
      end as past_due_timeframe
    from
      base b
      left join last_payment lp on b.agreement_id = lp.agreement_id
  ),
agreement_dates as (
  select
    agreement_id,
    min(case when l.status = 'origination' then transaction_date end) as origination_date,
    min(case when l.status = 'paid_payroll_deduction' then transaction_date end) as first_payment,
    max(case when l.status = 'paid_payroll_deduction' then transaction_date end) as last_payment,
    min(case when l.status in ('scheduled', 'paid_payroll_deduction', 'past_due') then transaction_date end) as first_schedule
  from
    bme.ledger l
    inner join bme.agreements a on l.agreement_id = a.id
  where
     l.cancelled_at is null
    and a.employer_id = 227
  group by agreement_id  
),
immutability as (
  select l.agreement_id, min(l.created_at) as ledger_creation_date, json_value(e.additional_data,'$.immutable_at') as immutable_at
  from bme.ledger l
    inner join bme.agreements a on l.agreement_id = a.id
    inner join bme.employer e on a.employer_id = e.employer_id
    where l.status = 'Origination'
  group by l.agreement_id
),
schedule_exceptions as (
    select 
      current_gaps.customer_id, 
      current_gaps.agreement_id, 
      current_gaps.magento_order_number, 
      group_concat(concat(transaction_date,' -',prior_date) separator ' | ') as compressed_schedules from (
    
    with basis as (
    select
      l.id,
      l.agreement_id,
      a.magento_order_number,
      a.customer_id,
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
    select * from analysis where date_gap <= 3
      and transaction_date <= date(sysdate())
      ) current_gaps
    group by current_gaps.agreement_id  
)
select
  c.agreement_id,
  a.customer_id,
  a.magento_order_number,
  a.status as agreement_status,  
  round(sum(c.paid_amount),2) paid_per_model,
  round(sum(c.scheduled_amt_model),2) scheduled_per_model,
  round(sum(coalesce(c.paid_amount,0)) + sum(coalesce(c.scheduled_amt_model,0)),2) as past_due_amount_per_model,
  a.past_due_amount as past_due_amount_per_agreements,
  a.balance,
  ad.origination_date,
  ad.first_payment,
  ad.last_payment,
  ad.first_schedule,
  dv.variance as deduction_variance,
  case when i.ledger_creation_date > immutable_at then 'Created After Immutability' else 'Created Before Immutability' end as immutability_flag,
  le.message as ledger_exception_message,
  se.compressed_schedules
from
  core_model c
  left join bme.agreements a on c.agreement_id = a.id
  left join agreement_dates ad on c.agreement_id = ad.agreement_id
  left join financials.v_paychex_agreement_deduction_variances dv on c.agreement_id = dv.agreement_id
  left join immutability i on a.id = i.agreement_id
  left join financials.v_ledger_exceptions le on c.agreement_id = le.agreement_id
  left join schedule_exceptions se on c.agreement_id = se.agreement_id
where
  past_due_timeframe = 'current'
group by
  agreement_id
