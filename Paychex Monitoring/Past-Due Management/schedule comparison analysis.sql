with
  payments as (
    select
      l.agreement_id,
      date(created_at) as effective_date,
      transaction_date as transaction_date,
      l.status as transaction_type,
      l.amount,
      'payments' as analysis_source
    from
      bme.ledger l
      inner join financials.ledger_status_attributes lsa on l.status = lsa.status
      inner join bme.agreements a on l.agreement_id = a.id
    where
      lsa.count_as_payment = 'Yes'
      and l.cancelled_at is null
      and a.employer_id = 227
  ),
  schedules as (
    select
      agreement_id,
      schedule_date as effective_date,
      schedule_date as transaction_date,
      'schedule_model' as transaction_type,
      amount,
      'schedule_model' as analysis_source
    from
      financials.v_paychex_schedule_audit
  ),
  ledger_schedule as (
    select
      l.agreement_id,
      l.transaction_date as effective_date,
      l.transaction_date as transaction_date,
      l.status as transaction_type,
      -l.amount,
      'schedule_ledger' as analysis_source
    from
      bme.ledger l
      inner join bme.agreements a on l.agreement_id = a.id
      inner join financials.ledger_status_attributes lsa on l.status = lsa.status
    where
      l.cancelled_at is null
      and lsa.include_in_schedule_calculation = 'Yes'
      and a.employer_id = 227
      and a.status != 'cancelled'
  ),  
  
  all_transactions as (
    select * from payments
    union all
    select * from schedules
    union all
    select * from ledger_schedule
  ),
  agreements as (
    select
      `a`.`customer_id` AS `customer_id`,
      `a`.`id` AS `agreement_id`,
      a.magento_order_number,
      `a`.`status` as `agreement_status`,
      `a`.`term` AS `term`,
      `a`.`date_created` AS `date_created`,
      `em`.`id` AS `employee_manifest_id`,
      `em`.`pay_frequency` AS `pay_frequency`,
      `a`.`total` AS `total_amount`,
      a.past_due_amount as agreement_past_due_amt,
      (
        select
          min(`bme`.`employee_paystubs`.`pay_date`)
        from
          `bme`.`employee_paystubs`
        where
          `bme`.`employee_paystubs`.`employee_manifest_id` = `em`.`id`
          and `bme`.`employee_paystubs`.`pay_date` > `a`.`date_created`
      ) AS `first_pay_date`,
      (
        select
          max(`bme`.`employee_paystubs`.`pay_date`)
        from
          `bme`.`employee_paystubs`
        where
          `bme`.`employee_paystubs`.`employee_manifest_id` = `em`.`id`
          and `bme`.`employee_paystubs`.`pay_date` <= `a`.`date_created`
      ) AS `most_recent_pay_date`
    from
      (
        `bme`.`agreements` `a`
        join `bme`.`employee_manifest` `em` on (`a`.`customer_id` = `em`.`customer_id`)
      )
    where
      `a`.`employer_id` = 227
  ),
customer_totals as (
  select customer_id, sum(past_due_amount) as total_customer_past_due
  from bme.agreements
  where employer_id = 227
  group by customer_id  
  )
select
  *,
  case
    when date(effective_date) <= date(sysdate()) then 'current_transactions'
    else 'future_transactions'
  end as transaction_timeframe,
  case when analysis_source = 'schedule_model' then amount end as scheduled_amt_model,
  case when analysis_source = 'schedule_ledger' then amount end as scheduled_amt_ledger,
  case when analysis_source = 'payments' then amount end as paid_amount
from
  all_transactions t
  left join agreements a on t.agreement_id = a.agreement_id
  left join customer_totals ct on a.customer_id = ct.customer_id
