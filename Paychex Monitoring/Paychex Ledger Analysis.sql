select
  l.*,
  concat_ws(' ', first_name, last_name) as employee_name,
  em.employment_status,
  em.location as sub_employer_name,
  e.name as employer_name,
  next_schedule.next_scheduled_date,
  a.magento_order_number,
  lsa.ledger_stage,
  lsa.include_in_balance_calculation,
  lsa.ledger_stage_category
from
  bme.ledger l
  left join bme.agreements a on l.agreement_id = a.id
  left join bme.customer_entity c on a.customer_id = c.entity_id
  left join bme.employee_manifest em on c.entity_id = em.customer_id
  left join bme.employer e on c.employer_id = e.employer_id
  left join financials.v_ledger_status_attributes lsa on l.status = lsa.status

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
