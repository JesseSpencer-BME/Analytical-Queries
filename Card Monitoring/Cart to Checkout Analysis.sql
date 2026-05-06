-- call financials.sp_update_card_history();

with core as (

select
  q.entity_id as quote_id,
  q.created_at as quote_created,
  q.customer_id,
  qi.cart_total,
  so.entity_id as sales_order_id,
  so.subtotal_incl_tax,
  so.created_at as sales_order_created,
  checkout_attempts.first_checkout,
  checkout_attempts.last_checkout,
  cards_attempted,
  card_approved,
  approval_vendor,
  card_errors,
  attempted_vendor,
  errors_encountered,
  NSF_status,
  NSF_Encountered
from
  bme.quote q
  left join bme.sales_order so on q.entity_id = so.quote_id
  
  -- quote detail totals (the quote itself doesn't calculate the right total)
  left join (
    select
      quote_id,
      sum(row_total) as cart_total
    from
      bme.quote_item
    group by
      quote_id
  ) qi on q.entity_id = qi.quote_id
  
  -- get checkout attempt data
  left join 
  (
    select
      s.customer_id,  
      min(r.created_at) as first_checkout,
      max(r.created_at) as last_checkout  
    from
      account.requests r
      inner join account.sessions s on r.session_id = s.id
    where
      r.created_at >= '2026-05-04' -- sysdate() - interval 24 hour
      and path = '/checkout'
      and customer_id is not null
    group by s.customer_id
  ) checkout_attempts on q.customer_id = checkout_attempts.customer_id

  
  --  get the card attempt data
  left join (
  with
  card_data as (
    select
      cah.*,
      concat(
        json_value(psc.additional, '$.cc_type'),
        '*',
        json_value(psc.additional, '$.cc_last4'),
        '(',gateway,')'
      ) as card_number
    from
      financials.card_attempt_history cah
      left join bme.paradoxlabs_stored_card psc on cah.card_id = psc.id
    where
      cah.attempt_date >= '2026-05-04'
  ),
  by_card as (
    select
      customer_id,
      card_number,
      group_concat(distinct error_message) as error_message,
      group_concat(distinct case when error_message = 'Approved' then card_number end) as approval_card,
      sum(distinct case when error_message = 'Insufficient funds' then 1 end) as NSF_Encountered,
      sum(case when error_message = 'approved' then 1 end) as card_approved,
      group_concat(distinct case when error_message = 'Approved' then gateway end) as approval_vendor,
      group_concat(distinct gateway) as attempted_vendor,
      count(distinct card_id) as cards_attempted,
      sum(case when error_message != 'approved' then 1 end) as error_count
    from
      card_data
    group by
      customer_id,
      card_number
  )
  select
    customer_id,  
    group_concat(concat(card_number,': ', error_message) SEPARATOR ' | ') as card_errors,
    group_concat(distinct approval_card) as approved_card,
    group_concat(distinct approval_vendor) as approval_vendor,
    case when sum(card_approved) > 0 then 'Yes' end as card_approved,
    case when sum(NSF_Encountered) > 0 then 'Insufficient Funds' end as NSF_status,
    group_concat(distinct attempted_vendor) as attempted_vendor,
    sum(cards_attempted) as cards_attempted,
    case when sum(error_count) > 0 then 'Errors Encountered' else 'No Errors Encountered' end as errors_encountered,
    sum(NSF_Encountered) as NSF_Encountered
  from
    by_card
  group by
    customer_id  
  ) card_attempt on q.customer_id = card_attempt.customer_id

-- Overall Filters
where
  q.created_at >= '2026-05-04' -- sysdate() - interval 24 hour
  and q.customer_id is not null
  and q.is_active = 1

)
select core.*,
  concat_ws(' ',em.first_name,em.last_name),
  c.email,
  e.name as employer_name,
  em.location as sub_employer_name,
  cs.total_orders,
  cs.total_order_dollars,
  cs.first_order,
  case 
    when sales_order_id is not null and first_checkout is null then 'Checked Out - but no Session Log?'
    when first_checkout is null then 'Have not checked out yet'  
    when card_approved = 'Yes' and errors_encountered = 'No Errors Encountered' then 'Checkout Successful, card approved, no errors'
    when card_approved = 'Yes' then 'Checkout Successful, card approved - but errors encountered'
    when cards_attempted > 0 then 'Card Attempted - failed to checkout'
    when cards_attempted is null then 'Reached Check-Out, but no Card Attempt'
    else 'Unknown'
  end as checkout_status
  
  from core
    left join bme.employee_manifest em on core.customer_id = em.customer_id
    left join bme.customer_entity c on em.customer_id = c.entity_id
    left join bme.employer e on em.employer_id = e.employer_id
    left join financials.v_customer_entity_summary cs on core.customer_id = cs.entity_id
