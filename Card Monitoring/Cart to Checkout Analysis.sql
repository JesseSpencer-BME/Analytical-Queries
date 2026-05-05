-- call financials.sp_update_card_history();

with core as (

select
  q.entity_id as quote_id,
  q.created_at as quote_created,
  q.customer_id,
  qi.cart_total,
  so.entity_id as sales_order_id,
  so.subtotal_incl_tax,
  checkout_attempts.first_checkout,
  checkout_attempts.last_checkout,
  cards_attempted,
  case when card_approved > 0 then 'Yes' end as card_approved,
  approval_vendor,
  card_errors,
  services_attempted  
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
    select
      customer_id,
      count(distinct card_id) as cards_attempted,
      sum(case when error_message = 'Approved' then 1 end) as card_approved,
      group_concat(distinct error_message) card_errors,
      group_concat(distinct gateway) as services_attempted,
      group_concat(distinct case when error_message = 'Approved' then gateway end) as approval_vendor
    from
      financials.card_attempt_history
    where
      attempt_date >= '2026-05-04'
      and amount = .01
    group by customer_id
  ) card_attempt on q.customer_id = card_attempt.customer_id

-- Overall Filters
where
  q.created_at >= '2026-05-04' -- sysdate() - interval 24 hour
  and q.customer_id is not null

)
select core.*,
  case 
    when sales_order_id is not null and first_checkout is null then 'Checked Out - but no Session Log?'
    when first_checkout is null then 'Have not checked out yet'
    when card_approved = 'Yes' and card_errors = 'approved' then 'Checkout Successful, card approved, no errors'
    when card_approved = 'Yes' then 'Checkout Successful, card approved - but errors encountered'
    when cards_attempted > 0 then 'Card Attempted - failed to checkout'
    when cards_attempted is null then 'Reached Check-Out, but no Card Attempt'
    else 'Unknown'
  end as checkout_status
  
  from core;
