with
  sso as (
    select distinct
      email
    from
      employers.sso_login_attempts sso
    where
      employer_id = 227
  )
select
  sso.email,
  em.id,
  case when em.id is not null then 'Employee Created' else 'Employee Not Created' end as has_employee,
  case when em.customer_id is not null then 'Customer Created' else 'Customer Not Created' end as has_customer,
  case when c.credit_limit >0 then 'Has Credit Limit' else 'No Credit Limit' end as has_credit_limit,  
  case when carts.customer_id is not null then 'Cart Created' else 'Cart Not Created' end as cart_created,  
  case when orders.customer_id is not null then 'Order Created' else 'No Order Created' end as order_created,
  em.customer_id,
  cs.ft_fail_reason,
  case when c.credit_limit >0 then 'credit granted' else ft_fail_reason_category end as credit_limit_details,
  cs.fast_track,
  c.credit_limit
from
  sso
  left join financials.v_paychex_employee_manifest em on sso.email = em.email
  left join bme.customer_entity c on em.customer_id = c.entity_id
  left join financials.v_paychex_customer_scoring cs on em.customer_id = cs.entity_id
  left join (
  select customer_id, count(1) as open_carts, sum(items_qty) as cart_items, sum(subtotal) as cart_subtotal from bme.quote q
inner join bme.customer_entity c on q.customer_id = c.entity_id
where c.employer_id = 227
group by customer_id
) carts
  on c.entity_id = carts.customer_id  
left join (
  select customer_id, sum(total_invoiced) as sales_dollars, sum(total_qty_ordered) as total_ordered_qty, count(1) as total_orders from bme.sales_order so
  where employer_id = 227
group by customer_id
  ) orders 
  on c.entity_id = orders.customer_id
