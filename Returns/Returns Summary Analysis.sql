with core as (
select
  soi.order_id,
  soi.item_id,
  soi.product_id,
  soi.sku,
  soi.name as product_name,
  soi.qty_ordered,  
  soi.qty_refunded,
  soi.qty_shipped,
  soi.base_row_total,
  soi.created_at as sales_order_item_created,
  soi.amount_refunded,
  soi.fulfillment_cost,
  soi.fulfillment_shipping_cost,
  coalesce(soi.fulfillment_vendor,'unknown') as fulfillment_vendor,
  a.id as agreement_id,
  a.magento_order_number,
  a.status as agreement_status,
  a.sale_date,
  e.name as employer_name,
  em.employment_status,
  ed.status as disconnect_status,
  em.start_date,
  em.salary,
  cs.gender,
  cs.is_fast_track,
  cs.scoring_points_bme,
  c.week_start_date,
  cm.created_at as returned_date_approximate,
  case when cm.created_at >= a.sale_date then datediff(cm.created_at,a.sale_date) end as days_purchase_to_return,
  soiph.category_l2,
  soiph.category_l3,
  soiph.category_l4,
  soiph.category_l5,
  soiph.category_l6
from  
  bme.sales_order_item soi   
  inner join bme.sales_order so on soi.order_id = so.entity_id
  inner join bme.agreements a on so.entity_id = a.order_id
  inner join bme.employer e on a.employer_id = e.employer_id
  inner join bme.employee_manifest em on a.customer_id = em.customer_id
  left join financials.v_sales_order_item_product_hierarchy soiph on soiph.item_id = soi.item_id
  left join financials.customer_supplemental cs on a.customer_id = cs.customer_id
  left join bme.employer_department ed on em.company_code = ed.department_prefix and em.employer_id = 227
  left join financials.v_calendar c on date(soi.created_at) = c.calendar_date
  left join bme.sales_creditmemo cm on so.entity_id = cm.order_id
  )
select core.*,
case when qty_refunded > 0 then 'has_return' else 'no_return' end as has_return,
  case when qty_shipped > 0 then 'has_shipped' else 'no_shipment' end as has_shipped
from core
-- select order_id, count(distinct item_id) from core
-- where qty_refunded > 0
-- group by order_id
-- having count(distinct item_id) > 1
