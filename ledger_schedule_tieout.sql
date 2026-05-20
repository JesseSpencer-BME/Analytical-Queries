with
  core as (
    select
      c.entity_id as customer_id,
      em.id as employee_manifest_id,
      lp.last_pay_date,
      lp.last_pay_date + interval cd.pay_frequency_cycle_days_w_buffer day as last_pay_cutoff,
      c.missing_last_paystub
    from
      bme.customer_entity c
      inner join bme.employee_manifest em on c.entity_id = em.customer_id
      inner join bme.employer_department ed on em.company_code = ed.department_prefix
      left join financials.v_pay_frequency_cycle_days cd on em.pay_frequency = cd.pay_frequency
      left join (
        select
          employee_manifest_id,
          max(pay_date) as last_pay_date
        from
          bme.employee_paystubs
        group by
          employee_manifest_id
      ) lp on em.id = lp.employee_manifest_id
    where
      c.employer_id = 227
      and em.employment_status != 'terminated'
      and ed.status != 'disconnected'
  )
select
  *
from
  core
where
  coalesce(last_pay_cutoff,'2026-01-01') < date(sysdate()) -- If no "last pay cutoff", default to the beginning of the year
  and coalesce(missing_last_paystub, 0) != 1
