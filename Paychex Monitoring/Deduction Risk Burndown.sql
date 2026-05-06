with
  analysis as (
    select
      dm.*,
      first_pay_component_deduction_sent as analysis_date,
      1 as customer_count,
      purchase_total as analysis_amount,
      'Purchases' as analysis_source
    from
      financials.paychex_customer_not_deducting_monitoring dm
    where
      created_at <= '2026-04-30 21:41:37'
  
    union all
  
    select
      dm2.*,
      resolved_at as analysis_date,
      -1 as customer_count,
      - purchase_total as analysis_amount,
      'Deductions' as analysis_source
    from
      financials.paychex_customer_not_deducting_monitoring dm2
    where
      created_at <= '2026-04-30 21:41:37'
      and resolved_at is not null

    union all

    select
        dm3.*,
        next_expected_deduction_date as analysis_date,
        -1 as customer_count,
        - dm3.purchase_total as analysis_amount,
        'Expected' as analysis_source
      from
        financials.paychex_customer_not_deducting_monitoring dm3
        left join financials.v_paychex_customer_status pcs on dm3.customer_id = pcs.customer_id
      where
        created_at <= '2026-04-30 21:41:37'
        and resolved_at is not null
  )
select
  analysis.*,
  em.pay_frequency
from
  analysis
  inner join bme.employee_manifest em on analysis.customer_id = em.customer_id
