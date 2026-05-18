with disconnected_paychex_employees as ( -- Disconnected paychex employees

select -- population of current employees who are currently under paychex, and not disconnected
  em.customer_id
from
  bme.employee_manifest em
  left join bme.employer_department ed on em.company_code = ed.department_prefix
where
  ed.status = 'disconnected'
  and em.customer_id is not null
  and ed.employer_id = 227

union
  
select -- Population created in Paychex, who are no longer under Paychex
  entity_id as customer_id
from
  bme.customer_entity
where
  created_in = 'Paychex'
  and employer_id != 227

  )

select 
  dpe.*, 
  c.employer_id,
  case when (c.created_in = 'Paychex' and c.employer_id =204) then 'Successful Conversion' end as conversion_status,
  case when num_agreements > 0 then 'Had Agreements' else 'No Agreements' end as has_agreements,
  pinwheel_data.*,
  a.*  
from disconnected_paychex_employees dpe
left join bme.customer_entity c on dpe.customer_id = c.entity_id

left join (
select 
  customer_id, 
  sum(balance) as balance, 
  count(1) as num_agreements, 
  sum(total) as purchase_total, 
  sum(past_due_amount) as past_due_amount
  from bme.agreements
  group by customer_id  
) a on dpe.customer_id = a.customer_id 


left join (
    with all_log_data as (
    select
        s.customer_id,
        s.id as session_identifier,
        s.session_id,
        s.ip_address,
        s.user_agent,
        s.referrer,
        s.created_at as session_created_at,
        s.data as session_data,
        s.workflow,
        r.id as request_id,
        r.created_at as request_created_at,
        r.method,
        r.path,
        r.query,
        ed.status,
        ul.message,
        ul.data,
        case when message = 'Pinwheel:open' then 1 end as pinwheel_opened,
        case when message = 'Pinwheel:select_platform' then 1 end as pinwheel_selected_platform,
        case when message = 'Pinwheel:login_attempt' then 1 end as pinwheel_attempted_login,
        case 
          when message = 'Pinwheel transition: Other' then 'Other'
          when message = 'Pinwheel transition: Paychex' then 'Paychex'
        end as new_payroll_provider,
        case when message = 'Pinwheel:error' then 1 end pinwheel_error,
        case when message = 'Pinwheel:success' then 1 end as pinwheel_succeeded,
        case when message = 'Pinwheel:success' then ul.created_at end as pinwheel_succeeded_at
      from
        account.sessions s
        inner join account.requests r on s.id = r.session_id
        left join bme.employee_manifest em on s.customer_id = em.customer_id
        left join bme.employer_department ed on em.company_code = ed.department_prefix
        left join account.user_log ul on s.session_id = ul.session_id
      where
        path = '/pinwheel/transition'      
    )
  select
    customer_id,
    'Hit Landing Page' as hit_landing_page,    
    count(distinct session_identifier) as sessions_attempted,
    case when sum(pinwheel_opened) > 1 then 'PinWheel Opened' end as pinwheel_opened,
    group_concat(distinct new_payroll_provider) as new_payroll_provider,
    case when sum(pinwheel_selected_platform) > 1 then 'Platform Selected' end as pinwheel_selected_platform,
    case when sum(pinwheel_attempted_login) > 1 then 'Attempted Login' end as pinwheel_attempted_login,
    case when sum(pinwheel_error) > 1 then 'Pinwheel Error' end as pinwheel_error,
    case when sum(pinwheel_succeeded) > 1 then 'Pinwheel Succeeded' end as pinwheel_succeeded,
    min(pinwheel_succeeded_at) as pinwheel_succeeded_at
    from all_log_data
  group by customer_id
  ) pinwheel_data on dpe.customer_id = pinwheel_data.customer_id
