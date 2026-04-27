select
  id,
  employer_id,
  company_id,
  pay_period_id, -- JSON_VALUE(raw_data, '$.payPeriodId') AS pay_period_id,
  check_date,
  JSON_VALUE(raw_data, '$.intervalCode') AS interval_code,
  status, -- JSON_VALUE(raw_data, '$.status') AS status,  
  JSON_VALUE(raw_data, '$.description') AS description,
  CAST(JSON_VALUE(raw_data, '$.startDate') AS DATETIME) AS start_date,
  CAST(JSON_VALUE(raw_data, '$.endDate') AS DATETIME) AS end_date,
  CAST(
    JSON_VALUE(raw_data, '$.submitByDate') AS DATETIME
  ) AS submit_by_date,
  -- CAST(JSON_VALUE(raw_data, '$.checkDate') AS DATETIME) AS check_date,
  num_checks, -- CAST(JSON_VALUE(raw_data, '$.checkCount') AS UNSIGNED) AS check_count,
  cpp.*
from
  employers.company_payperiods cpp
