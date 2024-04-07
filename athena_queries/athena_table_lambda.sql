CREATE EXTERNAL TABLE IF NOT EXISTS Lambda_data (
  source_account STRING,
  arn STRING,
  source_region STRING,
  function_name STRING,
  runtime STRING,
  architecture STRING,
  memory_size INT,
  timeout INT,
  primary_owner STRING,
  secondary_owner STRING,
  support_group STRING,
  cost_centre STRING,
  version STRING,
  eol_date STRING,
  days_until_eol STRING,
  years_months_until_eol STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://test-bucket-capstone/Lambda/'
TBLPROPERTIES ('skip.header.line.count'='1');