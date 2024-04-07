CREATE EXTERNAL TABLE IF NOT EXISTS Glue_data (
  source_account STRING,
  arn STRING,
  source_region STRING,
  resource_name STRING,
  glue_version STRING,
  max_capacity STRING,
  max_retries STRING,
  num_of_workers STRING,
  timeout STRING,
  worker_type STRING,
  python_version STRING,
  primary_owner STRING,
  secondary_owner STRING,
  support_group STRING,
  cost_centre STRING,
  eol_date STRING,
  days_until_eol STRING,
  years_months_until_eol STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://test-bucket-capstone/Glue/'
TBLPROPERTIES ('skip.header.line.count'='1');