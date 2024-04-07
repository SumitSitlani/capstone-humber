CREATE EXTERNAL TABLE IF NOT EXISTS RDS_data (
  source_account STRING,
  arn STRING,
  source_region STRING,
  instance_name STRING,
  engine STRING,
  engine_version STRING,
  instance_class STRING,
  storage_type STRING,
  primary_owner STRING,
  secondary_owner STRING,
  support_group STRING,
  cost_centre STRING,
  major_version STRING,
  minor_version STRING,
  maintenance_version STRING,
  eol_date STRING,
  days_until_eol STRING,
  years_months_until_eol STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://test-bucket-capstone/RDS/'
TBLPROPERTIES ('skip.header.line.count'='1');
