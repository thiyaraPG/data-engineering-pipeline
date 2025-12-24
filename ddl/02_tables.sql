-- FACT: SALES 
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.fact_sales` (
  sale_id STRING NOT NULL,
  sale_date DATE NOT NULL,
  region STRING,
  country STRING,
  product STRING,
  quantity INT64,
  currency_code STRING,
  unit_price_usd NUMERIC,
  total_sales_usd NUMERIC,
  load_ts TIMESTAMP NOT NULL
);

-- FACT: FINANCIALS 
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.fact_financial` (
  transaction_id STRING NOT NULL,
  txn_date DATE NOT NULL,
  region STRING,
  country STRING,
  product STRING,
  currency_code STRING,
  revenue_usd NUMERIC,
  expense_usd NUMERIC,
  profit_usd NUMERIC,
  load_ts TIMESTAMP NOT NULL
);

-- FACT: ATTENDANCE
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.fact_attendance` (
  staff_id STRING NOT NULL,
  att_date DATE NOT NULL,
  region STRING,
  country STRING,
  department STRING,
  status STRING,
  check_in TIME,
  check_out TIME,
  load_ts TIMESTAMP NOT NULL
);

-- STAGING TABLES (used for idempotent MERGE)
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.stg_sales`
AS SELECT * FROM `${project}.${dataset}.fact_sales` WHERE FALSE;

CREATE TABLE IF NOT EXISTS `${project}.${dataset}.stg_financial`
AS SELECT * FROM `${project}.${dataset}.fact_financial` WHERE FALSE;

CREATE TABLE IF NOT EXISTS `${project}.${dataset}.stg_attendance`
AS SELECT * FROM `${project}.${dataset}.fact_attendance` WHERE FALSE;
