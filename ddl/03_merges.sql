-- SALES UPSERT
MERGE `${project}.${dataset}.fact_sales` T
USING UNNEST(@sales_rows) S
ON T.sale_id = S.sale_id
WHEN MATCHED THEN UPDATE SET
  sale_date = S.sale_date,
  region = S.region,
  country = S.country,
  product = S.product,
  quantity = S.quantity,
  currency_code = S.currency_code,
  unit_price_usd = S.unit_price_usd,
  total_sales_usd = S.total_sales_usd,
  load_ts = S.load_ts
WHEN NOT MATCHED THEN INSERT ROW;

-- FINANCIAL UPSERT
MERGE `${project}.${dataset}.fact_financial` T
USING UNNEST(@financial_rows) S
ON T.transaction_id = S.transaction_id
WHEN MATCHED THEN UPDATE SET
  txn_date = S.txn_date,
  region = S.region,
  country = S.country,
  product = S.product,
  currency_code = S.currency_code,
  revenue_usd = S.revenue_usd,
  expense_usd = S.expense_usd,
  profit_usd = S.profit_usd,
  load_ts = S.load_ts
WHEN NOT MATCHED THEN INSERT ROW;

-- ATTENDANCE UPSERT
MERGE `${project}.${dataset}.fact_attendance` T
USING UNNEST(@attendance_rows) S
ON T.staff_id = S.staff_id AND T.att_date = S.att_date
WHEN MATCHED THEN UPDATE SET
  region = S.region,
  country = S.country,
  department = S.department,
  status = S.status,
  check_in = S.check_in,
  check_out = S.check_out,
  load_ts = S.load_ts
WHEN NOT MATCHED THEN INSERT ROW;
