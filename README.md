# Data Engineering Pipeline

## Overview

This project implements an **end-to-end data engineering pipeline** that orchestrates three provided Python scripts, validates and transforms their outputs, normalizes all monetary values to **USD**, and prepares the data for loading into **Google BigQuery** using clean schemas and idempotent upsert logic.

A real BigQuery account is not available, so the pipeline runs in **DRY-RUN mode**, executing all logic while logging and displaying outputs instead of performing a live load.

---

## End-to-End Pipeline Flow

### 1. Dataset Generation (Provided Scripts)

The pipeline first runs the **three provided scripts** from the `scripts/` folder.

| Script | Output |
|------|------|
| `sales_dataset_3m.py` | `sales_dataset_3m.csv` |
| `financial_data.py` | `financial_dataset_3m.csv` |
| `attendance_dataset_3m.py` | `attendance_dataset_3m.csv` |

Before running these scripts, **previous CSV files are deleted** because each script generates a complete batch snapshot of data. Deleting old CSV files ensures every pipeline run starts fresh and avoids mixing old and new data.

---

### 2. Schema Creation (One-Time)

BigQuery schemas are defined in SQL under the `ddl/` folder:

- `01_dataset.sql` – Dataset creation  
- `02_tables.sql` – Fact table creation  

These scripts are **idempotent** and safe to re-run.

---

### 3. Transformation & Validation

- Currency normalization and transformations are handled in `pipeline/transform.py`
- Data validation is handled in `pipeline/validate.py`
- All monetary values are converted to **USD only** using FX rates.
- Invalid records are rejected before loading

---

### 4. Idempotent Upserts (Stored-Procedure Logic)

The file `ddl/03_merges.sql` contains **MERGE statements** that act like a **stored procedure**.

This logic:

- Runs on every pipeline execution
- Uses natural business keys to upsert data
- Updates existing records and inserts new ones
- Prevents duplicates automatically

| Table | Natural Key |
|------|------------|
| `fact_sales` | `sale_id` |
| `fact_financial` | `transaction_id` |
| `fact_attendance` | `(staff_id, att_date)` |

This ensures the pipeline is **safe to run multiple times**.

---

## Reporting & Observability

After execution, a summary report is generated at:

```text
reports/summary.json
```
The report includes:
- Rows read
- Rows loaded
- Rows rejected
- USD min/max sanity values

Structured JSON logs are emitted throughout the pipeline to provide clear observability and debugging support.

---

## Expected Output Format

Sales

```text
sale_id, sale_date, region, country, product, quantity, currency_code, unit_price_usd, total_sales_usd, load_ts
```
Financial
```
transaction_id, txn_date, region, country, product, currency_code, revenue_usd, expense_usd, profit_usd, load_ts
```
Attendance
```
staff_id, att_date, region, country, department, status, check_in, check_out, load_ts
```
### Performance & Storage Decision

Batch loading was chosen because:
  - Data is generated in bulk
  - No real-time ingestion requirement exists

### DRY-RUN Mode 

 Because BigQuery access is not available, the pipeline runs in DRY-RUN mode:
  - All SQL statements are generated and logged
  - BigQuery execution is skipped
  - Final transformed datasets are **displayed using `print` statements** to demonstrate output correctness

This approach allows verification of transformation, validation, USD normalization, and idempotent upsert logic without requiring an active BigQuery account.

### Running the Pipeline

```text
python -m venv .venv
.venv\Scripts\activate

set DRY_RUN=true
python -m pipeline.run_pipeline

