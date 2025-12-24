# data-engineering-pipeline
Data engineering pipeline with USD normalization and BigQuery-ready design

## Overview
This project implements a runnable data pipeline that orchestrates three provided Python scripts, validates and normalizes their outputs, and prepares the data for loading into Google BigQuery with clean schemas and idempotent upserts.

All monetary values are normalized to **USD**.  
The pipeline is safe to run multiple times, emits structured logs, and produces a summary report for data quality verification.

---
## Project Structure

```text
Exercise/
├── scripts/          # Provided dataset generators
├── pipeline/         # Orchestration, validation, transformation
├── ddl/              # BigQuery DDL and MERGE SQL
├── data/             # Reference data (FX rates)
├── reports/          # Pipeline summary output
│
├── *.csv              # Generated datasets
└── .gitignore

```

## Provided Datasets
The pipeline orchestrates the following **given scripts**:

| Script | Output File | Description |
|------|------------|-------------|
| `sales_dataset_3m.py` | `sales_dataset_3m.csv` | Sales transactions |
| `financial_data.py` | `financial_dataset_3m.csv` | Revenue, expense, profit |
| `attendance_dataset_3m.py` | `attendance_dataset_3m.csv` | Staff attendance |

---

## Expected Output Format

### Sales Dataset (`sales_dataset_3m.csv`)
- `SaleID` (string)
- `Region`, `Country`, `Product` (string)
- `Date` (YYYY-MM-DD)
- `Currency` (ISO code)
- `Quantity` (integer)
- `UnitPrice`, `TotalSales` (local currency)

### Financial Dataset (`financial_dataset_3m.csv`)
- `TransactionID` (string)
- `Region`, `Country`, `Product` (string)
- `Date` (YYYY-MM-DD)
- `Currency` (ISO code)
- `Revenue`, `Expense`, `Profit` (local currency)

### Attendance Dataset (`attendance_dataset_3m.csv`)
- `StaffID` (string)
- `Name`, `Department`, `Country`, `Region`
- `Date` (YYYY-MM-DD)
- `Status` (Present | Absent | Remote)
- `CheckInTime`, `CheckOutTime`

---

## Pipeline Flow


```text
Provided Scripts
↓
CSV Outputs
↓
Validation & USD Normalization
↓
Summary Report
↓
(BigQuery Staging Tables)
↓
MERGE → Final Tables (Idempotent)

```

## Currency Normalization
- All monetary fields are automatically detected per dataset.
- Values are converted to **USD** using `data/fx_rates.csv`.
- BigQuery schemas store **USD-only numeric columns** (e.g. `total_sales_usd`).
- Rows with unsupported currencies are rejected and logged.

---

## Data Quality Checks
Validation occurs **before loading**:

**Sales**
- `SaleID` not null
- Quantity > 0
- Supported currency

**Financial**
- `TransactionID` not null
- Revenue & Expense ≥ 0
- Profit ≈ Revenue − Expense

**Attendance**
- `StaffID` not null
- Valid status values
- Check-in/out consistency

Rejected rows are excluded from loading and counted.

---

## Summary Report
Each run generates a summary report at:

reports/summary.json

The report includes:
- rows read
- rows loaded
- rows rejected
- USD minimum and maximum sanity values

---

## BigQuery Design & Idempotency
- Final tables:
  - `fact_sales`
  - `fact_financial`
  - `fact_attendance`
- Staging tables are loaded on each run.
- Final tables are updated using **MERGE (upsert)** statements.
- Natural keys ensure **no duplicate data** on re-runs:
  - `sale_id`
  - `transaction_id`
  - `(staff_id, att_date)`

All BigQuery DDL and MERGE SQL is committed under `/ddl`.

---

## Performance & Storage Decision
**Batch loading** was chosen over streaming because:
- Data is generated in bulk
- No real-time requirement exists
- Idempotent MERGE logic is simpler and safer

---

## Observability
- Structured **JSON logs** for all pipeline steps
- Clear error messages
- Pipeline exits **non-zero** on validation or execution failures

---

## Running the Pipeline

```bash
python -m venv .venv
.venv\Scripts\activate
set DRY_RUN=true   # Windows
python -m pipeline.run_pipeline





