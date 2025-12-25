import subprocess
import json
import os
import pandas as pd
import sys

from pipeline.log import log
from pipeline.bq import apply_sql

from pipeline.transform import load_fx_rates, convert_to_usd
from pipeline.validate import (
    validate_sales,
    validate_financial,
    validate_attendance
)

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


def run_script(path):
    log("INFO", "script_start", script=path)
    subprocess.run([sys.executable, path], check=True)
    log("INFO", "script_complete", script=path)


def main():
    report = {}

    try:
        # 1. Run provided scripts
        run_script("scripts/sales_dataset_3m.py")
        run_script("scripts/financial_data.py")
        run_script("scripts/attendance_dataset_3m.py")

        # 2. Load FX rates
        fx = load_fx_rates()

        # 3. Read generated CSVs
        sales = pd.read_csv("sales_dataset_3m.csv")
        financial = pd.read_csv("financial_dataset_3m.csv")
        attendance = pd.read_csv("attendance_dataset_3m.csv")

        # 4. Validate
        sales_ok, sales_rej = validate_sales(sales, fx)
        fin_ok, fin_rej = validate_financial(financial, fx)
        att_ok, att_rej = validate_attendance(attendance)

        # 5. Transform monetary fields → USD
        sales_ok["unit_price_usd"] = sales_ok.apply(
            lambda r: convert_to_usd(r["UnitPrice"], r["Currency"], fx), axis=1
        )
        sales_ok["total_sales_usd"] = sales_ok.apply(
            lambda r: convert_to_usd(r["TotalSales"], r["Currency"], fx), axis=1
        )

        fin_ok["revenue_usd"] = fin_ok.apply(
            lambda r: convert_to_usd(r["Revenue"], r["Currency"], fx), axis=1
        )
        fin_ok["expense_usd"] = fin_ok.apply(
            lambda r: convert_to_usd(r["Expense"], r["Currency"], fx), axis=1
        )
        fin_ok["profit_usd"] = fin_ok.apply(
            lambda r: convert_to_usd(r["Profit"], r["Currency"], fx), axis=1
        )

        # 6. Summary report
        report["sales"] = {
            "rows_read": len(sales),
            "rows_loaded": len(sales_ok),
            "rows_rejected": len(sales_rej),
            "usd_min": sales_ok["total_sales_usd"].min(),
            "usd_max": sales_ok["total_sales_usd"].max()
        }
        report["financial"] = {
            "rows_read": len(financial),
            "rows_loaded": len(fin_ok),
            "rows_rejected": len(fin_rej),
            "usd_min": fin_ok["revenue_usd"].min(),
            "usd_max": fin_ok["revenue_usd"].max()
        }
        report["attendance"] = {
            "rows_read": len(attendance),
            "rows_loaded": len(att_ok),
            "rows_rejected": len(att_rej)
        }

        # 7. Write report
        os.makedirs("reports", exist_ok=True)
        with open("reports/summary.json", "w") as f:
            json.dump(report, f, indent=2)

        log("INFO", "pipeline_completed", report="reports/summary.json")

        # 8. BigQuery (DRY-RUN supported)
        project = os.getenv("GCP_PROJECT", "demo_project")
        dataset = os.getenv("BQ_DATASET", "demo_dataset")
        location = os.getenv("BQ_LOCATION", "US")

        if DRY_RUN:
            log("INFO", "dry_run_enabled", message="BigQuery execution skipped")

        apply_sql(None, "ddl/01_dataset.sql", project, dataset, location, DRY_RUN)
        apply_sql(None, "ddl/02_tables.sql", project, dataset, location, DRY_RUN)

        log("INFO", "merge_prepared", rows={
            "sales": len(sales_ok),
            "financial": len(fin_ok),
            "attendance": len(att_ok)
        })

        apply_sql(None, "ddl/03_merges.sql", project, dataset, location, DRY_RUN)

    except Exception as e:
        log("ERROR", "pipeline_failed", error=str(e))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
