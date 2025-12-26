import subprocess
import json
import os
import pandas as pd
import sys
from datetime import datetime, timezone

from pipeline.bq import apply_sql
from pipeline.transform import load_fx_rates, convert_to_usd
from pipeline.validate import (
    validate_sales,
    validate_financial,
    validate_attendance
)

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

def log(level, event, **details):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "event": event,
        **details
    }
    output = json.dumps(record)
    if level == "ERROR":
        print(output, file=sys.stderr)
    else:
        print(output)

def cleanup_csv_files():
    files = [
        "sales_dataset_3m.csv",
        "financial_dataset_3m.csv",
        "attendance_dataset_3m.csv"
    ]
    for f in files:
        if os.path.exists(f):
            os.remove(f)
            log("INFO", "old_csv_deleted", file=f)


def run_script(path):
    try:
        log("INFO", "script_start", script=path)
        subprocess.run([sys.executable, path], check=True)
        log("INFO", "script_complete", script=path)
    except subprocess.CalledProcessError as e:
        log("ERROR", "script_failed", script=path, error=str(e))
        raise SystemExit(1)


def main():
    report = {}

    try:

        cleanup_csv_files()

        run_script("scripts/sales_dataset_3m.py")
        run_script("scripts/financial_data.py")
        run_script("scripts/attendance_dataset_3m.py")

        fx = load_fx_rates()
        if not fx:
            raise ValueError("FX rates could not be loaded")

        sales = pd.read_csv("sales_dataset_3m.csv")
        financial = pd.read_csv("financial_dataset_3m.csv")
        attendance = pd.read_csv("attendance_dataset_3m.csv")

        sales["unit_price_usd"] = sales.apply(
            lambda r: convert_to_usd(r["UnitPrice"], r["Currency"], fx), axis=1
        )
        sales["total_sales_usd"] = sales.apply(
            lambda r: convert_to_usd(r["TotalSales"], r["Currency"], fx), axis=1
        )

        financial["revenue_usd"] = financial.apply(
            lambda r: convert_to_usd(r["Revenue"], r["Currency"], fx), axis=1
        )
        financial["expense_usd"] = financial.apply(
            lambda r: convert_to_usd(r["Expense"], r["Currency"], fx), axis=1
        )
        financial["profit_usd"] = financial.apply(
            lambda r: convert_to_usd(r["Profit"], r["Currency"], fx), axis=1
        )

        sales_ok, sales_rej = validate_sales(sales, fx)
        fin_ok, fin_rej = validate_financial(financial, fx)
        att_ok, att_rej = validate_attendance(attendance)

        if sales_ok.empty:
            raise ValueError("All sales records rejected during validation")
        if fin_ok.empty:
            raise ValueError("All financial records rejected during validation")
        if att_ok.empty:
            raise ValueError("All attendance records rejected during validation")

        load_time = datetime.now(timezone.utc)
        sales_ok["load_ts"] = load_time
        fin_ok["load_ts"] = load_time
        att_ok["load_ts"] = load_time

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

        os.makedirs("reports", exist_ok=True)
        with open("reports/summary.json", "w") as f:
            json.dump(report, f, indent=2)

        log("INFO", "pipeline_completed", report="reports/summary.json")


        print("\n=== SALES (first 100 rows) ===")
        print(sales_ok.head(100))

        print("\n=== FINANCIAL (first 100 rows) ===")
        print(fin_ok.head(100))

        print("\n=== ATTENDANCE (first 100 rows) ===")
        print(att_ok.head(100))

        project = os.getenv("GCP_PROJECT", "demo_project")
        dataset = os.getenv("BQ_DATASET", "demo_dataset")
        location = os.getenv("BQ_LOCATION", "US")

        if DRY_RUN:
            log("INFO", "dry_run_enabled", message="BigQuery execution skipped")

        apply_sql(None, "ddl/01_dataset.sql", project, dataset, location, DRY_RUN)
        apply_sql(None, "ddl/02_tables.sql", project, dataset, location, DRY_RUN)
        apply_sql(None, "ddl/03_merges.sql", project, dataset, location, DRY_RUN)

    except Exception as e:
        log("ERROR", "pipeline_failed", error=str(e))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
