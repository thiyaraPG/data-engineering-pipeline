import pandas as pd

def validate_sales(df, fx):
    valid, rejected = [], []
    for _, r in df.iterrows():
        if pd.isna(r["SaleID"]):
            rejected.append("missing_sale_id")
        elif r["Currency"] not in fx:
            rejected.append("unsupported_currency")
        elif r["Quantity"] <= 0:
            rejected.append("invalid_quantity")
        else:
            valid.append(r)
    return pd.DataFrame(valid), rejected

def validate_financial(df, fx):
    valid, rejected = [], []
    for _, r in df.iterrows():
        if pd.isna(r["TransactionID"]):
            rejected.append("missing_transaction_id")
        elif r["Currency"] not in fx:
            rejected.append("unsupported_currency")
        elif abs(r["Profit"] - (r["Revenue"] - r["Expense"])) > 0.01:
            rejected.append("profit_mismatch")
        else:
            valid.append(r)
    return pd.DataFrame(valid), rejected

def validate_attendance(df):
    valid, rejected = [], []
    allowed = {"Present", "Absent", "Remote"}
    for _, r in df.iterrows():
        if pd.isna(r["StaffID"]):
            rejected.append("missing_staff_id")
        elif r["Status"] not in allowed:
            rejected.append("invalid_status")
        else:
            valid.append(r)
    return pd.DataFrame(valid), rejected
