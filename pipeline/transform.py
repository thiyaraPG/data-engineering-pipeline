import pandas as pd

def load_fx_rates(path="data/fx_rates.csv"):
    fx = pd.read_csv(path)
    return dict(zip(fx["currency_code"], fx["rate_to_usd"]))

def convert_to_usd(amount, currency, fx_rates):
    rate = fx_rates.get(currency)
    if rate is None:
        return None
    return round(float(amount) * float(rate), 2)
