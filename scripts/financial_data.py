import pandas as pd
import random
from datetime import datetime, timedelta

# Parameters
num_records = 3_000_000
output_file = "financial_dataset_3m.csv"

regions = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]
countries = {
    "North America": ["USA", "Canada", "Mexico"],
    "Europe": ["Germany", "France", "UK", "Italy"],
    "Asia": ["China", "India", "Japan", "Singapore"],
    "South America": ["Brazil", "Argentina", "Chile"],
    "Africa": ["South Africa", "Nigeria", "Egypt"],
    "Oceania": ["Australia", "New Zealand"]
}
currencies = {
    "USA": "USD", "Canada": "CAD", "Mexico": "MXN",
    "Germany": "EUR", "France": "EUR", "UK": "GBP", "Italy": "EUR",
    "China": "CNY", "India": "INR", "Japan": "JPY", "Singapore": "SGD",
    "Brazil": "BRL", "Argentina": "ARS", "Chile": "CLP",
    "South Africa": "ZAR", "Nigeria": "NGN", "Egypt": "EGP",
    "Australia": "AUD", "New Zealand": "NZD"
}
products = ["Software", "Hardware", "Consulting", "Cloud Services", "Licenses"]

def generate_data(n):
    for i in range(1, n+1):
        region = random.choice(regions)
        country = random.choice(countries[region])
        currency = currencies[country]
        product = random.choice(products)
        date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825))
        revenue = round(random.uniform(1000, 100000), 2)
        expense = round(revenue * random.uniform(0.4, 0.9), 2)
        profit = revenue - expense

        yield [
            f"T{i:07d}", region, country, product, date.strftime("%Y-%m-%d"),
            currency, revenue, expense, profit
        ]

columns = ["TransactionID", "Region", "Country", "Product", "Date", "Currency", "Revenue", "Expense", "Profit"]

chunk_size = 100_000
with open(output_file, "w", encoding="utf-8") as f:
    f.write(",".join(columns) + "\n")
    for start in range(0, num_records, chunk_size):
        chunk = list(generate_data(min(chunk_size, num_records - start)))
        df = pd.DataFrame(chunk, columns=columns)
        df.to_csv(f, header=False, index=False)

print(f"✅ File generated: {output_file}")

