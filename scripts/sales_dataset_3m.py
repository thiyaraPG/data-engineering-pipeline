import pandas as pd
import random
from datetime import datetime, timedelta

# Parameters
num_records = 3_000_000
output_file = "sales_dataset_3m.csv"

# Sample lists
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

# Generator function to avoid memory issues
def generate_sales_data(n):
    for i in range(1, n+1):
        region = random.choice(regions)
        country = random.choice(countries[region])
        product = random.choice(products)
        currency = currencies[country]
        date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825))  # 5 years
        quantity = random.randint(1, 50)
        unit_price = round(random.uniform(100, 5000), 2)
        total_sales = round(quantity * unit_price, 2)

        yield [
            f"S{i:07d}", region, country, product, date.strftime("%Y-%m-%d"),
            currency, quantity, unit_price, total_sales
        ]

# Write CSV in chunks
columns = ["SaleID", "Region", "Country", "Product", "Date", "Currency", "Quantity", "UnitPrice", "TotalSales"]
chunk_size = 100_000

with open(output_file, "w", encoding="utf-8") as f:
    f.write(",".join(columns) + "\n")
    for start in range(0, num_records, chunk_size):
        chunk = list(generate_sales_data(min(chunk_size, num_records - start)))
        df = pd.DataFrame(chunk, columns=columns)
        df.to_csv(f, header=False, index=False)

print(f"✅ Sales dataset generated: {output_file}")

