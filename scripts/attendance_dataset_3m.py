import pandas as pd
import random
from datetime import datetime, timedelta

# Parameters
num_records = 3_000_000
output_file = "attendance_dataset_3m.csv"

# Sample data
regions = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]
countries = {
    "North America": ["USA", "Canada", "Mexico"],
    "Europe": ["Germany", "France", "UK", "Italy"],
    "Asia": ["China", "India", "Japan", "Singapore"],
    "South America": ["Brazil", "Argentina", "Chile"],
    "Africa": ["South Africa", "Nigeria", "Egypt"],
    "Oceania": ["Australia", "New Zealand"]
}
departments = ["IT", "Sales", "Marketing", "HR", "Finance", "Operations"]
first_names = ["Alice", "Bob", "Chen", "Daniela", "Ethan", "Fatima", "George", "Hiro", "Isabella", "Juan"]
last_names = ["Johnson", "Smith", "Wei", "Lopez", "Brown", "Hassan", "Wilson", "Tanaka", "Rossi", "Martinez"]
statuses = ["Present", "Absent", "Remote"]

# Generator function
def generate_attendance_data(n):
    for i in range(1, n+1):
        staff_id = f"ST{i:07d}"
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        region = random.choice(regions)
        country = random.choice(countries[region])
        department = random.choice(departments)
        date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825))  # 5 years
        status = random.choices(statuses, weights=[0.7, 0.1, 0.2])[0]  # more likely to be Present
        if status == "Present" or status == "Remote":
            check_in_hour = random.randint(8, 10)
            check_in_minute = random.randint(0, 59)
            check_out_hour = random.randint(16, 18)
            check_out_minute = random.randint(0, 59)
            check_in = f"{check_in_hour:02d}:{check_in_minute:02d}"
            check_out = f"{check_out_hour:02d}:{check_out_minute:02d}"
        else:
            check_in = "-"
            check_out = "-"

        yield [
            staff_id, name, region, country, department, date.strftime("%Y-%m-%d"),
            status, check_in, check_out
        ]

# Write CSV in chunks
columns = ["StaffID", "Name", "Region", "Country", "Department", "Date", "Status", "CheckInTime", "CheckOutTime"]
chunk_size = 100_000

with open(output_file, "w", encoding="utf-8") as f:
    f.write(",".join(columns) + "\n")
    for start in range(0, num_records, chunk_size):
        chunk = list(generate_attendance_data(min(chunk_size, num_records - start)))
        df = pd.DataFrame(chunk, columns=columns)
        df.to_csv(f, header=False, index=False)

print(f"✅ Attendance dataset generated: {output_file}")

