import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine
import urllib

# =============================
# CONFIGURATION
# =============================
num_rows = 100000 # Start with 100K
start_date = datetime(2026, 3, 1)

# =============================
# GENERATE REALISTIC TIMESTAMPS
# =============================
timestamps = []

for _ in range(num_rows):
    random_day = start_date + timedelta(days=random.randint(0, 4))

    # Peak hour distribution (Evening heavy load)
    hour = np.random.choice(
        [10, 11, 12, 18, 19, 20, 21, 22],
        p=[0.08, 0.08, 0.08, 0.18, 0.20, 0.20, 0.10, 0.08]
    )
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    timestamps.append(datetime(
        random_day.year,
        random_day.month,
        random_day.day,
        hour,
        minute,
        second
    ))

# =============================
# CREATE DATAFRAME
# =============================
df = pd.DataFrame({
    "tnx_id": range(1000000, 1000000 + num_rows),
    "account_id": np.random.randint(1000, 5000, num_rows),
    "branch_id": np.random.randint(101, 120, num_rows),
    "tnx_type": np.random.choice(
        ["DEPOSIT", "WITHDRAWAL", "TRANSFER"],
        num_rows,
        p=[0.3, 0.4, 0.3]
    ),
    "channel": np.random.choice(
        ["BRANCH", "ATM", "ONLINE"],
        num_rows,
        p=[0.25, 0.35, 0.40]
    ),
    "amount": np.random.randint(1000, 200000, num_rows),
    "tnx_status": np.random.choice(
        ["SUCCESS", "FAILED"],
        num_rows,
        p=[0.95, 0.05]
    ),
    "tnx_timestamp": timestamps
})

print("Data Generated Successfully")
print("Rows in dataframe:", len(df))

# =============================
# VALIDATE DISTRIBUTION
# =============================
print("\nFailure Rate (%):")
print(df["tnx_status"].value_counts(normalize=True) * 100)

print("\nChannel Distribution (%):")
print(df["channel"].value_counts(normalize=True) * 100)

print("\nPeak Hour Distribution:")
print(df["tnx_timestamp"].dt.hour.value_counts().sort_index())

# =============================
# CONNECT TO SQL SERVER (CBS)
# =============================
server = "localhost"
database = "CBS"

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=CBS;"
    "Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

print("\nConnected to CBS database successfully")

# =============================
# INSERT INTO SQL SERVER
# =============================
df.to_sql(
    "core_banking_transactions",
    con=engine,
    schema="dbo",
    if_exists="append",
    index=False,
    chunksize=10000
)

print("Data Inserted Successfully into CBS.dbo.core_banking_transactions")