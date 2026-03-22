import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib

# =============================
# CONNECT TO SQL SERVER (CBS)
# =============================

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=CBS;"
    "Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

print("Connected to CBS")

# =============================
# CONFIG
# =============================

num_rows = 50000
start_date = datetime(2026, 3, 1)

# =============================
# GENERATE DATA
# =============================

timestamps = []
for _ in range(num_rows):
    random_day = start_date + timedelta(days=random.randint(0, 4))
    hour = random.choice([10,11,12,18,19,20,21])
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

atm_df = pd.DataFrame({
    "atm_id": np.random.randint(200, 230, num_rows),
    "location": np.random.choice(
        ["Mumbai - Andheri", "Mumbai - Bandra", "Mumbai - Dadar"],
        num_rows
    ),
    "withdrawal_amount": np.random.randint(1000, 20000, num_rows),
    "tnx_status": np.random.choice(
        ["SUCCESS", "FAILED"],
        num_rows,
        p=[0.96, 0.04]
    ),
    "response_time_ms": np.random.randint(500, 2000, num_rows),
    "timestamp": timestamps
})

# Increase response time for failed txns
atm_df.loc[atm_df["tnx_status"] == "FAILED", "response_time_ms"] *= 3

print("ATM Logs Generated:", len(atm_df))

# =============================
# INSERT INTO SQL SERVER
# =============================

atm_df.to_sql(
    "atm_logs",
    con=engine,
    schema="dbo",
    if_exists="append",
    index=False,
    chunksize=5000
)

print("ATM logs inserted successfully")