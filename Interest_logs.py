import pandas as pd
import numpy as np
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

num_accounts = 2000
start_date = datetime(2026, 3, 1)

accounts = np.random.randint(1000, 5000, num_accounts)
interest_rates = np.random.choice([6.0, 6.5, 7.0], num_accounts)

interest_data = []

for acc, rate in zip(accounts, interest_rates):
    balance = np.random.randint(50000, 1000000)
    daily_interest = (balance * (rate/100)) / 365

    interest_data.append({
        "account_id": acc,
        "interest_rate": rate,
        "daily_balance": balance,
        "interest_amount": round(daily_interest, 2),
        "accrual_date": start_date.date()
    })

interest_df = pd.DataFrame(interest_data)

print("Interest Logs Generated:", len(interest_df))

# =============================
# INSERT INTO SQL SERVER
# =============================

interest_df.to_sql(
    "interest_logs",
    con=engine,
    schema="dbo",
    if_exists="append",
    index=False
)

print("Interest logs inserted successfully")