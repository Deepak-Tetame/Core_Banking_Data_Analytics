import pandas as pd
import random
from datetime import datetime
from sqlalchemy import create_engine
import urllib

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

print("Connected to CBS database successfully")

# =============================
# READ HOURLY TRANSACTION VOLUME
# (Using tnx_timestamp as per your schema)
# =============================

query = """
SELECT
DATEPART(HOUR, tnx_timestamp) AS tnx_hour,
COUNT(*) AS tnx_count
FROM dbo.core_banking_transactions
GROUP BY DATEPART(HOUR, tnx_timestamp)
ORDER BY tnx_hour
"""

hourly_volume = pd.read_sql(query, engine)

print("\nHourly Volume:")
print(hourly_volume)

# =============================
# GENERATE REPLICATION METRICS
# =============================

replication_data = []

for _, row in hourly_volume.iterrows():
    hour = int(row["tnx_hour"])
    volume = int(row["tnx_count"])

    # Load-based lag logic
    base_lag = volume * 0.002

    replication_data.append({
        "environment": "PR",
        "extract_lag_sec": round(base_lag + random.uniform(0, 2), 2),
        "replicat_lag_sec": round(base_lag + random.uniform(1, 5), 2),
        "trail_file_size_mb": round(volume * 0.01, 2),
        "log_read_rate_mb": round(volume * 0.005, 2),
        "metric_timestamp": datetime(2026, 3, 1, hour, 0, 0)
    })

replication_df = pd.DataFrame(replication_data)

print("\nReplication Data Preview:")
print(replication_df.head())

# =============================
# INSERT INTO replication_metrics
# =============================

replication_df.to_sql(
    "replication_metrics",
    con=engine,
    schema="dbo",
    if_exists="append",
    index=False
)

print("\nReplication metrics inserted successfully into CBS.dbo.replication_metrics")