import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime


db = os.environ["POSTGRES_DB"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]

import time

for i in range(10):
    try:
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}")
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        print("✅ Database connection established")
        break
    except Exception as e:
        print(f"⏳ Waiting for DB... ({i+1}/10): {e}")
        time.sleep(2)
else:
    print("❌ Could not connect to the database after 10 tries.")
    exit(1)


# Read CSV file
df = pd.read_csv("data/nasdaq.csv", delim_whitespace=True)
print(df.columns.tolist())
df[['Date', 'Time']] = df['DateTime'].str.extract(r'(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}:\d{2})')
df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%Y.%m.%d %H:%M:%S')
df.drop(['Date', 'Time'], axis=1, inplace=True)
df['source'] = 'nasdaq.csv'
print(df.dtypes)

# Clean the data
print(df.isnull().sum())  # Check where nulls are
df_clean = df.dropna(subset=['Open', 'High', 'Low', 'Close'])  # Drop only if core values missing
print(f'Dropped rows: {len(df) - len(df_clean)}')
df_clean['Timestamp'] = datetime.now()

# Test DB connection
with engine.connect() as conn:
    result = conn.execute(text('SELECT version();'))
    print('\nPostgreSQL version', result.fetchone()[0])

# Write data to PostgreSQL
print("DateTime range:", df['DateTime'].min(), "to", df['DateTime'].max())

print(f"Number of rows to write: {len(df_clean)}")

df_clean.to_sql(
    "nasdaq_data",
    con=engine,
    if_exists='append',
    index=False
)

# Write a report
with open('etl_report.txt', 'w') as f:
    f.write(f'ETL Run timestamp: {datetime.now()}\n')
    f.write(f'Total rows: {len(df)}\n')
    f.write(f'Valid rows: {len(df_clean)}\n')
    f.write(f'Invalid rows: {len(df_clean)}\n')