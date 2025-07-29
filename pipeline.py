import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime

db = os.getenv("POSTGRES_DB", "my_etl")
user = os.getenv("POSTGRES_USER", "postgres")
password = os.getenv("POSTGRES_PASSWORD", "Heslo62ab")  # or "postgres" if you prefer
host = os.getenv("POSTGRES_HOST", "localhost")

try:
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}")
    with engine.connect() as conn:
        conn.execute(text('SELECT 1'))
except Exception as e:
    print('Database connesction failed:', e)

# Read CSV file
try:
    df = pd.read_csv('data/nasdaq.csv', delim_whitespace=True)
    df.columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'TickVolume']
    print(df.head())
    print(df.columns)
except Exception as e:
    print(f'Failed to read CSV:', e)

# Clean the data
df_clean = df.dropna()
print(f'Dropped rows: {len(df) - len(df_clean)}')
df_clean['Timestamp'] = datetime.now()

# Test DB connection
with engine.connect() as conn:
    result = conn.execute(text('SELECT version();'))
    print('\nPostgreSQL version', result.fetchone()[0])

# Write data to PostgreSQL
df_clean.to_sql(
    "my_etl",
    con=engine,
    if_exists='replace',
    index=False
)

# Write a report
with open('etl_report.txt', 'w') as f:
    f.write(f'ETL Run timestamp: {datetime.now()}\n')
    f.write(f'Total rows: {len(df)}\n')
    f.write(f'Valid rows: {len(df_clean)}\n')
    f.write(f'Invalid rows: {len(df_clean)}\n')