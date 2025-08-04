import pandas as pd
from datetime import datetime
from etl.transform import clean_data
from etl.db import get_engine

# Step 1: Read raw data
df = pd.read_csv("data/nasdaq.csv", sep=r'\s+')

# Step 2: Clean and transform
df_clean = clean_data(df)

# Step 3: Connect to DB
engine = get_engine()

# Step 4: Write to PostgreSQL
df_clean.to_sql(
    "nasdaq_data",
    con=engine,
    if_exists='append',
    index=False
)

# Step 5: Write ETL report
with open('etl_report.txt', 'w') as f:
    f.write(f'ETL Run timestamp: {datetime.now()}\n')
    f.write(f'Total rows: {len(df)}\n')
    f.write(f'Valid rows: {len(df_clean)}\n')
    f.write(f'Dropped rows: {len(df) - len(df_clean)}\n')

print("✅ ETL process completed successfully.")
