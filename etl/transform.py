# etl/transform.py
import pandas as pd
from datetime import datetime

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Extract date and time
    df[['Date', 'Time']] = df['DateTime'].str.extract(r'(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}:\d{2})')
    df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%Y.%m.%d %H:%M:%S')
    df.drop(['Date', 'Time'], axis=1, inplace=True)

    # Add metadata
    df['source'] = 'nasdaq.csv'

    # Drop rows with missing core values
    df_clean = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

    # Add processing timestamp
    df_clean['Timestamp'] = datetime.now()

    return df_clean
