# etl/transform.py
import pandas as pd
from datetime import datetime
import logging
 
#Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Starting data cleaning')

    # Extract date and time
    df[['Date', 'Time']] = df['DateTime'].str.extract(r'(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}:\d{2})')
    df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%Y.%m.%d %H:%M:%S')
    df.drop(['Date', 'Time'], axis=1, inplace=True)

    # Drop rows with missing core values
    df_clean = df.dropna(subset=['Open', 'High', 'Low', 'Close']).copy()

    #Add metadata
    df_clean['source'] = 'nasdaq.csv'
    df_clean['Timestamp'] = datetime.now()

    logger.info(f'Cleaned {len(df_clean)} rows')
    return df_clean
