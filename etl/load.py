from datetime import datetime
import logging
logger = logging.getLogger(__name__)

def enrich_data(df, source):
    df['source'] = source
    df['Timestamp'] = datetime.now()
    return df

def load_to_postgres(df, engine, table_name='nasdaq_data'):
    df.to_sql(table_name, engine, if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} rows to PostgreSQL.")