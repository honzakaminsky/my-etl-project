from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
sys.path.append('/opt/airflow')
from etl.transform import clean_data
from etl.db import get_engine

# Default DAG arguments
default_args = {
    'owner': 'Honza',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Your main ETL logic
def run_etl():
    file_path = '/opt/airflow/dags/data/nasdaq.csv'  # Airflow container path
    df = pd.read_csv(file_path, sep=r'\s+')

    # Add enrichment manually if clean_data doesn't do it
    df_clean = clean_data(df)
    df_clean['source'] = 'nasdaq.csv'
    df_clean['Timestamp'] = datetime.now()

    engine = get_engine()
    df_clean.to_sql('nasdaq_data', engine, if_exists='append', index=False)
    print(f"✅ {len(df_clean)} rows loaded to DB.")

# Define DAG
with DAG('nasdaq_etl_dag',
         default_args=default_args,
         schedule_interval=None,  # Manual for now
         catchup=False) as dag:

    run_etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl
    )
