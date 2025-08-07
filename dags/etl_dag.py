from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
sys.path.append('/opt/airflow')
from etl.transform import clean_data
from etl.db import get_engine
from etl.load import enrich_data, load_to_postgres
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'Honza',
    'depends_on_past': False,
    'email':['honza.kaminsky@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Your main ETL logic
def run_etl():
    file_path = '/opt/airflow/dags/data/nasdaq.csv'  # Airflow container path
    try:
        logger.info(f'Reading data from {file_path}')
        df = pd.read_csv(file_path, sep=r'\s+')

        # Add enrichment manually if clean_data doesn't do it
        df_clean = clean_data(df)
        logger.info(f'Cleaned data shape: {df_clean.shape}')
        df_enriched = enrich_data(df_clean, source='nasdaq.csv')

        engine = get_engine()
        load_to_postgres(df_enriched, engine)
        logger.info(f'{len(df_clean)} rows loaded to DB')

    except Exception as e:
        logger.error(f'ETL job failed: {e}')
        raise
def fail_intentionally():
    raise ValueError("This is a test failure for email alerting.")
# Define DAG
with DAG('nasdaq_etl_dag',
         default_args=default_args,
         schedule_interval=None,  # Manual for now
         catchup=False) as dag:

    run_etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl
    )
    fail_test = PythonOperator(
        task_id='fail_test',
        python_callable=fail_intentionally,
        dag=dag
    )
    run_etl_task >> fail_test