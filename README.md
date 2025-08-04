# ETL Pipeline Project 

## Overview
This is a containerized ETL pipeline using Apache Airflow and PostgreSQL. It ingests stock data (e.g., from `nasdaq.csv`), performs data cleaning, and stores it in a PostgreSQL database.

## Tech Stack
- Python
- Pandas
- PostgreSQL
- Apache Airflow (2.8.0)
- Docker Compose

## Folder Structure
my-etl-project/
├── airflow/
│   ├── dags/
│   │   └── etl_dag.py
│   ├── etl/
│   │   ├── db.py
│   │   └── transform.py
│   ├── data/
│   │   └── nasdaq.csv
│   └── docker-compose.yaml
└── README.md

## Usage

1. Navigate to the `airflow` folder:
   ```bash
   cd airflow
2. Start Airflow
    docker-compose up -d
3.	Open Airflow UI: http://localhost:8080
Default login: airflow / airflow
4.	Run the DAG nasdaq_etl_dag.


## Notes
	•	PostgreSQL stores the cleaned data in nasdaq_data table.
	•	The data source is /opt/airflow/dags/data/nasdaq.csv.

## Future Plans
	•	Add scheduling
	•	Add monitoring
	•	Load external data from an API