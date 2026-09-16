# airflow/dags/data_pipeline_dag.py
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG('gcp_mirror_etl', start_date=datetime(2026, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    start_pipeline >> end_pipeline
