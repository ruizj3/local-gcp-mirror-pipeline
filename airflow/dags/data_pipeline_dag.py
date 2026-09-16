from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Default arguments mapping directly to operational reliability concepts on the GCP exam
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the workflow schedule
with DAG(
    'warehouse_daily_aggregation_pipeline',
    default_args=default_args,
    description='Daily analytical processing and maintenance for the ecommerce data warehouse',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['analytics', 'gcp_mirror'],
) as dag:

    # 1. Verification Task: Ensure the streaming process table exists before running analytics
    # This mirrors checking for BigQuery landing partition readiness
    verify_warehouse_tables = SQLExecuteQueryOperator(
        task_id='verify_warehouse_tables',
        conn_id='postgres_warehouse',  # Configured in Airflow UI to point to port 5432
        sql="""
            CREATE TABLE IF NOT EXISTS aggregated_device_metrics (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                device VARCHAR(50),
                event_type VARCHAR(50),
                total_events INT
            );
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id VARCHAR(50),
                event_timestamp TIMESTAMP,
                user_id VARCHAR(50),
                event_type VARCHAR(50),
                product_id VARCHAR(50),
                price DOUBLE PRECISION,
                device VARCHAR(50)
            );
        """
    )

    # 2. Analytical Task: Aggregate raw minute windows into an executive daily metrics table
    # This mirrors a BigQuery scheduled query or dbt transformation
    generate_daily_executive_report = SQLExecuteQueryOperator(
        task_id='generate_daily_executive_report',
        conn_id='postgres_warehouse',
        sql="""
            CREATE TABLE IF NOT EXISTS daily_executive_device_summary AS 
            SELECT 
                window_start::DATE as reporting_date,
                device,
                event_type,
                SUM(total_events) as total_daily_events
            FROM aggregated_device_metrics
            GROUP BY 1, 2, 3;
            
            -- Insert new patterns cleanly if table exists
            INSERT INTO daily_executive_device_summary (reporting_date, device, event_type, total_daily_events)
            SELECT 
                window_start::DATE as reporting_date,
                device,
                event_type,
                SUM(total_events)
            FROM aggregated_device_metrics
            WHERE window_start >= CURRENT_DATE - INTERVAL '1 day'
            GROUP BY 1, 2, 3
            ON CONFLICT DO NOTHING;
        """
    )

    # 3. Governance Task: Delete metrics older than 30 days to optimize storage costs
    # This maps directly to GCP BigQuery Partition Expiration and Storage Lifecycle management
    prune_old_partitions = SQLExecuteQueryOperator(
        task_id='prune_old_partitions',
        conn_id='postgres_warehouse',
        sql="""
            DELETE FROM aggregated_device_metrics 
            WHERE window_start < CURRENT_DATE - INTERVAL '30 days';
        """
    )

    # Define DAG Orchestration Dependency Chain
    verify_warehouse_tables >> generate_daily_executive_report >> prune_old_partitions
