from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'trades_dag', default_args=default_args, schedule_interval=timedelta(days=1)
)

run_dbt = DockerOperator(
    task_id='run_dbt',
    image='02_data_platform-dbt-1', 
    api_version='auto',
    auto_remove=True,
    command='dbt run --select fct_pdt_count --profiles-dir . --project-dir /path/to/dbt_trades', 
    network_mode='tradesnetwork', 
    dag=dag,
)

run_dbt
