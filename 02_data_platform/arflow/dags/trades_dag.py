from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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

run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='dbt run --select fct_pdt_count --profiles-dir . --project-dir dbt_trades',
    dag=dag,
)

run_dbt
