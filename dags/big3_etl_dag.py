from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'big3_etl',
    default_args=default_args,
    description='Big3 ETL pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

extract_task = BashOperator(
    task_id='extract',
    bash_command='python /opt/airflow/dags/jobs/extract.py',
    dag=dag,
)

transform_task = BashOperator(
    task_id='transform',
    bash_command='python /opt/airflow/dags/jobs/transform.py',
    dag=dag,
)

load_task = BashOperator(
    task_id='load',
    bash_command='python /opt/airflow/dags/jobs/load.py',
    dag=dag,
)

extract_task >> transform_task >> load_task