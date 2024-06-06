from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_current_time():
    print(f"Current time: {datetime.now()}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'simple9_5_dag',
    default_args=default_args,
    description='A simple DAG that prints the current time every 8 AM',
    schedule_interval='5 9 * * *',  # Every 7 55 minutes
    start_date=datetime(2024, 5, 29, 4, 57, 0, 0),  # Start from today
    end_date=datetime(2024, 6, 29, 4, 57, 0, 0),
    catchup=True,
)

print_time_task = PythonOperator(
    task_id='print_current_time',
    python_callable=print_current_time,
    dag=dag,
)

print_time_task
