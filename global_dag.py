from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAGs and tasks
for num in range(1):
    dag_name = "dag_{}".format(num)
    globals()[dag_name] = DAG(
        "DAG_name_in_airflow_{}".format(num),
        schedule_interval="@once",
        start_date=datetime(2024, 5, 29, 4, 57, 0, 0),  # Start from today
        is_paused_upon_creation=True
    )

    start = BashOperator(
        task_id='start_{}'.format(num),
        bash_command='echo start pipeline',
        dag=globals()[dag_name]
    )

    hello = BashOperator(
        task_id="hello_{}".format(num),
        bash_command="echo hello",
        dag=globals()[dag_name]
    )

    end = BashOperator(
        task_id='end_{}'.format(num),
        bash_command='echo end pipeline',
        trigger_rule="always",
        dag=globals()[dag_name]
    )

    # Define task dependencies
    start >> hello >> end
