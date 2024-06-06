from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import requests

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="HttpCall",
    start_date=datetime(2024, 5, 30), 
    schedule='*/1 * * * *',
    catchup=False
    ) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")
        url = 'https://dev7-cloudrmm-api.sharpb2bcloud.com/rmm/device/status/count/b8bf935d-026c-426a-b9f2-ef97d01bf973'
        data = {'key': 'value'}
        response = requests.get(url, json=data)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the response content
            print(response)
        else:
            # Print an error message if the request was not successful
            print(f"HTTP request failed with status code {response.status_code}")

    # Set dependencies between tasks
    hello >> airflow()