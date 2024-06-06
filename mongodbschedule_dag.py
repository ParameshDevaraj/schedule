from datetime import datetime

from airflow import DAG
from pymongo import MongoClient
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Connect to MongoDB
#client = MongoClient('mongodb://127.0.0.1:27017')
client = MongoClient("mongodb://parameshtest:gWVDkzMf3nfdG3G6rikaEc85MfticwBHAGswoz7pTo97wVDftqkjIFLI7fvtUNdCbr3axyJ1vPdFACDbQmMLAg==@parameshtest.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@parameshtest@")
db = client['rmmdb']
collection = db['groupCollection']
schedule_details = collection.find({"groupType":"Cluster"})
# Fetch schedule details from MongoDB
schedule_length = 0
schedule_details = collection.find({"groupType":"Cluster"})
details = list(schedule_details);
for schedule in details:
    schedule_length += 1;            
print(schedule_length)
# Define the DAGs and tasks
for num in range(schedule_length):
    dag_name = "dag_param{}".format(num)
    globals()[dag_name] = DAG(
        "DAG_param_in_airflow_{}".format(num),
        schedule_interval="@once",
        start_date=datetime(2024, 5, 29, 4, 57, 0, 0),  # Start from today
        is_paused_upon_creation=True
    )

    def extract_data_callable():
        # Print message, return a response
        print("Extracting data from an weather API")
        schedule_details = collection.find({"groupType":"Cluster"})
        details = list(schedule_details);
        print(details)
        for schedule in details:
            print(schedule["groupId"])
        
        return {
            "date": "2023-01-01",
            "location": "NYC",
            "weather": {
                "temp": 33,
                "conditions": "Light snow and wind"
            }
        }

    start = PythonOperator(
        task_id='start_{}'.format(num),
        python_callable=extract_data_callable,
        dag=globals()[dag_name]
    )

    hello = BashOperator(
        task_id="hello_{}".format(num),
        bash_command="echo schedule_details",
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
