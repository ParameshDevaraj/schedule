from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook # type: ignore
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='sync_dags_from_azure',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    def download_dags():
        hook = WasbHook(wasb_conn_id='azure_blob_storage_default')
        container_name = '4ffa5c84-359f-4c33-ba5f-a0857f2311e8'
        local_dags_dir = '/reportdata/dags'

        # List all blobs in the container
        blobs = hook.list_blobs(container_name)

        for blob in blobs:
            if blob.endswith('.py'):
                local_path = os.path.join(local_dags_dir, blob)
                hook.get_file(local_path, container_name, blob)

    sync_dags_task = PythonOperator(
        task_id='sync_dags',
        python_callable=download_dags,
    )

    sync_dags_task
