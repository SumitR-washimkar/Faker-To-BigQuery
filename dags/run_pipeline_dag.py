from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.operators.python_operator import PythonOperator





default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'depend_on_past': False,
    'email': ['tomcruise17125@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag=DAG(
    'employee_data_pipeline',
    default_args=default_args,
    description='Runs on external python script',
    schedule_interval='@daily',
    catchup=False
)

with dag:
    run_script= BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/Scripts/extract.py',
        dag=dag
    )
    
    # Start the pipeline to extract data
    start_pipeline = CloudDataFusionStartPipelineOperator(
    task_id='start_pipeline',
    project_id='big-data-work-462814',
    location='us-west1',          
    instance_name='fusion-etl',
    pipeline_name='ETL-pipeline',
    wait=True,  # ensures operator waits for execution status
    timeout=600,  # waits 10 minutes instead of default 5
    dag=dag
)

    # Define the task dependencies
    run_script >> start_pipeline 