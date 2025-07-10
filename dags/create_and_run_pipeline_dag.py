from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionCreateInstanceOperator, CloudDataFusionDeleteInstanceOperator, CloudDataFusionStartPipelineOperator
from airflow.operators.python_operator import PythonOperator



import json
import requests
from google.auth.transport.requests import Request
from google.auth import default

def deploy_datafusion_pipeline(**kwargs):
    # Get default credentials
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    access_token = credentials.token

    # Data Fusion instance details
    project_id = "big-data-work-462814"
    location = "us-central1"
    instance = "employee-data-instance"
    pipeline_name = "extract_pipeline"

    # Pipeline JSON (minimal example)
    pipeline_json = {
        "name": pipeline_name,
        "description": "GCS to BigQuery pipeline",
        "artifact": {
            "name": "cdap-data-pipeline",
            "version": "6.7.1",  # Adjust based on your instance
            "scope": "SYSTEM"
        },
        "config": {
            "stages": [
                {
                    "name": "GCSFile",
                    "plugin": {
                        "name": "GCSFile",
                        "type": "batchsource",
                        "properties": {
                            "project": 'big-data-work-462814',
                            "referenceName": "gcsref",
                            "format": "csv",
                            "path": "gs://big-data-dataset-bucket14082003/employee_data.csv",
                            "schema": """{
                                "type":"record",
                                "name":"employee",
                                "fields":[
                                    {"name":"first_name","type":"string"},
                                    {"name":"last_name","type":"string"},
                                    {"name":"job_title","type":"string"},
                                    {"name":"department","type":"string"},
                                    {"name":"email","type":"string"},
                                    {"name":"address","type":"string"},
                                    {"name":"phone_number","type":"string"},
                                    {"name":"salary","type":"int"},
                                    {"name":"password","type":"string"}
                                ]
                            }"""
                        }
                    }
                },
                {
                    "name": "BigQuerySink",
                    "plugin": {
                        "name": "BigQueryTable",
                        "type": "batchsink",
                        "properties": {
                            "project": 'big-data-work-462814',
                            "dataset": "employee_data",
                            "table": "emp_table",
                            "referenceName": "bqref",
                            "schemaUpdateBehavior": "ALLOW_FIELD_ADDITION",
                            "operation": "INSERT"
                        }
                    }
                }
            ],
            "connections": [{"from": "GCSFile", "to": "BigQuerySink"}]
        }
    }

    url = f"https://{location}-datafusion.googleapis.com/v1/projects/{project_id}/locations/{location}/instances/{instance}/namespaces/default/apps"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, data=json.dumps(pipeline_json))
    if response.status_code != 200:
        raise Exception(f"Failed to create pipeline: {response.text}")


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
    # Create Data Fusion instance
    create_instance = CloudDataFusionCreateInstanceOperator(
        task_id='create_datafusion_instance',
        instance_name='employee-data-instance',
        location='us-central1',
        instance={                  
        "type": "BASIC",
        "displayName": "employee-data-instance"
    },
        dag=dag
    )
    
    # Deploy Data Fusion pipeline
    deploy_pipeline = PythonOperator(
    task_id='deploy_pipeline',
    python_callable=deploy_datafusion_pipeline,
    provide_context=True,
    dag=dag
    )

    # Start the pipeline to extract data
    start_pipeline = CloudDataFusionStartPipelineOperator(
    task_id='start_extract_pipeline',
    project_id='big-data-work-462814',
    location='us-central1',          
    instance_name='employee-data-instance',
    pipeline_name='extract_pipeline',
    dag=dag
)


    # Delete Data Fusion instance after completion
    delete_instance = CloudDataFusionDeleteInstanceOperator(
        task_id='delete_datafusion_instance',
        instance_name='employee-data-instance',
        location='us-central1',
        trigger_rule='all_done',  # Ensure this runs even if previous tasks fail
        dag=dag
    )

    run_script >> create_instance >> deploy_pipeline >>  start_pipeline >> delete_instance