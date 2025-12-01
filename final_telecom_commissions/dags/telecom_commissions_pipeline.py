from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.trigger_rule import TriggerRule
import json
import base64
 
# ---------- DAG DEFINITION ----------
dag = DAG(
    dag_id='telecom_commissions_pipeline',
    schedule_interval=None,                 # Triggered only by Pub/Sub
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data-engineer',
        'retries': 1,
        'retry_delay': 300,  # 5 minutes
    },
    tags=['telecom', 'commissions', 'pyspark']
)
 
# ---------- 1. WAIT FOR NEW FILE MESSAGE ----------
wait_for_file = PubSubPullSensor(
    task_id='wait_for_new_file',
    project_id='telecom-cdr',
    subscription='file-arrived-sub',         # ← You must create this subscription on topic: file-arrived-topic
    poke_interval=10,
    timeout=600,
    mode='reschedule',
    dag=dag
)
 
# ---------- 2. EXTRACT FILE PATH FROM PUB/SUB MESSAGE ----------
def extract_gcs_path(**context):
    message = context['task_instance'].xcom_pull(task_ids='wait_for_new_file')
    if not message:
        raise ValueError("No message received")
    
    # Decode base64 + JSON
    data = base64.b64decode(message[0]['message']['data']).decode('utf-8')
    payload = json.loads(data)
    gcs_path = f"gs://{payload['bucket']}/{payload['file']}"
    print(f"Extracted GCS path: {gcs_path}")
    return gcs_path
 
from airflow.operators.python import PythonOperator
get_file_path = PythonOperator(
    task_id='get_gcs_file_path',
    python_callable=extract_gcs_path,
    provide_context=True,
    dag=dag
)
 
# ---------- 3. RUN PYSPARK JOB ON DATAPROC SERVERLESS ----------
pyspark_job = {
    "reference": {"project_id": "telecom-cdr"},
    "placement": {"cluster_name": "telecom-comm-cluster"},   # Your cluster
    "pyspark_job": {
        "main_python_file_uri": "gs://telecom-commissions/scripts/etl_commissions.py",
        "args": ["{{ ti.xcom_pull(task_ids='get_gcs_file_path') }}"]
    }
}
 
run_etl = DataprocSubmitJobOperator(
    task_id='run_pyspark_etl',
    project_id='telecom-cdr',
    region='us-central1',
    job=pyspark_job,
    dag=dag
)
 
# ---------- TASK ORDER ----------
wait_for_file >> get_file_path >> run_etl
