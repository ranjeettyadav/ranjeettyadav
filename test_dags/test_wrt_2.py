from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import importlib
import airflow

try:
    from airflow.contrib.operators import gcs_to_bq
except ImportError:
    gcs_to_bq = None

if gcs_to_bq is not None:
    args = {
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    }

project_dm = 'dmgcp-ingestion-poc'
location = 'US'
bq_connection_id= 'bigquery_default'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'project_id': project_dm,
    'schedule_interval': None
}

dag = DAG(
    dag_id='test_wrt_2',
    default_args=default_dag_args
    
)

load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_example',
    bucket='ebcidc-to-bq-testing-us-central',
    source_objects=['csv/wrt/wrt_output.csv'],
    destination_project_dataset_table='airflow_test.wrt_2',
    schema_objects='gs://kubernetes-staging-85897c950b/Dags/json/wrt-00000-of-00001.json',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    bigquery_conn_id=bq_connection_id,           
    google_cloud_storage_conn_id=bq_connection_id)

load_csv


