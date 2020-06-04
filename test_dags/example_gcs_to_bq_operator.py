from airflow.models import Connection
from airflow.settings import Session
import airflow
from airflow import models

project_dm = 'dmgcp-ingestion-poc'
location = 'US'
bq_connection_id= 'bigquery_default'


try:
    from airflow.contrib.operators import gcs_to_bq
except ImportError:
    gcs_to_bq = None

if gcs_to_bq is not None:
    args = {
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    }


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': airflow.utils.dates.days_ago(0),
    }

dag = models.DAG(dag_id='example_gcs_to_bq_operator', default_args=default_dag_args,schedule_interval="00 7 * * *")

load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_example',
    bucket='kubernetes-staging-85897c950b',
    source_objects=['airflow_data_file/us-states.csv'],
    destination_project_dataset_table='airflow_test.gcs_to_bq_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    bigquery_conn_id=bq_connection_id,           
    google_cloud_storage_conn_id=bq_connection_id)
    # [END howto_operator_gcs_to_bq]

#    delete_test_dataset = bash_operator.BashOperator(
#        task_id='delete_airflow_test_dataset',
#        bash_command='bq rm -rf airflow_test',
#        dag=dag)

load_csv
