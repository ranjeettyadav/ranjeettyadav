AIRFLOW_CONN_BIGQUERY_DEFAULT="google-cloud-platform://?extra__google_cloud_platform__project=dmgcp-ingestion-poc"
    
    

import airflow
try:
    from airflow.contrib.operators import gcs_to_bq
except ImportError:
    gcs_to_bq = None
from airflow import models
from airflow.operators import bash_operator


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

dag = models.DAG(dag_id='example_gcs_to_bq_operator', default_args=default_dag_args,schedule_interval=None)

#create_airflow_test_dataset = bash_operator.BashOperator(
#    task_id='create_airflow_test_dataset',
#    bash_command='bq mk airflow_test',
#    dag=dag)

    # [START howto_operator_gcs_to_bq]
load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_example',
    bucket='kubernetes-staging-85897c950b',
    source_objects=['airflow_data_file/us-states.csv'],
    destination_project_dataset_table='transient.gcs_to_bq_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    bigquery_conn_id='bigquery_default',           
    google_cloud_storage_conn_id='bigquery_default')
    # [END howto_operator_gcs_to_bq]

#    delete_test_dataset = bash_operator.BashOperator(
#        task_id='delete_airflow_test_dataset',
#        bash_command='bq rm -rf airflow_test',
#        dag=dag)

load_csv
