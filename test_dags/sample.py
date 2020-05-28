from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import importlib
import airflow

project_dm = 'dmgcp-ingestion-poc'
location = 'US'
bq_connection_id= 'bigquery_default'

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'project_id': project_dm
}

dag = DAG(
    dag_id='sample',
    default_args=default_dag_args,
    schedule_interval='0 0 * * *',
)

create_sample = BigQueryOperator(
    task_id='create_sample',
    sql='ddl/sample.sql',
    bigquery_conn_id=bq_connection_id,
    use_legacy_sql=False,
    dag=dag
    )

create_sample