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

#acp = importlib.import_module("ranjeettyadav.ranjeettyadav.airflow_config_property")
#bq_connection_id = acp.bq_connection_id

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'project_id': project_dm
}

dag = DAG(
    dag_id='test',
    default_args=default_dag_args,
    schedule_interval='0 0 * * *',
)

#run_this = BashOperator(
#    task_id='run_this',
#    #use_legacy_sql=False,
#    bash_command='bq query --nouse_legacy_sql "SELECT count(*) FROM `dmgcp-ingestion-poc`.transient.cvn_stress_8gb" ',
#    dag=dag,
#    bigquery_conn_id=bq_connection_id
#)


BQ_Trans_alloc = BigQueryOperator(
        task_id='bq_trans_alloc',
        sql='bigquery/alloc.sql',
        bigquery_conn_id=bq_connection_id,
        use_legacy_sql=False,
        dag=dag
        )

BQ_Trans_alloc


#run_this

