from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import importlib
import airflow

project_dm = 'dmgcp-ingestion-poc'
location = 'US'
bq_connection_id= 'my_gcp_connection'

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
    dag_id='test2',
    default_args=default_dag_args,
    schedule_interval='0 0 * * *',


def run_query(query):
  client = bigquery.Client()
  query_job = client.query (query)
  results = query_job.result()
  return results

if __name__ == '__main__':
    params=sys.argv[1]
    newParams=params.replace("'", "\"")
    params1=json.loads(newParams)   
   
    query = """SELECT count(*) FROM `dmgcp-ingestion-poc.transient.cvn_stress_8gb` """ 
   
    results=run_query(query)
