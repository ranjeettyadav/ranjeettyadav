import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
import uuid

project_dm = 'dmgcp-ingestion-poc'
location = 'US'
bq_connection_id= 'bigquery_default'

# create a dictionary of default typical args to pass to the dag
default_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'schedule_interval': '@daily',
    'retry_delay': timedelta(minutes=2),
}

# define the dag
dag = DAG('test_df_job_23', # give the dag a name
		   description = 'Test_Job',
           
           default_args=default_args
         )

run_entl_stat_hist = DataFlowJavaOperator(
	dag=dag,
	task_id='test_df_job',
    gcp_conn_id=bq_connection_id,
	jar="gs://ebcidc-to-bq-testing-us-central/ebcdic/jar/ebcidctobq-1.0.jar",
	options={
		'binaryFile':"gs://ebcidc-to-bq-testing-us-central/ebcdic/ecdic_files/WRT/wrt_csv_out_eb", 
        'copyBook':"gs://ebcidc-to-bq-testing-us-central/ebcdic/copybooks/diff_cop_book/WRT.cob",
        'parserClass':"com.dm.parser.ebcdic.FixedWidthFileParser",
        'outSchemaJsonPath':"gs://ebcidc-to-bq-testing-us-central/ebcdic/json/bq_schema/wrt*.json",
        'dqCheckJson':"gs://ebcidc-to-bq-testing-us-central/ebcdic/json/dq/wrt_dq_config.json",
        'dataTransformationJson':"gs://ebcidc-to-bq-testing-us-central/ebcdic/json/datatrans/wrt_transform.json",
        'outputTable':"dmgcp-ingestion-poc:airflow_test.test_df_job_23",
        'outputTableWriteMethod':"write_truncate",
        'errorTable':"dmgcp-ingestion-poc:airflow_test.test_df_job_23",
        'errorTableWriteMethod':"write_truncate",
        'auditTable':"dmgcp-ingestion-poc:transient.test_ebcdic_audit",
        'batchId':"202004281500",
        'splitSize':"500 MB",
		'project': "dmgcp-ingestion-poc",
		'tempLocation': "gs://ebcidc-to-bq-testing-us-central/ebcdic/temp/",
		'region' : "us-central1",
		'numWorkers' : "2",
		'maxNumWorkers' : "20",
		'workerMachineType' : "n1-standard-4",
		'serviceAccount' : "dm-ingestion@dmgcp-ingestion-poc.iam.gserviceaccount.com",
        'subnetwork':"https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/us-central1-network-foundation" 
	})
    
run_entl_stat_hist